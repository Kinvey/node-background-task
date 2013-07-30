// Copyright 2012 Kinvey, Inc
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.


"use strict";

var uuid = require('node-uuid')
  , util = require('util')
  , redis = require('redis')
  , EventEmitter = require('events').EventEmitter
  , connect
  , MessageBus
  , makeId
  , wrapError
  , initResponder
  , initCreator
  , authClient
  , completeTransaction
  , KILOBYTES = 1024
  , MEGABYTES = 1024 * 1024
  , log;


log = function(msg){
    var d = new Date()
      , t = d.toISOString();
    util.debug(t + ": " + msg);
};

wrapError = function(that, evt){
    that.emit('error', evt);
};

exports.makeId = makeId = function(){
    return uuid().replace(/-/g, "");
};

initCreator = function(that){
    that.subClient.subscribe(that.listenChannel);
    that.subClient.on('message', function(channel, message){
        completeTransaction(that, channel, message);
    });
};


initResponder = function(that){
    var util = require('util');
    that.subClient.subscribe(that.broadcastChannel);
    that.subClient.on('message', function(channel, message){
        var msgBody, id;
        if (channel !== that.broadcastChannel){
            // This shouldn't happen, it would mean
            // that we've accidentally subscribed to
            // an extra redis channel...
            // If for some crazy reason we get an incorrect
            // channel we should ignore the message
            return;
        }
        id = message;
        that.emit('data_available', id);
    });
};

authClient = function(password, client, debug){
    if (password){
        client.auth(password, function(res){
            if (debug){
                console.log("Auth'd redis messaging started with " + res);
            }
        });
    }
};

exports.MessageBus = MessageBus = function(options){
    var that;

    if (!options){
        options = {};
    }

    EventEmitter.call(this);
    this.redisHost = null;
    this.redisPort = null;
    this.redisPassword = null;
    this.debug_is_enabled = false;
    this.idToChannelMap = {};
    this.callbackMap = {};
    this.listenChannel = "msgChannels:" + makeId();
    this.broadcastChannel = (options && options.broadcast) || "msgChannels:broadcast";
    this.dataHash = (options && options.dataHash) || "msgTable:normal";
    this.responseHash = (options && options.outputHash) || "responseTable:normal";
    this.shutdownFlag = false;
    this.id = makeId();        // For debugging / logging

    if (options && options.host){ this.redisHost = options.host; }
    if (options && options.port){ this.redisPort = options.port; }
    if (options && options.password){ this.redisPassword = options.password; }

    this.pubClient = redis.createClient(this.redisPort, this.redisHost);
    this.subClient = redis.createClient(this.redisPort, this.redisHost);
    this.dataClient = redis.createClient(this.redisPort, this.redisHost);

    authClient(this.redisPassword, this.dataClient, this.debug_is_enabled);
    authClient(this.redisPassword, this.pubClient, this.debug_is_enabled);
    authClient(this.redisPassword, this.subClient, this.debug_is_enabled);

    if (options && options.isResponder){
        initResponder(this);
    } else {
        initCreator(this);
    }

    that = this;
    this.dataClient.on('error', function(evt){wrapError(that, evt);});
    this.pubClient.on('error', function(evt){wrapError(that, evt);});
    this.subClient.on('error', function(evt){wrapError(that, evt);});
};

util.inherits(MessageBus, EventEmitter);

completeTransaction = function(that, ch, msg){
    var response, id, status, parsedMsg, daFaq, callback, multi;

    if (ch !== that.listenChannel){
        // This can NEVER happen (except for if there's an error)
        // in our code.  So the check is left in, but
        // should never be hit.
        that.emit('error', new Error("Got message for some other channel (Expected: " +
                                     that.listenChannel + ", Actual: " + ch + ")"));
        // Bail out!
        return;
    }

    parsedMsg = msg.split(' ');
    if (parsedMsg.length < 2){
        that.emit('error',  new Error("Invalid message received!"));
        // Bail out
        return;
    }

    id = parsedMsg[0];
    status = parsedMsg[1];

    callback = that.callbackMap[id];

    multi = that.dataClient.multi();

    multi.hget(that.responseHash, id)
    .hdel(that.responseHash, id)
    .exec(function(err, reply){
        var myErr = null, respondedErr, o;
        if (err){
            myErr = new Error("REDIS Error: " + err);
        }

        if (!util.isArray(reply) && !myErr){
            myErr = new Error("Internal REDIS error (" + err + ", " + reply + ")");
        } else if (util.isArray(reply)){
          // Reply[0] => hget, Reply[1] => hdel
          reply = reply[0];
        }

        if (reply === null && !myErr){
            myErr = new Error("No message for id " + id);
        }


        if (!myErr){
            try {
                response = JSON.parse(reply);
            } catch(e){
                myErr = new Error("JSON parsing failed! " + e);
            }
        }

        if (myErr){
            response = myErr;
            that.emit('error', myErr);
            return;
        }

				if(status === 'PROGRESS') {
				    that.emit('progress', response);
				    return;
				}

        if (status !== 'SUCCESS'){
            // Build the error
            that.emit('error', response);

            // If we're hard failed we stop working here,
            // no success
            if (status === "FAILED"){
                if (callback){
                    callback(response);
                    delete that.callbackMap[id];
                }
                return;
            }
        }


        that.emit('responseReady', response);

        if (callback){
            callback(response);
            delete that.callbackMap[id];
        }
    });

};

MessageBus.prototype.sendMessage = function(id, msg, callback){
    var that = this
      , err
      , msgString;

    if (this.shutdownFlag){
        callback(new Error("Attempt to use shutdown MessageBus."));
        return;
    }

    msg._listenChannel = this.listenChannel;
    msg._messageId = id; // Store the message id in-band

    msgString = JSON.stringify(msg);

    if (!msgString){
        callback(new Error("Error converting message to JSON."));
        return;
    }

    // TODO: This needs to be an option for the class
    if (msgString.length > MEGABYTES){
        callback(new Error("Payload too large!"));
        return;
    }

    this.dataClient.hset(this.dataHash, id, msgString, function(err, reply){
        if (err){
            callback(new Error("Error sending message: " + err));
            return;
        }
        that.pubClient.publish(that.broadcastChannel, id);
    });
    this.callbackMap[id] = callback;
};

MessageBus.prototype.acceptMessage = function(mid, callback){
    var that = this
      , multi
      , derivedId;

    if (!mid){
        throw new Error('Missing Message ID.');
    }

    if (!callback){
        throw new Error('Invalid callback.');
    }

    if (callback.length < 1){
        throw new Error('Missing parameters in callback.');
    }

    multi = that.dataClient.multi();

    // Pipeline!
    multi
    .hget(that.dataHash, mid)
    .hdel(that.dataHash, mid)
    .exec(function(err, reply){
        var listenChannel, id, dataErr, msgBody;

        // First make sure that everything is probably ok
        if (!util.isArray(reply)){
            dataErr = new Error("Internal REDIS error (" + err + ", " + reply + ")");
            callback(dataErr);
            return;
        } else {
            // reply is good, let's just get the first element
            // as the indeices should be hget,hdel
            reply = reply[0];
        }

        try {
            msgBody = JSON.parse(reply);
        } catch(e) {
            dataErr = new Error("Bad data in sent message, " + e);
            callback(dataErr);
            return;
        }

        if (!msgBody){
            dataErr = new Error("DB doesn't recognize message");
            callback(dataErr);
            return;
        }

        derivedId = id = msgBody._messageId;
        listenChannel = msgBody._listenChannel;

        if (id !== mid){
            console.log("ERROR: Mis-match on ids! (" + id + " does not equal " + mid + ")");
        }

        that.idToChannelMap[id] = listenChannel;


        delete msgBody._listenChannel;
        delete msgBody._messageId;

        callback(msgBody);

    });
};

MessageBus.prototype.sendResponse = function(msgId, status, msg){
    if (!msgId || !status || !msg){
        throw new Error("Missing msgId, status or msg.");
    }

    if (status !== 'SUCCESS' &&
        status !== 'ERROR'   &&
        status !== 'FAILED'  &&
        status !== 'PROGRESS')
    {
        throw new Error(status + ' is not a valid status.');
    }

    var listenChannel = this.idToChannelMap[msgId]
      , that = this, serializableError, o, tmpErr, testMode;

    if (arguments.length === 4){
        testMode = arguments[3];
    }

    if (!listenChannel && !testMode){
        // We have no record of this request
        // probably a callback being called twice, but
        // still need to throw
        tmpErr = new Error('Attempt to respond to message that was never accepted');
        tmpErr.debugMessage = msgId + " is not registered as having been accepted";
        throw tmpErr;
    }

    this.dataClient.hset(this.responseHash, msgId, JSON.stringify(msg), function(err, reply){
        // Keep the channel alive on PROGRESS, as more messages are to be expected.
        if(status !== 'PROGRESS') {
            delete that.idToChannelMap[msgId];
        }

        if (err){
            throw new Error(err);
        }
        that.pubClient.publish(listenChannel, msgId + " " + status);
    });
};


MessageBus.prototype.shutdown = function(){
    this.subClient.removeAllListeners('message');
    this.dataClient.removeAllListeners('error');
    this.pubClient.removeAllListeners('error');
    this.subClient.removeAllListeners('error');
    this.dataClient.end();
    this.pubClient.end();
    this.subClient.end();
    this.removeAllListeners();
    this.shutdownFlag = true;
};

exports.connect = connect = function(options){
    return new MessageBus(options);
};
