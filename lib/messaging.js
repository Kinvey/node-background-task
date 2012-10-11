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
  , MEGABYTES = 1024 * 1024;


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
        that.emit('data_available');
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
    this.dataQueue = (options && options.queue) || "msgQueue:normal";
    this.responseHash = (options && options.outputHash) || "responseTable:normal";
    this.shutdownFlag = false;

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
    var response, id, status, parsedMsg, daFaq, callback;

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

    that.dataClient.hget(that.responseHash, id, function(err, reply){
        var myErr = null, respondedErr, o;
        if (err){
            myErr = new Error("REDIS Error: " + err);
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

        that.dataClient.hdel(that.responseHash, id);

        if (myErr){
            response = myErr;
            that.emit('error', myErr);
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

    if (msgString.length > MEGABYTES){
        callback(new Error("Payload too large!"));
        return;
    }

    this.dataClient.rpush(this.dataQueue, msgString, function(err, reply){
        if (err){
            callback(new Error("Error sending message: " + err));
            return;
        }
        that.pubClient.publish(that.broadcastChannel, id);
    });
    this.callbackMap[id] = callback;
};

MessageBus.prototype.acceptMessage = function(callback){
    var that = this;

    if (!callback){
        throw new Error('Invalid argument');
    }

    // Treat that.dataQueue as a QUEUE
    that.dataClient.lpop(that.dataQueue, function(err, reply){
        var listenChannel, id, msgBody;
        try {
            msgBody = JSON.parse(reply);
        } catch(e) {
            that.emit('error', new Error("Bad data in sent message, " + e));
            return;
        }

        if (!msgBody){
            that.emit('error', new Error("No data in sent message"));
            return;
        }

        id = msgBody._messageId;
        listenChannel = msgBody._listenChannel;

        that.idToChannelMap[id] = listenChannel;

        delete msgBody._listenChannel;
        delete msgBody._messageId;

        callback(id, msgBody);
    });
};

MessageBus.prototype.sendResponse = function(msgId, status, msg){
    if (!msgId || !status || !msg){
        throw new Error("Missing msgId, status or msg.");
    }

    if (status !== 'SUCCESS' &&
        status !== 'ERROR'   &&
        status !== 'FAILED')
    {
        throw new Error(status + ' is not a valid status.');
    }

    var listenChannel = this.idToChannelMap[msgId]
      , that = this, serializableError, o;

    this.dataClient.hset(this.responseHash, msgId, JSON.stringify(msg), function(err, reply){
        if (err){
            throw new Error(err);
        }
        that.pubClient.publish(listenChannel, msgId + " " + status);
    });
    delete this.idToChannelMap[msgId];
};


MessageBus.prototype.shutdown = function(){
    this.subClient.removeAllListeners('message');
    this.dataClient.removeAllListeners('error');
    this.pubClient.removeAllListeners('error');
    this.subClient.removeAllListeners('error');
    this.dataClient.end();
    this.pubClient.end();
    this.subClient.end();
    this.shutdownFlag = true;
};

exports.connect = connect = function(options){
    return new MessageBus(options);
};
