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

var uuid = require('uuid')
  , util = require('util')
  , redis = require('redis')
  , EventEmitter = require('events').EventEmitter
  , connect
  , MessageBus
  , makeId
  , wrapError
  , initResponder;


wrapError = function(that, evt){
    that.emit('error', evt);
};

exports.makeId = makeId = function(){
    return uuid().replace(/-/g, "");
};

initResponder = function(that, evt){
    that.messageClient.subscribe('message');
    that.messageClient.on('message', function(channel, message){
        var msgBody;
        if (channel !== that.broadcastChannel){
            // Ignore the message
            return;
        }

        that.dataClient.rpop(that.dataQueue, function(err, reply){
            msgBody = reply;
            that.emit('dataAvailable', msgBody);
        });

    });
};

MessageBus = function(options){
    EventEmitter.call(this);
    this.redisHost = null;
    this.redisPort = null;
    this.redisPassword = null;
    this.debug_is_enabled = false;
    this.idToChannelMap = {};
    this.listenChannel = "msgChannels:" + makeId();
    this.broadcastChannel = "msgChannels:broadcast";
    this.dataQueue = (options && options.queue) || "msgQueue:normal";
    this.responseHash = (options && options.outputHash) || "";

    if (options && options.host){ this.redisHost = options.host; }
    if (options && options.port){ this.redisPort = options.port; }
    if (options && options.password){ this.password = options.password; }

    this.messageClient = redis.createClient(this.redisHost, this.redisPort);
    this.dataClient = redis.createClient(this.redisHost, this.redisPort);

    if (this.redisPassword){
      this.messageClient.auth(this.redisPassword, function(res){
        if (this.debug_is_enabled){
          console.log("Auth'd redis messaging started with " + res);
        }
      });
      this.dataClient.auth(this.redisPassword, function(res){
        if (this.debug_is_enabled){
          console.log("Auth'd redis data service started with " + res);
        }
      });

    }

    if (options && options.isResponder){
        initResponder(this);
    }

    this.dataClient.on('error', function(evt){wrapError(this, evt);});
    this.messageClient.on('error', function(evt){wrapError(this, evt);});
};

util.inherits(MessageBus, EventEmitter);

MessageBus.prototype.sendMessage = function(id, msg, callback){
    var that = this;
    msg.listenChannel = this.listenChannel;
    this.dataClient.rpush(this.dataQueue, msg);
    this.messageClient.publish(this.broadcastChannel);
    // this.idToChannelMap[id] = this.listenChannel;

    this.messageClient.subscribe(this.listenChannel);
    this.on('message', function(channel, message){
        var response;

        if (channel !== that.listenChannel){
            // Error?
            throw new Error("Got message for some other channel");
        }
        that.dataClient.hget(that.responseHash, id, function(err, reply){
            response = reply;
            that.dataClient.hdel(that.responseHash, id);
            that.emit('responseReady', response);

            if (callback){
                callback(reply);
            }
        });
    });
};

MessageBus.prototype.sendResponse = function(msgId, status, msg){
    this.dataClient.hset(this.responseHash, msgId, msg);
    this.messageClient.publish(msg.listenChannel, msgId + " " + status);
};


MessageBus.prototype.shutdown = function(){
    this.dataClient.end();
    this.messageClient.end();
};

exports.connect = connect = function(options){
    return new MessageBus(options);
};
