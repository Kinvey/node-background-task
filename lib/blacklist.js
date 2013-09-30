// Copyright 2013 Kinvey, Inc
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

var redis = require("redis")
  , util = require('util')
  , EventEmitter = require('events').EventEmitter
  , wrapError;


wrapError = function(emitter){
    return function(error){
        emitter.emit('error', error);
    };
};


var Blacklist = exports.Blacklist = function(options, callback){
    EventEmitter.call(this);

    if (!options){
        throw new Error("I need a task key!");
    }

    if (options.host){ this.redisHost = options.host; }
    if (options.port){ this.redisPort = options.port; }
    if (options.password){ this.redisPassword = options.password; }

    this.taskKey = options.taskKey;

    this.failureInterval = options.failureInterval || 1;
    this.blacklistThreshold = options.blacklistThreshold || 10;
    this.globalBlacklistTimeout = options.globalBlacklistTimeout || 3600;

    this.logBlacklist = options.logBlacklist || false; // NB: This won't work if you want the default to be true

    this.redisKeyPrefix = "blacklist:";
    this.globalBlacklistKeyPrefix = this.redisKeyPrefix + "globalBlacklist:";
    this.blacklistLogKeyPrefix = this.redisKeyPrefix + "logs:"

    this.blacklistClient = redis.createClient(this.redisPort, this.redisHost);

    this.blacklistClient.on('error', wrapError(this));

    if (options.password){
        this.blacklistClient.auth(options.password, function() {
          if (callback) {
            callback();
          }
        });
    }
};

// Inherit EventEmitter's methods
util.inherits(Blacklist, EventEmitter);

Blacklist.prototype.blacklistStatus = function(task, callback){
    var taskKey = task && task[this.taskKey]
      , redisKey
      , that = this;

    if (!callback){
        callback = function(){};
    }

    if (!taskKey){
        callback(false, "No task key, can't check blacklist.");
    } else {
        redisKey = that.globalBlacklistKeyPrefix + taskKey;
        that.blacklistClient.get(redisKey, function(error, reply){
            if (reply){
                // We're blacklisted
                that.blacklistClient.ttl(redisKey, function(error, timeRemaining){
                    if (timeRemaining){
                        callback(true, timeRemaining, reply);
                    } else {
                        callback(true, -1, reply);
                    }
                });
            } else {
                callback(false, -1, "");
            }
        });
    }
};

Blacklist.prototype.addFailure = function(taskKey, reason, callback){
    var errKey, countKey, that = this;

    if (!callback){
        callback = function(){};
    }

    if (!reason){
        callback(new Error("Must supply a reason for the failure"));
        return;
    }

    if (!taskKey){
        callback(new Error("Invalid task, not running."));
    } else {
        countKey = that.redisKeyPrefix + taskKey + ":count";

        that.blacklistClient.get(countKey, function(error, reply){
            var blacklistKey;

            // Count not in redis
            if (!reply){
                that.blacklistClient.setex(countKey, that.failureInterval, "1", function(e, r){
                    if (!error){
                        callback("OK");
                    } else {
                        callback(error);
                    }
                });
            } else {
                if (reply >= that.blacklistThreshold){
                    // Blacklist
                    blacklistKey = that.globalBlacklistKeyPrefix + taskKey;
                    that.blacklistClient.setex(blacklistKey, that.globalBlacklistTimeout, reason, function(e, r){
                        var logKey, d;

                        if (!error){
                            if (that.logBlacklist){
                                d = new Date();
                                logKey = that.blacklistLogKeyPrefix + taskKey;
                                // Fire and forget
                                that.blacklistClient.rpush(logKey, d + "|" + reason);
                            }
                            callback("Blacklisted");
                        } else {
                            callback(error);
                        }

                    });
                } else {
                    that.blacklistClient.incr(countKey, function(error, reply){
                        if (!error){
                            callback("OK");
                        } else {
                            callback(error);
                        }
                    });
                }
            }
        });
    }
};

Blacklist.prototype.shutdown = function(){
    this.blacklistClient.end();
};


