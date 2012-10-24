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

var redis = require("redis")
  , util = require('util')
  , EventEmitter = require('events').EventEmitter
  , wrapError;


wrapError = function(emitter){
    return function(error){
        emitter.emit('error', error);
    };
};


var TaskLimit = exports.TaskLimit = function(options){
    EventEmitter.call(this);

    if (!options){
        throw new Error("I need a task key!");
    }

    if (options.host){ this.redisHost = options.host; }
    if (options.port){ this.redisPort = options.port; }
    if (options.password){ this.redisPassword = options.password; }

    this.taskKey = options.taskKey;
    this.redisKeyPrefix = "taskKey:";
    this.maxTasksPerKey = options.maxTasksPerKey||10;
    this.taskLimitClient = redis.createClient(this.redisPort, this.redisHost);

    this.taskLimitClient.on('error', wrapError(this));

    if (options.password){
        this.taskLimitClient.auth(options.password);
    }

};

// Inherit EventEmitter's methods
util.inherits(TaskLimit, EventEmitter);

TaskLimit.prototype.startTask = function(task, callback){
    var value = task && task[this.taskKey]
      , redisKey, that = this;

    if (!callback){
        callback = function(){};
    }

    if (!value){
        callback(new Error("Invalid task, not running."));
    } else {
        redisKey = that.redisKeyPrefix + value;
        that.taskLimitClient.llen(redisKey, function(err, len){
            if (len >= that.maxTasksPerKey){
                callback(new Error("Too many tasks"));
            } else {
                that.taskLimitClient.lpush(redisKey, Date(), function(x){
                  callback(len+1);
                });
            }
        });
    }
};

TaskLimit.prototype.stopTask = function(task){
    var value = task && task[this.taskKey]
      , redisKey, that = this;


    if (!value){
        throw new Error("Invalid task, can't stop.");
    } else {
        redisKey = that.redisKeyPrefix + value;
        that.taskLimitClient.lpop(redisKey);
    }
};
