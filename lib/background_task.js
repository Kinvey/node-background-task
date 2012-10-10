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
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , message = require('./messaging')
  , task_limit = require('./task_limit')
  , wrapError;


// We should handle this...
wrapError = function(emitter){
    return function(error){
        emitter.emit('error', error);
    };
};

exports.connect = (function(){
    var callbacks = []
      , makeTimeoutError
      , BackgroundTask
      , extractResponse;

    BackgroundTask = function(options){
        EventEmitter.call(this);

        if (!options){
            options = {};
        }

        if (options.isWorker){
            options.isResponder = true;
        }

        if (options.taskKey){
            this.taskKey = options.taskKey;
            if (!options.maxTasksPerKey){
                this.maxTasksPerKey = options.maxTasksPerKey = 5;
            }

            this.taskLimit = new task_limit.TaskLimit(options);
        }

        this.msgBus = new message.connect(options);
        this.timeout = 5000; // 5 second defualt timeout

        this.msgBus.on('error', wrapError(this));

        if (options.task){
            options.broadcast = options.task + "Broadcast";
            options.queue = options.task + "Queue";
            options.outputHash = options.task + "Hash";
        }

        if (options && options.timeout){
            this.timeout = options.timeout;
        }


        if (options.isWorker){
            var that = this;
            this.msgBus.on('data_available', function(){
                that.emit('TASK_AVAILABLE');
            });
        }
    };

    // Inherit EventEmitter's methods
    util.inherits(BackgroundTask, EventEmitter);

    BackgroundTask.prototype.end = function(){
        // Hard end, don't worry about shutting down
        // gracefully here...
        this.msgBus.shutdown();
    };

    BackgroundTask.prototype.acceptTask = function(callback){
        var newCallback;
        if (callback.length < 2){
            throw new Error('Invalid callback specified');
        }

        newCallback = function(id, reply){
          if (reply.taskDetails){
            callback(id, reply.taskDetails);
          } else {
            callback(id, reply);
          }
        };
        this.msgBus.acceptMessage(newCallback);
    };

    extractResponse = function(response){
        if (!response.taskId && !response.taskDetails){
            throw new Error("Incomplete task response.");
        }

        return {id: response.taskId, details: response.taskDetails};
    };

    BackgroundTask.prototype.addTask = function(msg, callback){
        var that = this
          , id = message.makeId()
          , cb, timeoutId, timedoutCb, msgToSend;

        if (that.taskKey && msg[that.taskKey]){
            that.taskLimit.startTask(msg, function(tasks){
                if (tasks instanceof Error){
                    var err = new Error('Too many tasks');
                    that.emit('TASK_ERROR', err);
                    callback(err);
                }

                callbacks[id] = callback;

                cb = function(reply){
                    var origCallback
                    , tid
                    , details
                    , rply;

                    rply = extractResponse(reply);
                    tid = rply.id;
                    details = rply.details;

                    origCallback = callbacks[tid];

                    clearTimeout(timeoutId);
                    origCallback(tid, details);
                    delete callbacks[tid];
                };

                timedoutCb = function(){
                    var origCallback = callbacks[id];
                    // replace the "orig" callback with an empty function
                    // in case the request still completes in the future and
                    // tries to call our callback.
                    callbacks[id] = function(reply){};

                    // Return an error
                    origCallback(id, makeTimeoutError());
                };

                msgToSend = {
                    taskId: id,
                    taskDetails: msg
                };

                timeoutId = setTimeout(timedoutCb, that.timeout);
                that.msgBus.sendMessage(id, msgToSend, cb);
                that.msgBus.on('responseReady', function(resp){
                    var rply = extractResponse(resp);
                    that.emit('TASK_DONE', rply.id, rply.details);
                });
                that.taskLimit.stopTask(msg);
            });
        }
    };

    BackgroundTask.prototype.completeTask = function(taskId, status, msg){
        var that = this
          , msgToSend = {
            taskId: taskId,
            taskDetails: msg
        };

        if (!msg){
          throw new Error('Missing msgId, status or msg.');
        }
        
        that.msgBus.sendResponse(taskId, status, msgToSend);
    };


    makeTimeoutError = function(){
        return new Error('Task timed out');
    };


    return function(options){
        return new BackgroundTask(options);
    };

}());
