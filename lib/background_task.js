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
  , blacklist = require('./blacklist')
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

        if (options.task){
            options.broadcast = options.task + "Broadcast";
            options.dataHash = options.task + "Table";
            options.outputHash = options.task + "Hash";
        }

        if (options.taskKey){
            this.taskKey = options.taskKey;
            if (!options.maxTasksPerKey){
                this.maxTasksPerKey = options.maxTasksPerKey = 5;
            }

            this.taskLimit = new task_limit.TaskLimit(options);
            this.taskLimit.on('error', wrapError(this));

            this.taskLimit.cleanupTasks();

            this.blacklist = new blacklist.Blacklist(options);
            this.blacklist.on('error', wrapError(this));

        }

        this.msgBus = new message.connect(options);
        this.timeout = 5000; // 5 second defualt timeout

        this.msgBus.on('error', wrapError(this));

        if (options && options.timeout){
            this.timeout = options.timeout;
        }


        if (options.isWorker){
            var that = this;
            this.msgBus.on('data_available', function(id){
                that.emit('TASK_AVAILABLE', id);
            });
        }


        // Simple way to ensure we're not shut down
        this.isAvailable = true;
    };

    // Inherit EventEmitter's methods
    util.inherits(BackgroundTask, EventEmitter);

    BackgroundTask.prototype.end = function(){
        if (!this.isAvailable) {
          return; // Nothing to do
        }

        this.isAvailable = false;
        // Hard end, don't worry about shutting down
        // gracefully here...
        if (this.blacklist){this.blacklist.shutdown();}
        if (this.taskLimit){this.taskLimit.shutdown();}
        this.msgBus.shutdown();
        this.removeAllListeners();
    };

    BackgroundTask.prototype.acceptTask = function(id, callback){
        var newCallback;

        if (!this.isAvailable){
            callback(new Error("Attempt to use invalid BackgroundTask"));
            return;
        }

        if (!id || id.length === 0){
            throw new Error('Missing Task ID.');
        }

        if (!callback || callback.length < 1){
            throw new Error('Invalid callback specified');
        }

        newCallback = function(reply){
          if (reply instanceof Error){
              if (reply.message === "DB doesn't recognize message"){
                  reply = new Error('Task not in database, do not accept');
              }
          }

          if (reply.taskDetails){
            callback(reply.taskDetails);
          } else {
            callback(reply);
          }
        };
        this.msgBus.acceptMessage(id, newCallback);
    };

    extractResponse = function(r){
        var response, id, respondedErr, o;

        if (!r.taskId && !r.taskDetails){
            throw new Error("Incomplete task response.");
        }

        id = r.taskId;
        response = r.taskDetails;

        if (response.isError){
            respondedErr = new Error(response.message);
            for (o in response){
                if (response.hasOwnProperty(o) &&
                    o !== 'isError' &&
                    o !== 'message'){
                    respondedErr[o] = response[o];
                }
            }
            response = respondedErr;
        }

        return {id: id, details: response};
    };

    BackgroundTask.prototype.addTask = function(msg, callback, progress){
        var that = this
          , id = message.makeId()
          , cb, timeoutId, timedoutCb, msgToSend, tmpErr, startTheTask;

        if (!this.isAvailable){
            callback(id, new Error("Attempt to use invalid BackgroundTask"));
            return id;
        }


        startTheTask = function(){
            that.taskLimit.startTask(msg, function(tasks){
                var err, progressCb, responseCb;
                if (tasks instanceof Error){
                    err = new Error('Too many tasks');
                    that.emit('TASK_ERROR', err);
                    callback(id, err);
                    return;
                }

                callbacks[id] = callback;

                progressCb = function(resp) {
                    var rply = extractResponse(resp);

                    that.emit('TASK_PROGRESS', rply.id, rply.details);

                    if(progress) {
                        progress(rply.id, rply.details);
                    }
                };

                responseCb =  function(resp){
                    var uniqueIndex = id // Make this callback unique
                      , rply = extractResponse(resp);

                    that.emit('TASK_DONE', rply.id, rply.details);
                };

                cb = function(reply){
                    var origCallback
                    , tid = id
                    , details = reply
                    , rply, fallback = false;

                    try {
                        rply = extractResponse(reply);
                        details = rply.details;
                        tid = rply.id;
                    } catch (e) {
                        // The system had an error
                        that.emit('TASK_ERROR', e);
                    }
                    origCallback = callbacks[tid];

                    that.taskLimit.stopTask(msg);
                    clearTimeout(timeoutId);
                    origCallback(tid, details);
                    delete callbacks[tid];
                    that.msgBus.removeListener('progress:' + id, progressCb);
                };

                timedoutCb = function(){
                    var origCallback = callbacks[id];
                    // replace the "orig" callback with an empty function
                    // in case the request still completes in the future and
                    // tries to call our callback.
                    callbacks[id] = function(reply){};

                    // Return an error
                    that.taskLimit.stopTask(msg);
                    origCallback(id, makeTimeoutError());
                    that.msgBus.removeListener('responseReady:' + id, responseCb);
                    that.msgBus.removeListener('progress:' + id, progressCb);
                };

                msgToSend = {
                    taskId: id,
                    taskDetails: msg
                };

                timeoutId = setTimeout(timedoutCb, that.timeout);
                that.msgBus.sendMessage(id, msgToSend, cb);
                that.msgBus.once('responseReady:' + id,responseCb);
                that.msgBus.on('progress:' + id, progressCb);
            });
        };

        if (that.taskKey && msg[that.taskKey]){
            that.blacklist.blacklistStatus(msg, function(isBlacklisted, timeLeft, reason){
                var tmpErr;
                if (isBlacklisted){
                    tmpErr = new Error('Blacklisted');
                    tmpErr.debugMessage = "Blocked, reason: " + reason + ", remaining time: " + timeLeft;
                    that.emit('TASK_ERROR', tmpErr);
                    callback(id, tmpErr);
                } else {
                    startTheTask();
                }
            });
        } else {
            tmpErr = new Error('Missing taskKey');
            that.emit('TASK_ERROR', tmpErr);
            callback(id, tmpErr);
        }
        return id;
    };

    BackgroundTask.prototype.reportTask = function(taskId, status, msg){
        var that = this
          , msgToSend, serializableError, o;


        if (!this.isAvailable){
            throw new Error("Attempt to use invalid BackgroundTask");
        }

        // We can't send Error's via JSON...
        if (msg instanceof Error){
            serializableError = {
                isError: true,
                message: msg.message
            };

            for (o in msg){
                if (msg.hasOwnProperty(o)){
                    serializableError[o] = msg[o];
                }
            }

            msg = serializableError;
        }


        msgToSend = {
            taskId: taskId,
            taskDetails: msg
        };

        if (!msg){
          throw new Error('Missing msgId, status or msg.');
        }

        that.msgBus.sendResponse(taskId, status, msgToSend);
    };

    BackgroundTask.prototype.completeTask = function(taskId, status, msg) {
        this.reportTask(taskId, status, msg);
    };
    BackgroundTask.prototype.progressTask = function(taskId, msg) {
        this.reportTask(taskId, 'PROGRESS', msg);
    };

    BackgroundTask.prototype.reportBadTask = function(taskKey, reason, callback){
        var reasonToSend;

        if (!this.isAvailable){
            callback("ERR", "Attempt to use invalid BackgroundTask");
            return;
        }


        if (reason instanceof Error){
            reasonToSend = reason.message;
        }

        this.blacklist.addFailure(taskKey, reason, callback);

    };


    makeTimeoutError = function(){
        return new Error('Task timed out');
    };


    return function(options){
        return new BackgroundTask(options);
    };

}());
