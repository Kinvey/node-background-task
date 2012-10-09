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
  , wrapError;


// We should handle this...
wrapError = function(error){ console.log(error); };

// Two clients required, pubsub and databas
// Wrap our module
exports.connect = (function(){
    var callbacks = []
      , makeTimeoutError
      , BackgroundTask;

    BackgroundTask = function(options){
        EventEmitter.call(this);

        this.msgBus = new message.connect(options);
        this.timeout = 5000; // 5 second defualt timeout
        
        
        if (options && options.timeout){
            this.timeout = options.timeout;
        }

        if (options && options.isResponder){
            var that = this;
            this.msgBus.on('data_available', function(id, body){
                that.emit('taskAvailable', body);
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

    BackgroundTask.prototype.addTask = function(msg, callback){
        var that = this
          , id = message.makeId()
          , cb, timeoutId, timedoutCb, msgToSend;

        callbacks[id] = callback;

        cb = function(reply){
            var origCallback = callbacks[id];
            clearTimeout(timeoutId);
            origCallback(reply);
            delete callbacks[id];
        };

        timedoutCb = function(){
            var origCallback = callbacks[id];
            // replace the "orig" callback with an empty function
            // in case the request still completes in the future and
            // tries to call our callback.
            callbacks[id] = function(reply){};

            // Return an error
            origCallback(makeTimeoutError());
        };

        msgToSend = {
            taskId: id,
            taskDetails: msg
        };

        this.msgBus.sendMessage(id, msgToSend, cb);
        this.msgBus.on('responseReady', function(resp) { that.emit('taskDone', resp); });
    };

    BackgroundTask.prototype.completeTask = function(taskId, status, msg){
        var that = this;
        that.msgBus.sendResponse(taskId, status, msg);
    };

    
    makeTimeoutError = function(){
        console.log("Fail");
        return "FAIL!";
    };


    return function(options){
        return new BackgroundTask(options);
    };

}());
