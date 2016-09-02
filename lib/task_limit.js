// Copyright 2016 Kinvey, Inc
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

var dataStore = require("./data_store"),
  util = require('util'),
  common = require('./common'),
  async = require('async'),
  EventEmitter = require('events').EventEmitter,
  os = require('os'),
  precond = require('precond');

/**
 * TaskLimit is used to define and manage the maximum number of tasks for a given taskKey.  A maxTasksPerKey is set,
 * and as tasks are added and completed, the limitStore value for that taskKey is incremented and decremented.
 *
 * If a taskKey reaches its max, new tasks are not allowed for that taskKey until at least one task finishes.
 *
 * @type {Function}
 */
var TaskLimit = exports.TaskLimit = function(options, callback) {    // optional callback for tests
  // Call to EventEmitter constructor to intitialize arguments
  EventEmitter.call(this);

  if (!options) {
    throw new Error("I need a task key!");
  }

  this.taskKey = options.taskKey;
  this.keyPrefix = "taskKey:";
  this.maxTasksPerKey = options.maxTasksPerKey || 5;
  this.taskLimitClient = dataStore.initialize(options, callback);

  this.taskLimitClient.on('error', common.wrapError(this));
};

// Inherit EventEmitter's methods
util.inherits(TaskLimit, EventEmitter);

/**
 * Starts a task - adds it to the limitStore if the limit maxTasksPerKey has not been reached.  If the limit has been
 * reached, an error is passed to the callback.
 *
 * @param task
 * @param callback
 */
TaskLimit.prototype.startTask = function(task, callback) {
  var value = task && task[this.taskKey],
    key,
    that = this,
    hostname = os.hostname();

  if (!callback) {
    callback = function() {
    };
  }

  try {
    precond.checkArgument(value, "Invalid task, not running.");
  } catch (e) {
    callback(e);
    return;
  }

  // No limitStore action if limitless client
  if (that.maxTasksPerKey == -1) {
    return callback(0); // Result task count is 0 because this client is limitless
  }

  key = that.keyPrefix + value;

  that.taskLimitClient.limitStore.increment(key, hostname, that.maxTasksPerKey, function(err, result) {
      if (err) {

        callback(err);
      } else {

        callback(result);
      }
    });
};

/**
 * End the task and remove it from the limitStore
 *
 * @param task
 */
TaskLimit.prototype.stopTask = function(task, callback) {
  var value = task && task[this.taskKey],
    key,
    that = this,
    hostname = os.hostname();;

  if (!callback) {
    callback = function() {
    };
  }

  try {
    precond.checkArgument(value, "Invalid task, can't stop.");
  } catch (e) {
    e.message += ": " + task + " : " + this.taskKey;
    callback(e);
    return;
  }

  key = that.keyPrefix + value;
  that.taskLimitClient.limitStore.decrement(key, hostname, that.maxTasksPerKey, function(err, result) {
    common.processResult(err, result, callback);
  });
};

/**
 * Shutdown TaskLimit
 */
TaskLimit.prototype.shutdown = function(callback) {
  var that = this;
  that.taskLimitClient.shutdown(callback);
};

// TODO:  Redo cleanup function; rethink.

/**
 * Cleanup the task limits - used upon startup of a client to remove any orphan tasks for the client host left over
 * from a previous instance of the application.  Necessary if an application failure on the client occurs.
 *
 * @param callback
 */
TaskLimit.prototype.cleanupTasks = function(callback) {
  var prefix,
    that = this,
    hostname = os.hostname();

  if (!callback) {
    callback = function() {
    };
  }

  prefix = that.keyPrefix + "*";

  that.taskLimitClient.limitStore.cleanup(prefix, hostname, function(err, result) {
    common.processResult(err, result, callback);
  });
};
