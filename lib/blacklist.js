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

var dataStore = require("./data_store"),
  util = require('util'),
  common = require('./common'),
  EventEmitter = require('events').EventEmitter,
  precond = require('precond');

/**
 * Initializes the blacklist - used for keeping track of taskKeys that perform bad actions, and preventing them
 * from adding tasks if a certain threshold is reached.
 *
 * @type {Function}
 */
var Blacklist = exports.Blacklist = function (options, callback) {     // Optional callback for auth operation test case
  // Call to EventEmitter constructor to intitialize arguments
  EventEmitter.call(this);

  if (!options) {
    throw new Error("I need a task key!");
  }

  // TaskKey for this blacklist
  this.taskKey = options.taskKey;

  // Set options
  this.failureInterval = options.failureInterval || 1;        // The max interval for the threshold to be reached
  this.blacklistThreshold = options.blacklistThreshold || 10; // The maximum number of bad tasks
  this.globalBlacklistTimeout = options.globalBlacklistTimeout || 3600; // Length of time on the blacklist

  // Whether or not to log blacklist events
  this.logBlacklist = options.logBlacklist || false; // NB: This won't work if you want the default to be true

  this.keyPrefix = "blacklist:";
  this.globalBlacklistKeyPrefix = this.keyPrefix + "globalBlacklist:";
  this.blacklistLogKeyPrefix = this.keyPrefix + "logs:";

  this.blacklistClient = dataStore.initialize(options, callback);

  this.blacklistClient.on('error', common.wrapError(this));
};

// Inherit EventEmitter's methods
util.inherits(Blacklist, EventEmitter);

/**
 * Check the blacklist status for a particular task
 *
 * @param task
 * @param callback
 */
Blacklist.prototype.blacklistStatus = function (task, callback) {
  var taskKey = task && task[this.taskKey],
    key,
    that = this;

  if (!callback) {
    callback = function () {
    };
  }

  if (!taskKey) {
    callback(false, "No task key, can't check blacklist.");
  } else {
    key = that.globalBlacklistKeyPrefix + taskKey;
    that.blacklistClient.blacklistStore.check(key, function (error, reply) {
      if (reply) {
        // We're blacklisted
        callback(reply.status, reply.timeRemaining, reply.result);

      } else {
        callback(false, -1, "");
      }
    });
  }
};

/**
 * Returns the number of taskKeys currently blacklisted
 *
 * @param callback
 */
Blacklist.prototype.getBlacklistCount = function(callback){
  var that = this;

  console.log(that.globalBlacklistKeyPrefix);

  that.blacklistClient.blacklistStore.getBlacklistCount(that.globalBlacklistKeyPrefix, function(err, result) {
    console.log(result);
    common.processResult(err, result, callback);
  });
};

/**
 * Add a failure - if the threshold is reached, add taskKey to the blacklist
 *
 * @param taskKey
 * @param reason
 * @param callback
 */
Blacklist.prototype.addFailure = function (taskKey, reason, callback) {
  var countKey, blacklistKey, that = this;

  if (!callback) {
    callback = function () {
    };
  }

  try {
    precond.checkArgument(reason, "Must supply a reason for the failure");
    precond.checkArgument(taskKey, "Invalid task, not running.");
  } catch (e) {
    callback(e);
    return;
  }

  countKey = that.keyPrefix + taskKey + ":count";
  blacklistKey = that.globalBlacklistKeyPrefix + taskKey;

  that.blacklistClient.blacklistCounterStore.increment(countKey, that.failureInterval, function (error, reply) {
    if (error) {
      callback(error);
    } else {

      if (reply > that.blacklistThreshold) {
        that.blacklistClient.blacklistStore.add(blacklistKey, that.globalBlacklistTimeout, reason, function (e, r) {
          var logKey, d;
          if (!e) {
            if (that.logBlacklist) {
              d = new Date();
              logKey = that.blacklistLogKeyPrefix + taskKey;
              that.blacklistClient.blacklistStore.log(logKey, d + "|" + reason, function () {
              });
            }
            callback("Blacklisted");
          } else {
            callback(error);
          }
        });
      } else {
        callback("OK");
      }
    }
  });
};

/**
 * Shutdown the blacklist
 *
 */
Blacklist.prototype.shutdown = function () {
  this.blacklistClient.shutdown();
};


