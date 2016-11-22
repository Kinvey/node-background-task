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

/*jshint loopfunc: true */

"use strict";

var redisClient = require('./redis_client'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter,
  common = require('./common'),
  async = require('async'),
  precond = require('precond');

/**
 * Intitializes the DataStore.  The DataStore includes four data objects:  limitStore for tracking taskKey limits,
 * a blacklistCounterStore and blacklistStore for tracking blacklisted taskKeys, and messageStore, for storing and retrieving message
 * payloads.
 *
 * @param options
 * @param callback
 * @returns {DataStore}
 */
exports.initialize = function(options, callback) {

  var DataStore;

  /**
   * Creates the datastore
   *
   * @param options
   * @constructor
   */
  DataStore = function(options) {
    // Call to EventEmitter constructor to intitialize arguments
    EventEmitter.call(this);

    if (!options) {
      options = {};
    }

    // Initializes the redis client
    this.redis = redisClient.initialize(options, callback);

    // Defines limitstore object and attaches associated functions
    this.limitStore = {
      increment: limitStoreIncrement,
      decrement: limitStoreDecrement,
      cleanup: limitCleanup,
      redis: this.redis
    };

    // Defines blacklist counter store and attaches associated funciton.  The blacklist counter store is used
    // to track failure events that contribute to the blacklist threshold.
    this.blacklistCounterStore = {
      increment: blacklistCounterIncrement,
      redis: this.redis
    };

    // Defines blacklist store (list of taskKeys blacklisted)
    this.blacklistStore = {
      add: blacklistStoreAdd,
      getBlacklistCount: getBlacklistCount,
      check: blacklistStoreCheck,
      log: blacklistStoreLog,
      redis: this.redis
    };

    // Defines the message store and associated functions, used for storing and processing message payloads
    this.messageStore = {
      create: messageStoreCreate,
      process: messageStoreProcess,
      redis: this.redis
    };
  };

  // TaskClient inherits from EventEmitter
  util.inherits(DataStore, EventEmitter);

  /**
   * Shut down the datastore
   */
  DataStore.prototype.shutdown = function() {
    var that = this;
    that.redis.shutdown();
  };

  // TODO:  Better abstraction for cleanup

  /**
   * Performs a limitstore cleanup for a specific instance of a client.  This function is executed on client
   * initialization to clean out any taskKeys associated with the client, in case of a prior unexpected termination.
   *
   * @param prefix
   * @param hostname
   * @param callback
   */
  var limitCleanup = function(prefix, message, callback) {
    var that = this,
      currentKey,
      currentValue;

    if (!callback) {
      callback = function() {
      };
    }
    // Find all keys that match the appropraite prefix
    that.redis.getKeys(prefix, function(err, keys) {
      if (err) {
        return callback(err);
      }

      if (!keys || keys.length === 0) {
        return callback();
      } else {
        // Here we iterate through the keys, and add a function for deleting any keys from the list that are from
        // the current client hostname.

        async.each(keys, function(item, cb) {
          that.redis.removeFromList(item, message, function(err, keys) {
            if (err) {
              return cb(err);
            }

            cb();
          });
        }, function(err, result) {
          if (err) {
            return callback(err);
          } else {
            callback();
          }
        });
      }
    });
  };

  /**
   * Add a value to the limit store counter for a particular taskKey, if the limit hasn't been reached yet.
   *
   * @param key
   * @param message
   * @param max
   * @param callback
   */
  var limitStoreIncrement = function(key, message, max, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    // Abort operation if unlimited tasks allowed for this key
    if (max === -1) {
      return setImmediate(callback, null, false);
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(message);
      precond.checkArgument(max);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.checkListMaxReached(key, max, function(err, result) {
      if (err) {
        callback(err);
      } else {
        if (!result) {
          that.redis.incrementList(key, message, function(err, result) {
            common.processResult(err, result, callback);
          });
        } else {
          callback(new Error("Too many tasks"));
        }
      }
    });
  };

  /**
   * Removes a task from the limit count.
   *
   * @param key
   * @param callback
   */
  var limitStoreDecrement = function(key, message, max, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    // Abort limitStore decrement operation if unlimited tasks allowed for this key
    if (max === -1) {
      return setImmediate(callback, null, 0);
    }

    try {
      precond.checkArgument(key);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.decrementList(key, message, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Gets the count of currently keys on the global blacklist
   *
   * @param prefix
   * @param callback
   */
  var getBlacklistCount = function(prefix, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(prefix);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    prefix += "*";

    that.redis.getKeys(prefix, function(err, result) {
      if (result && util.isArray(result)) {
        result = result.length;
      }
      common.processResult(err, result, callback);
    });
  };

  /**
   * Increments the blacklist counter of illegal events for a particular taskKey
   *
   * @param key
   * @param timeout
   * @param callback
   */
  var blacklistCounterIncrement = function(key, timeout, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(timeout);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.incrementExpiringKey(key, timeout, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Adds a taskKey to the blackList for a specified period of time
   *
   * @param key
   * @param timeout
   * @param value
   * @param callback
   */
  var blacklistStoreAdd = function(key, timeout, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(timeout);
      precond.checkArgument(value);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.createExpiringKey(key, timeout, value, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Checks to see if a taskKey is in the blacklist and how much longer it will remain in the blacklist
   *
   * @param key
   * @param callback
   */
  var blacklistStoreCheck = function(key, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(key);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.getKey(key, function(err, result) {
      if (err) {
        callback(err);
      }
      if (result) {
        that.redis.getTtlStatus(key, function(err, reply) {
          if (reply) {
            reply.result = result;
          }
          common.processResult(err, reply, callback);
        });
      } else {
        callback(null, {status: false, timeRemaining: -1, result: ""});
      }
    });
  };

  /**
   * Add an entry to a blacklist store log
   *
   * @param key
   * @param value
   * @param callback
   */
  var blacklistStoreLog = function(key, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(value);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    that.redis.push(key, value, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Add a message payload to the message store
   *
   * @param hash
   * @param id
   * @param message
   * @param callback
   */
  var messageStoreCreate = function(hash, id, message, metadata, callback) {
    var that = this,
      getType = {};

    if (metadata && getType.toString.call(metadata) === '[object Function]') {
      callback = metadata;
      metadata = null;
    }

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(hash);
      precond.checkArgument(id);
      precond.checkArgument(message);
    } catch (e) {
      callback(new Error("Invalid Argument"));
      return;
    }

    that.redis.setHashKey(hash, id, message, metadata, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Process and return a message payload - this retrieves and removes the payload from theunderlying store
   *
   * @param hash
   * @param id
   * @param callback
   */
  var messageStoreProcess = function(hash, id, metadata, callback) {
    var that = this,
      getType = {};

    if (metadata && getType.toString.call(metadata) === '[object Function]') {
      callback = metadata;
      metadata = null;
    }

    if (!callback) {
      callback = function() {
      };
    }

    try {
      precond.checkArgument(hash);
      precond.checkArgument(id);
    } catch (e) {
      callback(new Error("Invalid Argument"));
      return;
    }

    that.redis.getAndDeleteHashKey(hash, id, metadata, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  return new DataStore(options);

};

