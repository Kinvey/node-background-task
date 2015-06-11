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

var RedisClient,
  redis = require('redis'),
  util = require('util'),
  common = require('./common'),
  EventEmitter = require('events').EventEmitter,
  precond = require('precond');

/**
 * Redis client wrapper - provides a series of redis wrapper convenience methods for the data store to consume to
 * perform specific operations.
 *
 * @param options
 * @param callback
 * @returns {RedisClient}
 */
exports.initialize = function (options, callback) {
  RedisClient = function (options, callback) {    // optional callback for auth test
    // Call to EventEmitter constructor to intitialize arguments
    EventEmitter.call(this);

    var redisOptions = {};
    var that = this;

    if (options.host) {
      this.redisHost = options.host;
    }
    if (options.port) {
      this.redisPort = options.port;
    }
    if (options.password) {
      this.redisPassword = options.password;
      redisOptions.auth_pass = this.redisPassword;
    }



    // Create the redis client
    this.client = redis.createClient(this.redisPort, this.redisHost, redisOptions);

    // Emit an event when the redis client is ready
    this.client.once('ready', function () {
      this.emit('clientReady');

      if (callback) {
        callback();
      }
    });

    // Emit all error events
    this.client.on('error', function (evt) {
      this.emit('taskError', evt);
    });
  };

  // TaskClient inherits from EventEmitter
  util.inherits(RedisClient, EventEmitter);

  /**
   * Checks the size of a list against the supplied maxiumum size.  Returns true in the callback if the length is
   * >= the max, false if the length < max.
   *
   * @param key
   * @param max
   * @param callback
   */
  RedisClient.prototype.checkListMaxReached = function (key, max, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(max);
    } catch(e) {
      callback(e);
      return;
    }

    that.client.llen(key, function (err, len) {
      if (err) {
        callback(err);
      } else {
        if (len >= max) {
          callback(null, true);
        } else {
          callback(null, false);
        }
      }
    });
  };

  /**
   * Get all keys that match a prefix
   *
   * @param prefix
   * @param callback
   */
  RedisClient.prototype.getKeys = function (prefix, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(prefix);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.keys(prefix, function (err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Get a range of values from a redis list that match a specific key
   *
   * @param key
   * @param start
   * @param stop
   * @param callback
   */
  RedisClient.prototype.getRange = function (key, start, stop, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(start);
      precond.checkArgument(stop);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.lrange(key, 0, -1, function (err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, result);
      }
    });
  };

  /**
   * Remove specific keys from a list
   *
   * @param key
   * @param count
   * @param value
   * @param callback
   */
  RedisClient.prototype.removeKeys = function (key, count, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(count);
      precond.checkArgument(value);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.lrem(key, count, value, function (err, result) {
      if (err) {
        callback(err);
      } else {
        callback(result);
      }
    });
  };

  /**
   * Add an element to the front of a list; used to "increment" a list
   *
   * @param key
   * @param message
   * @param callback
   */
  RedisClient.prototype.incrementList = function (key, message, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(message);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.lpush(key, message, function (err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Remove an element from a list - used to "decrement" a list
   *
   * @param key
   * @param message
   * @param callback
   */
  RedisClient.prototype.decrementList = function (key, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
    } catch(e) {
      callback(e);
      return;
    }

    that.client.lpop(key, function (err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Get a specific key value
   *
   * @param key
   * @param callback
   */
  RedisClient.prototype.getKey = function (key, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.get(key, function (err, result) {
      common.processResult(err, result, callback);
    });
  };

  /**
   * Create an expiring key if it doesn't exist
   *
   * @param key
   * @param timeout
   * @param value
   * @param callback
   */
  RedisClient.prototype.createExpiringKey = function (key, timeout, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(timeout);
      precond.checkArgument(value);
    } catch (e) {
      callback(e);
      return;
    }

    that.getKey(key, function (err, result) {
      if (err) {
        callback(err);
      } else {
        if (result) {
          callback(null, result);
        } else {
          that.client.setex(key, timeout, value, function (err, reply) {
            // Passing value back as result because reply is "OK, which is meaningless, and value can be used to
            // increment if an integer.
            common.processResult(err, value, callback);
          });
        }
      }
    });
  };

  /**
   * Increment an expiring key
   *
   * @param key
   * @param timeout
   * @param callback
   */
  RedisClient.prototype.incrementExpiringKey = function (key, timeout, callback) {
    var that = this;
    var multi = that.client.multi();

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
    } catch (e) {
      callback(new Error("Invalid Argument: " + e));
      return;
    }

    that.client.incr(key, function (err, result) {
      if (err) {
        callback(err);
      } else {
        multi
          .ttl(key)
          .get(key)
          .exec(function (err, result) {
            var myErr,
              ttl,
              value;

            if (err) {
              myErr = new Error("REDIS Error: " + err);
            }

            if (!util.isArray(result) && !myErr) {
              myErr = new Error("Internal REDIS error (" + err + ", " + result + ")");
            } else if (util.isArray(result)) {
              ttl = result[0];
              value = result[1];
            }

            if (myErr) {
              callback(myErr);
            } else if (ttl < 0 && value) {
              that.client.expire(key, timeout, function(error, result) {
                common.processResult(error, value, callback);
              });
            } else {
              callback(null, value);
            }
          });
      }
    });
  };

  /**
   * Get the TTL status of a key
   *
   * @param key
   * @param callback
   */
  RedisClient.prototype.getTtlStatus = function (key, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
    } catch (e){
      callback(e);
      return;
    }

    that.client.ttl(key, function (error, timeRemaining) {
      if (error) {
        callback(error);
      } else if (timeRemaining) {
        callback(null, {status: true, timeRemaining: timeRemaining});
      } else {
        callback(null, {status: true, timeRemaining: -1});
      }
    });
  };

  /**
   * Push a value to the end of a list
   *
   * @param key
   * @param value
   * @param callback
   */
  RedisClient.prototype.push = function (key, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(key);
      precond.checkArgument(value);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.rpush(key, value, function (err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, result);
      }
    });
  };


  /**
   * Subscribe to a redis pubsub channel, and emit an event when a message is received
   *
   * @param channel
   * @param callback
   */
  RedisClient.prototype.subscribe = function (channel, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(channel);
    } catch(e) {
      callback(e);
      return;
    }

    that.client.subscribe(channel, function (err) {
      if (err) {
        if (callback) {
          callback(err);
        }
      } else {
        that.client.on('message', function (channel, message) {
          that.emit('taskMessage', channel, message);
        });
        if (callback) {
          callback();
        }
      }
    });
  };

  /**
   * Retrieve and delete a hashkey value - used for processing message payloads
   *
   * @param hash
   * @param key
   * @param callback
   */
  RedisClient.prototype.getAndDeleteHashKey = function (hash, key, callback) {
    var that = this;
    var multi = that.client.multi();

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(hash);
      precond.checkArgument(key);
    } catch (e) {
      callback(new Error("Invalid argument"));
      return;
    }

    multi.hget(hash, key)
      .hdel(hash, key)
      .exec(function (err, result) {
        var myErr = null, msgBody;

        if (err) {
          myErr = new Error("REDIS Error: " + err);
        }

        if (!util.isArray(result) && !myErr) {
          myErr = new Error("Internal REDIS error (" + err + ", " + result + ")");
        } else if (util.isArray(result)) {
          // Reply[0] => hget, Reply[1] => hdel
          result = result[0];
        }

        if (result === null && !myErr) {
          myErr = new Error("No message for key " + key);
        }

        if (!myErr) {
          try {
            msgBody = JSON.parse(result);
          } catch (e) {
            myErr = new Error("Bad data in sent message, " + e);
          }
        }

        if (!myErr && !msgBody) {
          myErr = new Error("DB doesn't recognize message");
        }

        common.processResult(myErr, msgBody, callback);
      });
  };

  /**
   * Set a value in a hash.  Used for storing message payloads
   *
   * @param hash
   * @param key
   * @param value
   * @param callback
   */
  RedisClient.prototype.setHashKey = function (hash, key, value, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(hash);
      precond.checkArgument(key);
      precond.checkArgument(value);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.hset(hash, key, value, function (err, result) {
      if (err) {
        err = new Error("Error sending message: " + err);
      }
      common.processResult(err, result, callback);
    });
  };

  /**
   * Publish a value to a specified channel using redis pubsub
   *
   * @param channel
   * @param key
   * @param callback
   */
  RedisClient.prototype.publish = function (channel, key, callback) {
    var that = this;

    if (!callback) {
      callback = function() {};
    }

    try {
      precond.checkArgument(channel);
      precond.checkArgument(key);
    } catch (e) {
      callback(e);
      return;
    }

    that.client.publish(channel, key, function (err, result) {
      if (err) {
        return err = new Error("Error publishing message: " + err);
      }
      common.processResult(err, result, callback);
    });
  };

  /**
   * Shut down the redis client
   */
  RedisClient.prototype.shutdown = function () {
    this.client.removeAllListeners();
    this.client.end();
  };

  return new RedisClient(options, callback);

};