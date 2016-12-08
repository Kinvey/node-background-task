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

var uuid = require('uuid'),
  dataStore = require('./data_store'),
  redis = require('./redis_client'),
  common = require('./common'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter,
  initCreator,
  initResponder,
  MEGABYTES = 1024 * 1024,
  async = require('async'),
  precond = require('precond'),
  apiLevel = 2,
  makeId;


/**
 * Make a new unique ID for purposes of creating a channel for sending messages
 *
 * @type {Function}
 */
exports.makeId = makeId = function() {
  return uuid().replace(/-/g, "");
};

/**
 * Initializes the bus as a creator
 *
 * @param that
 */
initCreator = function(that) {
  that.subClient.subscribe(that.listenChannel);
  that.subClient.on('taskMessage', function(channel, message) {
    var msgObj;
    try {
      msgObj = JSON.parse(message);
    } catch (e) {
      that.emit('error', new Error("Invalid message received!"));
      return;
    }
    that.emit('notification_received', msgObj);
  });
};

/**
 * Initializes the bus as a responder
 *
 * @param that
 */
initResponder = function(that) {
  that.subClient.subscribe(that.broadcastChannel);
  that.subClient.on('taskMessage', function(channel, message) {
    var msgObj;
    if (channel !== that.broadcastChannel) {
      // This shouldn't happen, it would mean
      // that we've accidentally subscribed to
      // an extra redis channel...
      // If for some crazy reason we get an incorrect
      // channel we should ignore the message
      return;
    }
    try {
      msgObj = JSON.parse(message);
    } catch (e) {
      that.emit('error', new Error("Invalid message Received!"));
      return;
    }
    that.emit('notification_received', msgObj);
  });
};

/**
 * Initailizes the Notificaiton bus
 *
 * @param options
 * @param callback
 * @returns {NotificationBus}
 */
exports.initialize = function(options, callback) {

  var NotificationBus;

  /**
   * Initializes a NotificationBus for sending and receiving messages.  The bus has two methods - sendNotification and
   * processNotification.  The bus also emits an event when a new message is received for processing.  The receiver, once
   * having received the notification_received event, should issue a call to process the message to obtain the message payload.
   *
   * @param options
   * @param callback
   * @constructor
   */
  NotificationBus = function(options, callback) {
    // Call to EventEmitter constructor to intitialize arguments
    EventEmitter.call(this);

    var that = this;

    if (!options) {
      options = {};
    }

    this.apiLevel = apiLevel;

    // The listenChannel is the primary channel for this sender/receiver
    this.listenChannel = "msgChannels:" + makeId();

    // a Base hash to be appended to the status
    this.baseHash = (options && options.baseHash) || ":normal";

    // The broadcast channel
    this.broadcastChannel = (options && options.broadcast) || "broadcast:level" + this.apiLevel;

    // The maximum message payload size in bytes
    this.maxPayloadSize = (options && options.maxPayloadSize) || MEGABYTES;

    // Optional argument for custom statuses.  The hashMap contains a status and its appropriate hash.
    if (options && options.hashMap) {
      this.hashMap = options.hashMap;
    }

    this.shutdownFlag = false;
    this.id = makeId();        // For debugging / logging

    // Intitializing connections to the dataStore and to redis
    var functions = [
      function(cb) {
        that.subClient = redis.initialize(options, cb);
      },
      function(cb) {
        that.pubClient = redis.initialize(options, cb);
      },
      function(cb) {
        that.dataClient = dataStore.initialize(options, cb);
      }
    ];

    var finalCallback = function() {
      // Determine role of this bus
      if (options && options.isWorker) {
        initResponder(that);
      } else {
        initCreator(that);
      }

      // Handle error events
      that.dataClient.on('taskError', function(evt) {
        common.wrapError(evt);
      });
      that.pubClient.on('taskError', function(evt) {
        common.wrapError(evt);
      });
      that.subClient.on('taskError', function(evt) {
        common.wrapError(evt);
      });
      if (callback) {
        callback();
      }
    };

    async.parallel(functions, finalCallback);


  };

  // TaskClient inherits from EventEmitter
  util.inherits(NotificationBus, EventEmitter);

  /**
   * Send a notification.  This method is used regardless of whether the notification is a new task, progress, or a
   * completion message.  The method writes the payload to the dataStore, and publishes a message notification to
   * redis PubSub.
   *
   * @param channel
   * @param id
   * @param message
   * @param status
   * @param callback
   */
  NotificationBus.prototype.sendNotification = function(channel, id, message, status, metadata, callback) {
    var that = this,
      msgString,
      pubMessage,
      hash,
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
      precond.checkArgument(channel);
      precond.checkArgument(id);
      precond.checkArgument(message);
      precond.checkArgument(status);
    } catch (e) {
      callback(new Error("Invalid Argument"));
      return;
    }

    // Determine that Node Background Task hasn't been terminated
    if (this.shutdownFlag) {
      callback(new Error("Attempt to use shutdown Notification Bus."));
      return;
    }

    if (status && getType.toString.call(status) === '[object Function]') {
      callback = status;
      status = "message";
    }

    // Add metadata to the message
    message._listenChannel = that.listenChannel; // Store the channel that will be used for responses
    message._messageId = id; // Store the message id
    message._status = status;  // Store the status of the message

    try {
      msgString = JSON.stringify(message);
    } catch (e) {
      callback(new Error("Error converting message to JSON."));
    }

    if (!msgString) {
      callback(new Error("Error converting message to JSON."));
      return;
    }

    // TODO: This needs to be an option for the class
    if (msgString.length > that.maxPayloadSize) {
      callback(new Error("The message has exceeded the payload limit of " + that.maxPayloadSize + " bytes."));
      return;
    }

    // Create the JSON message to publish
    pubMessage = {};
    pubMessage.id = id;
    pubMessage.status = status;
    pubMessage.metadata = metadata;

    // Determine the hash to write the payload to.  By default, is the status + baseHash, but can be
    // overriden.
    hash = that.hashMap && that.hashMap[status] ? that.hashMap[status] : status.toLowerCase() + this.baseHash;


    //Insert the key to the message store
    this.dataClient.messageStore.create(hash, id, msgString, metadata, function(err, reply) {
      if (err) {
        callback(new Error("Error sending message: " + err));
      } else {
        that.pubClient.publish(channel, JSON.stringify(pubMessage), function(error, res) {
          callback(error, that.listenChannel);
        });
      }
    });
  };

  /**
   * Process the notification and retrieve it from the messageStore
   *
   * @param id
   * @param status
   * @param callback
   */
  NotificationBus.prototype.processNotification = function(id, status, metadata, callback) {

    var that = this,
      hash,
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
      precond.checkArgument(id);
      precond.checkArgument(status);
    } catch (e) {
      callback(new Error("Invalid Arguments"));
      return;
    }

    // Determine the hash to write the payload to.  By default, is the status + baseHash, but can be
    // overriden.
    hash = that.hashMap && that.hashMap[status] ? that.hashMap[status] : status.toLowerCase() + this.baseHash;

    // Retrieve the payload from the message store
    that.dataClient.messageStore.process(hash, id, metadata, function(err, result) {
      common.processResult(err, result, callback);
    });
  };

  NotificationBus.prototype.cleanupTask = function(id, callback) {
    var that = this;
    that.processNotification(id, "NEWTASK", callback);
  };

  // Shutdown the bus
  NotificationBus.prototype.shutdown = function(callback) {
    var that = this;
    that.shutdownFlag = true;

    that.subClient.removeAllListeners();
    that.pubClient.removeAllListeners();
    that.dataClient.removeAllListeners();

    async.parallel([that.dataClient.shutdown.bind(that.dataClient), that.pubClient.shutdown.bind(that.pubClient), that.subClient.shutdown.bind(that.subClient)], function() {
      that.removeAllListeners();
      if (typeof callback === 'function') {
        callback();
      }
    });
  };

  return new NotificationBus(options, callback);
};

