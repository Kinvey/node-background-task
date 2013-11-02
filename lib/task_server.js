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

var common = require('./common'),
  EventEmitter = require('events').EventEmitter,
  notification = require('./notification_bus'),
  util = require('util'),
  async = require('async'),
  precond = require('precond');

/**
 * The function initialize is called automatically by background_task, and configures and returns a TaskServer, used
 * for accepting tasks, sending progress, and sending final completion messages.
 *
 * @param options
 * @returns {TaskServer}
 */
exports.initialize = function (options) {

  var TaskServer;

  /**
   * TaskServer object - API for responding to task requests
   *
   * @param options
   * @constructor
   */
  TaskServer = function (options) {
    // Call to EventEmitter constructor to intitialize arguments
    EventEmitter.call(this);

    if (!options) {
      options = {};
    }

    // Set up a map of task Ids to the notificaiton channel to send responses on
    this.idToChannelMap = {};

    if (options.task) {
      options.broadcast = options.task + "Broadcast";
      options.dataHash = options.task + "Table";
      options.outputHash = options.task + "Hash";
    }

    if (options.taskKey) {
      this.taskKey = options.taskKey;
      if (!options.maxTasksPerKey) {
        this.maxTasksPerKey = options.maxTasksPerKey = 5;
      }

    }

    var that = this;

    // initialize the notification bus for sending/receiving messages between the client and server
    this.notificationBus = new notification.initialize(options);
    this.notificationBus.on('error', common.wrapError(this));

    // sets up an event listener for all notifications received on the broadcastChannel
    this.notificationBus.on('notification_received', function (id) {
      that.emit('TASK_AVAILABLE', id.id);
    });

    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  // TaskClient inherits from EventEmitter
  util.inherits(TaskServer, EventEmitter);

  /**
   * Shutdown the TaskServer
   */
  TaskServer.prototype.shutdown = function () {
    var that = this;

    if (!that.isAvailable) {
      return; // Nothing to do
    }

    that.isAvailable = false;
    // Hard end, don't worry about shutting down
    // gracefully here...

    that.notificationBus.shutdown();

    that.removeAllListeners();
  };

  /**
   * Attempts to accept a task for processing
   *
   * @param id The task Id
   * @param callback Passes the task data or an error
   */
  TaskServer.prototype.acceptTask = function (id, callback) {
    var that = this,
      message,
      status;

    if (!callback) {
      callback = function() {};
    }

    // Since it is an accepted task, assuming NEWTASK as the status
    status = "NEWTASK";

    if (!that.isAvailable) {
      callback(new Error("Attempt to use invalid BackgroundTask"));
      return;
    }

    if (!id || id.length === 0) {
      throw new Error('Missing Task ID.');
    }

    if (!callback || callback.length < 1) {
      throw new Error('Invalid callback specified');
    }

    // Process the notification - retrieve it for use by the calling worker application
    that.notificationBus.processNotification(id, status, function (err, reply) {
      if (err) {
        if (err.message === "DB doesn't recognize message" || err.message.match(/^No message for id/) || err.message.match(/^No message for key/)) {
          console.log("here");
          callback(new Error('Task not in database, do not accept'));
          return;
        } else {
          callback(err);
          return;
        }
      }

      // Add the listener channel from the task data to the channel map in order to publish the response later

      that.idToChannelMap[id] = reply._listenChannel;

      if (reply.taskDetails) {
        callback(reply.taskDetails);
      } else {
        callback(reply);
      }
    });
  };

  /**
   * Send a response (whether it be progress or a completion message) back to the TaskClient.
   *
   * @param taskId
   * @param status
   * @param msg
   */
  TaskServer.prototype.reportTask = function (taskId, status, msg) {
    var that = this,
      msgToSend,
      serializableError,
      o;

    if (!taskId || !status || !msg) {
      throw new Error("Missing msgId, status or msg.");
    }

    if (!that.isAvailable) {
      throw new Error("Attempt to use invalid BackgroundTask");
    }

    // We can't send Error's via JSON...
    if (msg instanceof Error) {
      serializableError = {
        isError: true,
        message: msg.message
      };

      for (o in msg) {
        if (msg.hasOwnProperty(o)) {
          serializableError[o] = msg[o];
        }
      }

      msg = serializableError;
    }


    msgToSend = {
      taskId: taskId,
      taskDetails: msg
    };

    if (!msg) {
      throw new Error('Missing msgId, status or msg.');
    }

    that.notificationBus.sendNotification(that.idToChannelMap[taskId], taskId, msgToSend, status, function (err, result) {
    });
  };

  /**
   *
   * Wrapper method for reportTask to send a completion message back to the TaskClient
   *
   * @param taskId
   * @param status
   * @param msg
   */
  TaskServer.prototype.completeTask = function (taskId, status, msg) {
    if (status !== "SUCCESS" && status !== "ERROR" && status !== "FAILED") {
      throw new Error(status + " is not a valid status for completeTask.");
    }
    this.reportTask(taskId, status, msg);
  };

  /**
   * Wrapper method for reportTask to send progress back to the TaskClient
   *
   * @param taskId
   * @param msg
   */
  TaskServer.prototype.progressTask = function (taskId, msg) {
    this.reportTask(taskId, 'PROGRESS', msg);
  };

  return new TaskServer(options);

};