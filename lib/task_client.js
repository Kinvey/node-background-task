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
  util = require('util'),
  EventEmitter = require('events').EventEmitter,
  notification = require('./notification_bus'),
  task_limit = require('./task_limit'),
  blacklist = require('./blacklist'),
  precond = require('precond');

/**
 * The function initialize is called automatically by background_task, and configures and returns a TaskClient, used
 * for adding tasks and receiving responses.
 *
 * @param options
 * @returns {TaskClient}
 */
exports.initialize = function (options) {
  var TaskClient,
    extractResponse,
    makeTimeoutError;

  /**
   * TaskClient object - used as api for all initiation of background tasks
   *
   * @param options
   * @constructor
   */
  TaskClient = function (options) {
    // Call to EventEmitter constructor to intitialize arguments
    EventEmitter.call(this);

    if (!options) {
      options = {};
    }

    // Set up the default hashes
    if (options.task) {
      options.broadcast = options.task + "Broadcast";
      options.dataHash = options.task + "Table";
      options.outputHash = options.task + "Hash";
    }

    // TODO: consolodate to single map continaing an object
    // Initialize various maps
    this.completionCallbackMap = {}; // Contains map of taskId to callbacks for each task to call upon completion of task
    this.progressCallbackMap = {}; // Contains map of taskId to callbacks for each task to call upon progress update
    this.taskTimeouts = {}; // map of task timeouts
    this.msgMap = {}; // map containig the original task message

    // taskKey defines a grouping of tasks, and sets up limits (maximum concurrent tasks per key).
    if (options.taskKey) {
      this.taskKey = options.taskKey;
      if (!options.maxTasksPerKey) {
        this.maxTasksPerKey = options.maxTasksPerKey = 5;
      }

      // tasklimit object initialization.  TaskLimit is checked as each task is added to ensure that the maximum
      // for the taskKey is not exceeded.
      this.taskLimit = new task_limit.TaskLimit(options);
      this.taskLimit.on('error', common.wrapError(this));

      this.taskLimit.cleanupTasks();

      // blacklist object initialization.  Blacklists are time and counter based events that cause a particualr taskKey
      // to be temporarily blocked from submitting new tasks.
      this.blacklist = new blacklist.Blacklist(options);
      this.blacklist.on('error', common.wrapError(this));

    }

    this.timeout = 5000; // 5 second defualt timeout
    if (options && options.timeout) {
      this.timeout = options.timeout;
    }

    var that = this;

    // initialize the notification bus for sending/receiving messages between the client and server
    this.notificationBus = new notification.initialize(options);
    this.notificationBus.on('error', common.wrapError(this)); // sets up an error event handler

    // sets up an event listener for all notifications received on the listenerChannel for this TaskClient
    this.notificationBus.on('notification_received', function (task) {
      receiveTransaction(that, task);
    });

    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  // TaskClient inherits from EventEmitter
  util.inherits(TaskClient, EventEmitter);

  // Callback for notification_received events, this processes the message and returns the result back to the appropriate callback
  var receiveTransaction = function (that, task) {
    var taskCallback, status, id;

    if (!task.id) {
      that.emit('error', "Malformed task: " + task);
      cleanupMaps(that, task.id);
      return;
    }

    if (!task.status) {
      taskCallback = that.completionCallbackMap[task.id];
      if (taskCallback) {
        taskCallback(task.id, new Error("Missing Status for message"));
        cleanupMaps(that, task.id);
      }
      return;
    }
    status = task.status;
    id = task.id;

    // Determine which callback to call
    taskCallback = status === "PROGRESS" ? that.progressCallbackMap[id] : that.completionCallbackMap[id];

    // ProcessNotification to retrieve the data
    that.notificationBus.processNotification(id, status, function (err, result) {
      if (err) {
        // Silently drop progress events if an error occurred.
        if (status === "PROGRESS") {
          return;
        }

        that.taskLimit.stopTask(that.msgMap[id]);
        cleanupMaps(that, id);
        clearTimeout(that.taskTimeouts[id]);
        taskCallback(id, err);
        return;
      }

      var reply = extractResponse(result);

      if (status === "ERROR" || status === "SUCCESS" || status === "FAILED") {
        that.taskLimit.stopTask(that.msgMap[id]);
        clearTimeout(that.taskTimeouts[id]);
        cleanupMaps(that, id);
      }

      if (status === "SUCCESS") {
        that.emit('TASK_DONE', reply.id, reply.details);
      }

      if (status === "PROGRESS") {
        that.emit('TASK_PROGRESS', reply.id, reply.details);
      }

      if (taskCallback) {
        taskCallback(reply.id, reply.details);
      }
    });
  };


  /**
   * Shutdown the TaskClient
   */
  TaskClient.prototype.shutdown = function () {
    var that = this;

    if (!that.isAvailable) {
      return; // Nothing to do
    }

    that.isAvailable = false;
    // Hard end, don't worry about shutting down
    // gracefully here...
    if (that.blacklist) {
      that.blacklist.shutdown();
    }
    if (that.taskLimit) {
      that.taskLimit.shutdown();
    }
    that.notificationBus.shutdown();
    that.removeAllListeners();
  };

  /**
   * Adds a new task to be processed by a worker.
   *
   * @param msg Message to be sent
   * @param options object of task options
   * @param completionCallback Callback function to be called upon task completion
   * @param progressCallback Callback function to be called when reporting progress
   * @returns {*}
   */
  TaskClient.prototype.addTask = function (msg, options, completionCallback, progressCallback) {
    var that = this,
      id = notification.makeId(),
      timedoutCb,
      msgToSend,
      tmpErr,
      startTheTask,
      taskTimeout,
      getType = {};

    // Check if options are included, if not shuffle callbacks to the appropriate argument
    if (options && getType.toString.call(options) === '[object Function]') {
      progressCallback = completionCallback;
      completionCallback = options;
      options = {};
    }

    // Set empty default progressCallback if it is not passed
    if (!progressCallback) {
      progressCallback = function () {
      };
    }

    try {
      precond.checkArgument(msg, "Task must contain a message");
    } catch (e) {
      that.emit('TASK_ERROR', e);
      completionCallback(id, e);
      return;
    }

    // Set the task timeout length if it is included in the task options
    taskTimeout = (options && options.taskTimeout && options.taskTimeout > 0) ? options.taskTimeout : that.timeout;

    // Error if client is not available
    if (!that.isAvailable) {
      completionCallback(id, new Error("Attempt to use invalid BackgroundTask"));
      return id;
    }


    startTheTask = function () {
      // Check the limit for the task key to see if the task can proceed
      that.taskLimit.startTask(msg, function (tasks) {

        var err;
        if (tasks instanceof Error) {
          err = new Error("Too many tasks");
          that.emit('TASK_ERROR', err);
          completionCallback(id, err);
          return;
        }

        // Store callbacks and original message in maps
        that.completionCallbackMap[id] = completionCallback;
        that.progressCallbackMap[id] = progressCallback;
        that.msgMap[id] = msg;

        // Timeout callback, if task exceeds the timeout
        timedoutCb = function () {
          var origCallback = that.completionCallbackMap[id];
          cleanupMaps(that, id);

          // Return an error
          that.taskLimit.stopTask(msg);
          origCallback(id, makeTimeoutError());
        };

        msgToSend = {
          taskId: id,
          taskDetails: msg
        };

        // Set the task timeout
        that.taskTimeouts[id] = setTimeout(timedoutCb, taskTimeout);

        // Broadcast a notification to a server
        that.notificationBus.sendNotification(that.notificationBus.broadcastChannel, id, msgToSend, "NEWTASK", function () {

        });
      });
    };

    // Check to see if the taskKey is blacklisted before executing the task
    if (that.taskKey && msg[that.taskKey]) {
      that.blacklist.blacklistStatus(msg, function (isBlacklisted, timeLeft, reason) {
        var tmpErr;
        if (isBlacklisted) {
          tmpErr = new Error('Blacklisted');
          tmpErr.debugMessage = "Blocked, reason: " + reason + ", remaining time: " + timeLeft;
          that.emit('TASK_ERROR', tmpErr);
          completionCallback(id, tmpErr);
        } else {
          startTheTask();
        }
      });
    } else {
      tmpErr = new Error('Missing taskKey');
      that.emit('TASK_ERROR', tmpErr);
      completionCallback(id, tmpErr);
    }
    return id;

  };

  // Helper method to extract the response
  extractResponse = function (r) {
    var response, id, respondedErr, o;

    if (!r.taskId && !r.taskDetails) {
      throw new Error("Incomplete task response.");
    }

    id = r.taskId;
    response = r.taskDetails;

    if (response.isError) {
      respondedErr = new Error(response.message);
      for (o in response) {
        if (response.hasOwnProperty(o) &&
          o !== 'isError' &&
          o !== 'message') {
          respondedErr[o] = response[o];
        }
      }
      response = respondedErr;
    }

    return {id: id, details: response};
  };

  /**
   * Cleanup map values for a particular task once the task is complete or has failed with an error.
   *
   * @param that Current context
   * @param id TaskId to remove
   */
  var cleanupMaps = function (that, id) {
    delete that.completionCallbackMap[id];
    delete that.progressCallbackMap[id];
    delete that.taskTimeouts[id];
    delete that.msgMap[id];
  };

  makeTimeoutError = function () {
    return new Error('Task timed out');
  };

  return new TaskClient(options);
};