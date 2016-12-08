[![kinvey logo](http://www.kinvey.com/images/logo/300.png)](http://www.kinvey.com)

node-background-task
====================

Distributed task execution framework for [node](http://nodejs.org), backed by [redis](http://redis.io/).
[![Build Status](https://travis-ci.org/Kinvey/node-background-task.png)](https://travis-ci.org/Kinvey/node-background-task)

```js
var bgTask = require('background-task').connect({ taskKey: 'someKey' })
  , task;

task = {
  someKey      : 'someValue',
  someOtherKey : ['someValues', 'someMoreValues']
};

bgTask.addTask(task, function(resp){
    console.log(resp);
});
```

## Migrating from v1 to v2

v2 of NBT has switched from using the `node_redis` module to using `ioredis`.  The `host`, `password`, and `port` keys in the
options object that is used when initializing a client/server are deprecated.  Use `redisOptions` instead.

The `shutdown` method now contains an optional callback.

## Installation

    $ npm install background-task

## Quick Start

Background tasks involve two concepts, creators and workers.  Creators
have tasks that they need executed, workers know how to execute
tasks. Workers listen for events tagged with a specific *task* and
execute them. Should no worker execute the task in a specific time
(which can be customized) then a failure is reported back.

Additionally, the number of active tasks can be limited using a "task
key" (which must be a property at the top level of you task object),
where only N tasks can be pending on a certain key prior to failures.

### Task Creators

* `connect(options)` -- Creates a new instance of a BackgroundTask, allowing you
  to specify options.
* `addTask(task, callback[, progress])` -- Schedule a task for background
  execution, calling progress on task progress and `callback when complete.


### Task Workers

Processing for a worker is kicked off by the `TASK_AVAILABLE` event,
once you get this event you can either accept the task, or ignore the
task.  If multiple workers accept the task then the first to accept
will handle the task.

* `connect(options)` -- Create a new instance of a BackgroundTask,
  allowing you to specify options.  You must specify `isWorker: true`
  to register as a background worker.
* `acceptTask(callback)` -- Accepts a task from the queue to start processing,
  callback must accept two args, `id` and a paramater for the task.
* `completeTask(taskId, status, msg)` -- Mark a task as complete.
* `progressTask(taskId, msg)` -- Report task progress.
* `shutdown(callback` -- Shuts down NBT


### Options

The `options` hash for creators and workers allows you to set:

* `redisOptions` -- The redis options object.  Supports all options used by [ioredis v2.4.2](https://github.com/luin/ioredis/blob/v2.4.2/API.md#new-redisport-host-options)
* `maxTasksPerKey` -- limit the amount of active tasks based on the "task key".
* `taskKey` -- the task key.
* `task` -- listen for specific tasks only.
* `timeout` -- task timeout (in ms).

The following options are deprecated and will be removed in the next major release of this library.  Use `redisOptions` instead.

* **DEPRECATED** `password` -- redis password.
* **DEPRECATED** `host` -- redis host.
* **DEPRECATED** `port` -- redis port.

### Events

node-background-task uses the following events:

* `TASK_AVAILABLE` -- There is data available for a background worker.
* `TASK_DONE` -- A task has finished and the response is ready, task
  may not be successful, just complete.
* `TASK_PROGRESS` -- Progress is reported for a task. 
* `TASK_ERROR` -- Something went wrong.


## Features

* Lightweight
* Backed by [redis](http://redis.io/) (Can be portable to other DBs)
* Callback and Event based
* Easy to build distributed architectures
* Workers are event driven, not polling
* Task limiting

## Why?

We looked at the excellent
[coffee-resque](https://github.com/technoweenie/coffee-resque)
project to fit an internal need, however we needed a non-polling based
worker architecture.  There are up and down-sides to this approach,
but the balance was correct for the architecture that we're creating
at [Kinvey](http://www.kinvey.com).

## License

Copyright 2013 Kinvey, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
