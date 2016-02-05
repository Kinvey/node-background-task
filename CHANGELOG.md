## Changelog

### 0.9.1
* Pinned dependencies
* Updated license and dependency information

### 0.9.0
* Add support for broadcasting tasks

### 0.8.0
* Added passing of metadata that gets passed as an argument in TASK_AVAILABLE event

### 0.7.4
* Added some redis error handling, and handle addTask callback errors

### 0.7.3
* Changed redis_client callback to listen only once

### 0.7.2
* Delete callback in redis_client on first use, to work around redis bug where ready event is called twice

### 0.7.1
* Changed description and metadata

### 0.7.0
* Added a per-task broadcast channel option to addTask to allow for a single client to send to specific workers
* Cleaned up unit tests
* Refactored the initialization procedure to provide consistent callbacks
* Cleaned up error messages

### 0.6.8
* Clean up callbacks prior to calling any asynchronous functions on task response, and check for presence of callbacks before any response
* Added more error information on TaskLimit.stopTask precondition check

### 0.6.7
* Fixes #14 - Task Limit errors on stopTask were thrown rather than emitted
* Removes a race condition where a task timeout and task completion could both call TaskLimit.stopTask()
* Fixed some test case issues where tests were completing with asynchronous code still running

### 0.6.6
* Added new initialization option, maxPayloadSize, which sets the maximum size for a task message.

### 0.6.5
* Updated blacklist addFailure to avoid race condition where the blacklist counter ttl was set to -1
* Added a method to Task Server getBlacklistCount() to get a count of total # of blacklisted taskKeys

### 0.6.4
* Changed payload limit error message
* Changed use of version to API level to allow for mixed versions of the same level to exist
* Improved precondition error handling
* Added more informational error message for attempting to respond to a task that wasn't accepted

### 0.6.3
* Fix to drop progress notificaiton and ignore when an error occurs

### 0.6.2
* Fixed context issue when an error occurs processing a task
* Added cleanup of keys for when a task times out

### 0.6.1
* Several bug fixes
* Added test coverage for notification bus

### 0.6.0
* Complete refactor of Node Background Tasks. 
  * BREAKING CHANGE:  Ending the client and workers now use shutdown() command instead of end()
  * Refactored background_task to return sepearate objects, either a TaskClient or TaskServer, which each have their own methods
  * Removed messageBus and replaced with a data_store for reading/writing to logical data entites, and a notification_bus for message notifications
  * All redis interaction has been moved to redis_client
  * notification_bus only handles sending and processing of data.  The former methods to accept a task and complete a task have been pulled up into the task client and server
  * Concept of progressBus and messageBus have been removed and replaced with a single notification bus with different status.

### 0.5.3
* Make sure progressBus is ready when new task is received
* Changed initial sendMessage order

### 0.5.2
* Added some additional error logging
* Fixed issues with unit tests
* Upgrade to node_redis v0.8.6
* Modified test cases for auth check based on the fact that node_redis no longer returns an error message when a password is supploed but not required
* Added logging for progressBus.sendMessage callback to trap if an error occurs

### 0.5.1
* Fixed a leak of event listeners whenever an error was signaled.

### 0.5.0
* Added a taskTimeout to set the timeout value for an individual task
* Moved progress events to a seperate bus, updated tests.
* Improved test setup to get rid of test delays

### 0.4.2
* Fixed a leak of event listeners whenever an error was signaled (bug fix on 0.4.x branch, also added to 0.5.x branch version 0.5.1)

### 0.4.1
* Set to use redis module v0.8.3 due to change in how password auth works. 
* Publish only once when sedning multiple responses concurrently
* Publish even if any of the concurrent response-calls fail.

### 0.4.0
* Added support for reporting task progress
* Refactored complete- and progressTask methods.
* addTask now returns the task id.
* Fixed bug with TASK_DONE firing for all tasks when the first completes.  
* Added TASK_PROGRESS event
* Raised granularity of emitted progress event.
* Fixed task filter option.

### 0.3.4
* FIxed uncaught JSON parsing exception

### 0.3.3
* Fixed issue on limit cleanup to handle undefined key result

### 0.3.2
* Added task limit cleanup on start to remove orphaned taskKeys when an application fails
* Modified test descriptors to provide more detail
* Simplified test cases; refactored to make more clear
* Fixed some timeout issues in unit tests
* Added limit cleanup on start per hostname

### 0.3.1
* Fixed an issue where task_limit was being invoked, but still allowing BL to proceed.

### 0.3.0
* Added support for logging blacklist entries
* Updated test coverage

### 0.2.4
* Added a unit test for redis < 0.8.2 65k msg limit

### 0.2.3
* Force redis driver > 0.8.2

### 0.2.2
* Support redis 2.2

### 0.2.1
* Fixed bug with task-limits if there is a redis password problem.  TAsk limit is now an event emitter, so it can indicate errors
* Bacground-task updated to forward task-limit errors to the client
* Updated test coverage

### 0.2.0
* Prevent multiple workers from responding to the same task and trying to notify the task creator twice that they were successful.
* Improved tests
* No more leaking of event emitters
* More error handling
* Removal of emit('error') when callbacks are available

### 0.1.0
* Initial Release

