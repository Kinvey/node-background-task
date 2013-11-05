/*global describe, it, beforeEach, afterEach */

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

var sinon = require('sinon')
  , util = require('util')
  , moment = require('moment')
  , background_task = require('../lib/background_task')
  , should = require('should')
  , redis = require('redis')
  , async = require('async')
  , delay = 30 // This allows object creation to always finish
  , testUtils = require('./test-utils')
  , blacklist = require('../lib/blacklist') // Needed for timeouts and keys
  , longTaskTimeoutTime = 10000
  , longMochaTimeoutTime = 15000
  , zeroTimeoutTime = 0
  , negativeTimeoutTime = -1
  , timeoutMarginOfError = 100
  , normalTimeoutTime = 1000
  , ll = function(m){
      var d = new Date()
      , t = d.toISOString();
      util.debug(t + ": " + m);
  };

describe('node-background-task', function(){
    var bgTask, bgTaskWorker
      , rc = redis.createClient();

    beforeEach(function(done) {
        rc.flushall();
        bgTask       = background_task.connect({taskKey: "kid", maxTasksPerKey: 2});
        bgTaskWorker = background_task.connect({isWorker: true});

        // Wait until setup is complete.
        var pending = 2;
        var next    = function() {
            pending -= 1;
            if(0 === pending) {
                done();
            }
        };
        testUtils.waitForSetup(bgTask, next);
        testUtils.waitForSetup(bgTaskWorker, next);
    });

    afterEach(function() {
        if(bgTask) {
            bgTask.shutdown();
        }
        if(bgTaskWorker) {
            bgTaskWorker.shutdown();
        }
    });

    describe('Events', function(){
        describe('#emit()', function(){
            it('should invoke the callback', function(){
                var spy = sinon.spy();


                bgTask.on('foo', spy);
                bgTask.emit('foo');
                spy.called.should.be.true;
            });

            it('should emit TASK_DONE when a task completes', function(done){
                var tid;
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        tid = id;
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });

                bgTask.on('TASK_DONE', function(id, reply){
                    id.should.equal(tid);
                    reply.should.eql({kid: "kidEmitTaskDone", body: "test"});
                    done();
                });

                bgTask.addTask({kid: "kidEmitTaskDone", body: "test"}, function(){});
            });

            // Test for bugfix where TASK_DONE was emitted for all tasks at once
            // when one task completes, instead of when each task actually
            // completes.
            it('should emit TASK_DONE for each task when it completes', function(done) {
                var completed = 0, pending = 2;

                // Complete tasks with some delay between them.
                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        setTimeout(function() {
                            completed += 1;
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay);
                    });
                });

                bgTask.on('TASK_DONE', function() {
                    pending -= 1;
                    if(0 === pending) {
                        completed.should.equal(2);
                        done();
                    }
                });

                // Add two tasks.
                bgTask.addTask({ kid: 'kidEmitTaskDone' }, function() { });
                bgTask.addTask({ kid: 'kidEmitTaskDone' }, function() { });
            });

            it('should emit TASK_AVAILABLE when a task is added', function(done){
                bgTaskWorker.on('TASK_AVAILABLE',  function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        done();
                    });
                });

                bgTask.addTask({kid: "emitTaskAvailable", body: "test"}, function(){});
            });

            it('should emit TASK_PROGRESS when task progress is reported', function(done) {
                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        bgTaskWorker.progressTask(id, msg);
                        setTimeout(function() {
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay);
                    });
                });

                var spy = sinon.spy();
                bgTask.on('TASK_PROGRESS', spy);

                bgTask.addTask({ kid: 'emitTaskProgress' }, function() {
                    spy.callCount.should.equal(1);
                    done();
                });
            });

            it('should emit TASK_ERROR if something goes wrong', function(done){
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(reply){
                        bgTaskWorker.completeTask(id, reply);
                    });
                });

                bgTask.on('TASK_ERROR', function() {
                    done();
                });

                bgTask.addTask('', function(id, d){
                    d.should.be.an.instanceOf(Error);
                });
            });

            it('should handle an error', function(done){
                var mm = 'I can haz cheezburger'
                  , dm = 'I like turtles';
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg) {
                        var err = new Error(mm);
                        err.debugMessage = dm;
                        bgTaskWorker.completeTask(id, 'FAILED', err);
                    });
                });

                bgTask.on('error', function(){});
                bgTask.addTask({kid: "handle error", body: "test"}, function(id, reply){
                    reply.should.be.an.instanceOf(Error);
                    reply.message.should.equal(mm);
                    reply.debugMessage.should.equal(dm);
                    done();
                });
            });

            it('should have the task when TASK_AVAILABLE is emitted', function(done){
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        should.exist(id);
                        msg.should.eql({kid: "should have task", body: "test"});
                        done();
                    });
                });

                bgTask.addTask({kid: "should have task", body: "test"}, function(){});
            });
        });
    });

    describe('#initialize', function(){
        it('should return a valid TaskClient with no options', function(){
            var task = background_task.connect();
            task.should.be.an.instanceOf(Object);
            task.addTask.should.be.an.instanceOf(Function);
            should.not.exist(task.acceptTask);
            should.not.exist(task.reportTask);
            should.not.exist(task.reportBadTask);
            should.not.exist(task.completeTask);
            should.not.exist(task.progressTask);
            task.shutdown();
        });

        it('should return a valid TaskClient with all options', function() {
          var task = background_task.connect({
            task: "hey",
            taskKey: "kid",
            queue: "someNewQueue",
            outputHash: "someOutputHash",
            host: "0.0.0.0",
            port: "6379",
            isWorker: false
          });
          task.should.be.an.instanceOf(Object);
          task.addTask.should.be.an.instanceOf(Function);
          should.not.exist(task.acceptTask);
          should.not.exist(task.reportTask);
          should.not.exist(task.reportBadTask);
          should.not.exist(task.completeTask);
          should.not.exist(task.progressTask);
          task.shutdown();

        });

      it('should return a valid TaskClient with some options', function() {
        var task = background_task.connect({
          queue: "newQueue",
          host: "localhost",
          isWorker: false
        });
        task.should.be.an.instanceOf(Object);
        task.addTask.should.be.an.instanceOf(Function);
        should.not.exist(task.acceptTask);
        should.not.exist(task.reportTask);
        should.not.exist(task.reportBadTask);
        should.not.exist(task.completeTask);
        should.not.exist(task.progressTask);
        task.shutdown();

      });

        it('should return a valid TaskServer with no options', function(){
          var task = background_task.connect({isWorker: true});
          task.should.be.an.instanceOf(Object);
          should.not.exist(task.addTask);
          task.acceptTask.should.be.an.instanceOf(Function);
          task.reportTask.should.be.an.instanceOf(Function);
          task.reportBadTask.should.be.an.instanceOf(Function);
          task.completeTask.should.be.an.instanceOf(Function);
          task.progressTask.should.be.an.instanceOf(Function);
          task.shutdown();
        });

        it('should return a valid TaskServer with all options', function(){
            var task = background_task.connect({
                task: "hey",
                taskKey: "kid",
                queue: "someNewQueue",
                outputHash: "someOutputHash",
                host: "0.0.0.0",
                port: "6379",
                isWorker: true
            });
            task.should.be.an.instanceOf(Object);
            should.not.exist(task.addTask);
            task.acceptTask.should.be.an.instanceOf(Function);
            task.reportTask.should.be.an.instanceOf(Function);
            task.reportBadTask.should.be.an.instanceOf(Function);
            task.completeTask.should.be.an.instanceOf(Function);
            task.progressTask.should.be.an.instanceOf(Function);
            task.shutdown();
        });


        it('should return a valid TaskServer with some options', function(){
            var task = background_task.connect({
                queue: "newQueue",
                host: "localhost",
                isWorker: true
            });
            task.should.be.an.instanceOf(Object);
            should.not.exist(task.addTask);
            task.acceptTask.should.be.an.instanceOf(Function);
            task.reportTask.should.be.an.instanceOf(Function);
            task.reportBadTask.should.be.an.instanceOf(Function);
            task.completeTask.should.be.an.instanceOf(Function);
            task.progressTask.should.be.an.instanceOf(Function);
            task.shutdown();
        });

        it('should be a TaskServer when isWorker: true', function(done){
            bgTaskWorker.on('TASK_AVAILABLE', function(id){
                bgTaskWorker.acceptTask(id, function(msg){
                    bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    done();
                });
            });

            bgTask.addTask({kid: "should be worker", body: "test"}, function(){});
        });

        it('should filter tasks when the task option is set.', function(done) {
          var taskName = 'myTask';
          var bgTask   = background_task.connect({
            task    : taskName,
            taskKey : 'kid'
          });
          var bgTaskWorker1 = background_task.connect({
            task     : taskName,
            isWorker : true
          });
          var bgTaskWorker2 = background_task.connect({
            task     : 'myOtherTask',
            isWorker : true
          });

          // On failure, `done` will be invoked once by each worker.
          var count = 0;
          bgTaskWorker1.on('TASK_AVAILABLE', function(id) {
            count += 1;
            bgTaskWorker1.acceptTask(id, function(msg) {
              bgTaskWorker1.completeTask(id, 'SUCCESS', msg);
            });
          });
          bgTaskWorker2.on('TASK_AVAILABLE', function() {
            count += 1;
          });

          // The task below should only be available to `bgTaskWorker1`.
          bgTask.addTask({ kid: 'task' }, function() {
            // Cleanup.
            bgTask.shutdown();
            bgTaskWorker1.shutdown();
            bgTaskWorker2.shutdown();

            count.should.equal(1, 'Task was available to both workers.');
            done();
          });
        });

    });

    describe('BackgroundTask', function(){

        describe('#end', function(){
            it('should not allow more tasks to complete', function(done){
                var t = background_task.connect({taskKey: "hi"});
                t.shutdown();
                t.addTask({hi: "test"}, function(id, v){
                    v.should.be.an.instanceOf(Error);
                    v.message.should.equal('Attempt to use invalid BackgroundTask');
                    done();
                });
            });
        });

        describe('#addTask', function(){
            it('should return the task id', function(done) {
                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });

                var id = bgTask.addTask({ kid: 'should return id' }, function(theId) {
                    theId.should.equal(id);
                    done();
                });
            });

            it('should call callback', function(done){
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });

                bgTask.addTask({kid: "should call callback", body: "test"}, function() {
                    done();
                });
            });

            it('should timeout if timeout value exceeded', function(done){
                var task = background_task.connect({taskKey: "kid", timeout: 200});

                task.addTask({kid: "should timeout", body: "test"}, function(id, reply){
                    reply.should.be.an.instanceOf(Error);
                    reply.message.should.equal('Task timed out');
                    task.shutdown();
                    done();
                });
            });

          it('should use default timeout value if no task timeout value is passed', function(done){
            var task = background_task.connect({taskKey: "kid", timeout: normalTimeoutTime});
            var start = moment();

            task.addTask({kid: "should timeout", body: "test"}, function(id, reply){
              var diff = moment().diff(start);
              diff.should.be.approximately(normalTimeoutTime, timeoutMarginOfError);
              reply.should.be.an.instanceOf(Error);
              reply.message.should.equal('Task timed out');
              task.shutdown();
              done();
            });

          });

          it('should allow specific timeout to be passed as part of a task', function(done){
            this.timeout(longMochaTimeoutTime);
            var task = background_task.connect({taskKey: "kid", timeout: 200});
            var start = moment();

            task.addTask({kid: "should timeout", body: "test"}, {taskTimeout: longTaskTimeoutTime}, function(id, reply){
              var diff = moment().diff(start);
              diff.should.be.approximately(longTaskTimeoutTime, timeoutMarginOfError);
              reply.should.be.an.instanceOf(Error);
              reply.message.should.equal('Task timed out');
              task.shutdown();
              done();
            });

          });

          it('should use default timeout if a task timeout of zero is passed', function(done){
            var task = background_task.connect({taskKey: "kid", timeout: normalTimeoutTime});
            var start = moment();

            task.addTask({kid: "should timeout", body: "test"}, {taskTimeout: zeroTimeoutTime}, function(id, reply){
              var diff = moment().diff(start);
              diff.should.be.approximately(normalTimeoutTime, timeoutMarginOfError);
              reply.should.be.an.instanceOf(Error);
              reply.message.should.equal('Task timed out');
              task.shutdown();
              done();
            });
          });

          it('should use default timeout if a task timeout with a negaitve integer is passed', function(done){
            var task = background_task.connect({taskKey: "kid", timeout: normalTimeoutTime});
            var start = moment();

            task.addTask({kid: "should timeout", body: "test"}, {taskTimeout: negativeTimeoutTime}, function(id, reply){
              var diff = moment().diff(start);
              diff.should.be.approximately(normalTimeoutTime, timeoutMarginOfError);
              reply.should.be.an.instanceOf(Error);
              reply.message.should.equal('Task timed out');
              task.shutdown();
              done();
            });
          });

            it('should not call callback twice if timeout value exceeded (Will fail with double done() if code is broken)', function(done){
                var task = background_task.connect({taskKey: "kid", timeout: delay});
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(r){
                        if (util.isError(r)){
                            ll("ERROR!!!");
                        } else {
                            setTimeout(function(){
                                bgTaskWorker.completeTask(id, 'SUCCESS', r);
                                task.shutdown();
                                done();
                            }, delay+1);
                        }
                    });
                });

                task.addTask({kid: 'double callback'}, function(id, r){
                    if (util.isError(r)){
                        r.should.be.an.instanceOf(Error);
                        r.message.should.equal('Task timed out');
                    } else {
                        // This will blow up if it gets called
                        done();
                    }
                });
            });


            it('should reject tasks over key threshold', function(done){
                // Need to send three tasks and make sure the third is rejected
                var t1, t2, t3;

                t1 = function(){
                    bgTask.addTask({kid: "keyT", task: 1}, function(id, r){

                    });
                };

                t2 = function(){
                    bgTask.addTask({kid: "keyT", task: 2}, function(id, r){

                    });
                };


                t3 = function(){
                    bgTask.addTask({kid: "keyT", task: 3}, function(id, r){
                        r.should.be.an.instanceOf(Error);
                        r.message.should.equal("Too many tasks");
                        done();
                    });
                };


                setTimeout(t1, 5);
                setTimeout(t2, 10);
                setTimeout(t3, 20);

            });

            it('should allow for multiple tasks to be added', function(done){
                // Two tasks should be able to complete
                var count = 2, cb;

                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(d){
                        bgTaskWorker.completeTask(id, 'SUCCESS', d);
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "multi"}, function(id, v){
                        v.should.be.eql({kid: "multi"});
                        count = count - 1;
                        if (count === 0){
                            done();
                        }
                    });
                };
                cb();
                cb();
            });

            it('should not add a task with a blacklisted taskkey', function(done){
                var key = "kid_aaaa";
                var task = {taskKey: key};
                var bl = new blacklist.Blacklist(task);
                var count = bl.blacklistThreshold + 1;
                async.timesSeries(count, function(n, next){
                    bl.addFailure(key, "Testing failure", function(reason){
                        process.nextTick(next);
                    });
                }, function(err, _) {
                    bgTask.addTask({kid: key}, function(id, v){
                        v.should.be.an.instanceOf(Error);
                        v.message.should.eql("Blacklisted");
                        v.debugMessage.should.eql("Blocked, reason: Testing failure, remaining time: 3600");

                        done();
                    });
                });

            });

            it('should call progress callback', function(done) {
                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        bgTaskWorker.progressTask(id, msg);
                        setTimeout(function() {
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay);
                    });
                });

                var spy = sinon.spy();
                bgTask.addTask({ kid: 'should call callback' }, function(){
                  spy.callCount.should.equal(1);
                  done();
                }, spy);
            });
            it('should call progress callback multiple times', function(done) {
                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        bgTaskWorker.progressTask(id, msg);
                        setTimeout(function() {
                            bgTaskWorker.progressTask(id, msg);
                            setTimeout(function() {
                                bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                            }, delay);
                        }, delay);
                    });
                });

                var spy = sinon.spy();
                bgTask.addTask({ kid: 'should call callback' }, function(){
                  spy.callCount.should.equal(2);
                  done();
                }, spy);
            });
            it('should not call progress callback if timeout value exceeded', function(done) {
                var task = background_task.connect({ taskKey: 'kid', timeout: delay });

                bgTaskWorker.on('TASK_AVAILABLE', function(id) {
                    bgTaskWorker.acceptTask(id, function(msg) {
                        setTimeout(function() {
                            // At this point, the task should already have been
                            // aborted, so the method below should throw
                            // "Attempt to use invalid BackgroundTask".
                            try {
                                bgTaskWorker.progressTask(id, msg);
                            }
                            catch(e) { }
                        }, delay + 1);
                    });
                });

                var spy = sinon.spy();
                task.addTask({ kid: 'should timeout' }, function(id, reply) {
                    spy.callCount.should.equal(0);
                    reply.should.be.an.instanceOf(Error);
                    reply.message.should.equal('Task timed out');
                    task.shutdown();
                    done();
                }, spy);
            });
            it('should allow for multiple tasks to have progress callbacks', function(done) {
                // Two tasks should be able to complete.
                var count = 2;

                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg) {
                        bgTaskWorker.progressTask(id, msg);
                        setTimeout(function() {
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay);
                    });
                });

                var spy = sinon.spy();

                var cb = function() {
                    bgTask.addTask({ kid: 'multi' }, function(id, reply) {
                        reply.should.be.eql({ kid: 'multi' });

                        count -= 1;
                        if(0 === count) {
                            spy.callCount.should.equal(2);
                            done();
                        }
                    }, spy);
                };

                cb();
                cb();
            });
        });

        describe('#acceptTask', function(){
            it('should not be called without parameters', function(){
                (function() {
                    bgTaskWorker.acceptTask(null, null);
                }).should.throw('Missing Task ID.');
            });
            it('should ensure that task id is a parameter', function(){
                (function() {
                    bgTaskWorker.acceptTask(null, function(v){});
                }).should.throw('Missing Task ID.');

                (function() {
                    bgTaskWorker.acceptTask("", function(v){});
                }).should.throw('Missing Task ID.');

            });
            it('should ensure that callback is valid', function(){
                (function() {
                    bgTaskWorker.acceptTask("0xdeadbeef", null);
                }).should.throw('Invalid callback specified');

                (function() {
                    bgTaskWorker.acceptTask("0xdeadbeef", function(){});
                }).should.throw('Invalid callback specified');
              });

            it('should only work once per message for multiple workers', function(done){
                var btw = background_task.connect({isWorker: true})
                  , count = 2;

                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        if (msg instanceof Error){
                            msg.message.should.equal('Task not in database, do not accept');
                        } else {
                            count = count - 1;
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }
                    });
                });

                btw.once('TASK_AVAILABLE', function(id){
                    btw.acceptTask(id, function(msg){
                        if (msg instanceof Error){
                            msg.message.should.equal('Task not in database, do not accept');
                        } else {
                            count = count - 1;
                            btw.completeTask(id, 'SUCCESS', msg);
                        }
                    });
                });

                testUtils.waitForSetup(btw, function() {
                    bgTask.addTask({kid: "only called once", body: "test"}, function(id, reply){
                        count.should.equal(1);
                        btw.shutdown();
                        done();
                    });
                });
            });
        });

      describe('#reportTask', function() {
        var msg = {kid: "reportTask", body: "test"};
         it('should reject responding to tasks that were not accepted', function(){

          (function(){
            bgTaskWorker.reportTask("123456", "SUCCESS", msg);
         }).should.throw('Attempt to respond to message that was never accepted');
        });
      });

        describe('#completeTask', function(){
            var msg = {kid: "completeTask", body: "test"};
            it('it should reject tasks without ids', function(){
                (function() {
                    bgTaskWorker.completeTask(null, 'SUCCESS', msg);
                }).should.throw('Missing msgId, status or msg.');
            });

            it('it should reject tasks without a status', function(){
                (function() {
                    bgTaskWorker.completeTask("12345", undefined, msg);
                }).should.throw(/is not a valid status/);
            });

            it('it should reject tasks without message', function(){
                (function() {
                    bgTaskWorker.completeTask("12345", 'SUCCESS', null);
                }).should.throw('Missing msgId, status or msg.');
            });

            it('should accept only SUCCESS, ERROR or FAILED for status', function(){
                var id = Date()
                  , msg = {body: 'hi mom'}
                  , allowed = ['SUCCESS', 'ERROR', 'FAILED']
                  , notAllowed = ['GOOD', 1, 21.2, {test: "object"}, 'S', true]
                  , i;

                for (i = 0; i < allowed.length; i = i + 1){
                    (function(){
                        bgTaskWorker.idToChannelMap[id] = "listenerChannel";
                        bgTaskWorker.completeTask(id, allowed[i], msg);
                    }).should.not.throw();
                }

                for (i = 0; i < notAllowed.length; i = i + 1){
                    (function(){
                        bgTaskWorker.completeTask(id, notAllowed[i], msg);
                    }).should.throw(/is not a valid status/);
                }
            });
        });

        describe('#reportBadTask', function(){
            // NB: Non-integration tests should be handled by test-blacklist.js
            it('should be able to blacklist a task', function(done){
                var key = "kid_blacklist";
                var task = {taskKey: key};
                var bl = new blacklist.Blacklist(task);
                var count = bl.blacklistThreshold + 1;
                async.timesSeries(count, function(n, next){
                    bl.addFailure(key, "Testing failure", function(reason){
                        process.nextTick(next);
                    });
                }, function(err, _) {
                    bgTask.addTask({kid: key}, function(id, v){
                        v.should.be.an.instanceOf(Error);
                        v.message.should.eql("Blacklisted");
                        v.debugMessage.should.eql("Blocked, reason: Testing failure, remaining time: 3600");
                        done();
                    });
                });
            });

            it('should reset after a time interval', function(done){
                var key = "kid_reset";
                var task = {taskKey: key};
                var bl = new blacklist.Blacklist(task);
                var count = bl.blacklistThreshold;

                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(d){
                        bgTaskWorker.completeTask(id, 'SUCCESS', d);
                    });
                });

                async.timesSeries(count, function(n, next){
                    bl.addFailure(key, "Testing failure", function(reason){
                        process.nextTick(next);
                    });
                }, function(err, result) {
                    setTimeout(function(){
                        async.timesSeries(count, function(n, next){
                            bl.addFailure(key, "Testing failure", function(reason){
                                process.nextTick(next);
                            });
                        }, function(err, _){
                            bgTask.addTask({kid: key}, function(id, v){
                                v.should.not.be.an.instanceOf(Error);
                                v.should.be.eql({kid: "kid_reset"});
                                done();
                            });

                        });
                    }, 1000);
                });

            });
        });

        describe('General functionality', function(){
            it('Should send larger (> 65k files)', function(done){
                var test
                  , fiveHundredAndTwelveK = 512 * 1024;


                test = function(str){
                    bgTaskWorker.on('TASK_AVAILABLE', function(id){
                        bgTaskWorker.acceptTask(id, function(msg){
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        });
                    });

                    bgTask.addTask({kid: "should handle 512k", body: str}, function(id, reply){
                        reply.body.should.eql(str);
                        done();
                    });
                };

                testUtils.testWithFile(fiveHundredAndTwelveK, test);
            });
        });
    });
});
