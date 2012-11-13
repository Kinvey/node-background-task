/*global describe, it, beforeEach, afterEach, after */

// Copyright 2012 Kinvey, Inc
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
  , background_task = require('../lib/background_task')
  , should = require('should')
  , redis = require('redis')
  , delay = 30 // This allows object creation to always finish
  , testUtils = require('./test-utils')
  , ll = function(m){
      var d = new Date()
      , t = d.toISOString();
      util.debug(t + ": " + m);
  };

describe('node-background-task', function(){
    var bgTask, bgTaskWorker
      , rc = redis.createClient();

    beforeEach(function(done){
        rc.flushall();

        if (bgTask){
            bgTask.end();
        }

        if (bgTaskWorker){
            bgTaskWorker.end();
        }

        setTimeout(function(){
            bgTask = background_task.connect({taskKey: "kid", maxTasksPerKey: 2});
            bgTaskWorker = background_task.connect({isWorker: true});
            done();
        }, delay);
    });

    after(function(done){
        if (bgTask){
            bgTask.end();
        }

        if (bgTaskWorker){
            bgTaskWorker.end();
        }
        done();
    });

    describe('Events', function(){
        describe('#emit()', function(){
            it('should invoke the callback', function(){
                var spy = sinon.spy();


                bgTask.on('foo', spy);
                bgTask.emit('foo');
                spy.called.should.equal.true;
            });

            it('should emit TASK_DONE when a task completes', function(done){
                var cb, tid;
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

                cb = function(){
                    bgTask.addTask({kid: "kidEmitTaskDone", body: "test"}, function(){});
                };
                setTimeout(cb, delay);

            });

            it('should emit TASK_AVAILABLE when a task is added', function(done){
                var cb;
                bgTaskWorker.on('TASK_AVAILABLE',  function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        done();
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "emitTaskAvailable", body: "test"}, function(){});
                };
                setTimeout(cb, delay);

            });

            it('should emit TASK_ERROR if something goes wrong', function(done){
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(reply){
                        bgTaskWorker.completeTask(id, reply);
                    });
                });

                bgTask.on('TASK_ERROR', function(err){
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
                    bgTaskWorker.acceptTask(id, function(msg){
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
                var cb;
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        should.exist(id);
                        msg.should.eql({kid: "should have task", body: "test"});
                        done();
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "should have task", body: "test"}, function(){});
                };
                setTimeout(cb, delay);

            });
        });
    });

    describe('#connect', function(){
        it('should return a valid BackgroundTask with no options', function(){
            var task = background_task.connect();
            task.should.be.a('object');
            task.end();
        });

        it('should return a valid BackgroundTask with all options', function(){
            var task = background_task.connect({
                task: "hey",
                taskKey: "kid",
                queue: "someNewQueue",
                outputHash: "someOutputHash",
                host: "0.0.0.0",
                port: "6379",
                isWorker: true
            });
            task.should.be.a('object');
            task.end();
        });


        it('should return a valid BackgroundTask with some options', function(){
            var task = background_task.connect({
                queue: "newQueue",
                host: "localhost",
                isWorker: true
            });
            task.should.be.a('object');
            task.end();
        });
        it('should be a worker when isWorker: true', function(done){
            var cb;
            bgTaskWorker.on('TASK_AVAILABLE', function(id){
                bgTaskWorker.acceptTask(id, function(msg){
                    bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    done();
                });
            });

            cb = function(){
                bgTask.addTask({kid: "should be worker", body: "test"}, function(){});
            };
            setTimeout(cb, delay);

        });

    });

    describe('BackgroundTask', function(){

        describe('#end', function(){
            it('should not allow more tasks to complete', function(done){
                var t = background_task.connect({taskKey: "hi"});
                t.end();
                t.addTask({hi: "test"}, function(id, v){
                    v.should.be.an.instanceOf(Error);
                    v.message.should.equal('Attempt to use shutdown MessageBus.');
                    done();
                });
            });
        });

        describe('#addTask', function(){
            it('should call callback', function(done){
                var cb;
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "should call callback", body: "test"}, function(id, reply){
                        done();
                    });

                };
                setTimeout(cb, delay);

            });

            it('should timeout if timeout value exceeded', function(done){
               var cb, task = background_task.connect({taskKey: "kid", timeout: 200})
                 , timer;

                cb = function(){
                    task.addTask({kid: "should timeout", body: "test"}, function(id, reply){
                        reply.should.be.an.instanceOf(Error);
                        reply.message.should.equal('Task timed out');
                        task.end();
                        done();
                    });

                };
                setTimeout(cb, delay);
            });

            it('should not call callback twice if timeout value exceeded (Will fail with double done() if code is broken)', function(done){
                var cb, task = background_task.connect({taskKey: "kid", timeout: delay});
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(r){
                        if (util.isError(r)){
                            ll("ERROR!!!");
                        } else {
                            setTimeout(function(){
                                bgTaskWorker.completeTask(id, 'SUCCESS', r);
                                task.end();
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
                        r.should.eql({kid: "keyT", task: 1});
                    });
                };

                t2 = function(){
                    bgTask.addTask({kid: "keyT", task: 2}, function(id, r){
                        r.should.eql({kid: "keyT", task: 2});
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
                var count = 2, f;

                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(d){
                        bgTaskWorker.completeTask(id, 'SUCCESS', d);
                    });
                });

                f = function(){
                    bgTask.addTask({kid: "multi"}, function(id, v){
                        v.should.be.eql({kid: "multi"});
                        count = count - 1;
                        if (count === 0){
                            done();
                        }
                    });
                };

                process.nextTick(function(){process.nextTick(f);});
                process.nextTick(f);
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
                var cb
                  , btw = background_task.connect({isWorker: true})
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


                cb = function(){
                    bgTask.addTask({kid: "only called once", body: "test"}, function(id, reply){
                        count.should.equal(1);
                        btw.end();
                        done();
                    });

                };
                process.nextTick(cb);

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
                }).should.throw('Missing msgId, status or msg.');
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
                        bgTaskWorker.msgBus.idToChannelMap[id] = "retChn";
                        bgTaskWorker.completeTask(id, allowed[i], msg);
                    }).should.not.throw();
                }

                for (i = 0; i < notAllowed.length; i = i + 1){
                    (function(){
                        bgTaskWorker.completeTask(id, notAllowed[i], msg);
                    }).should.throw(/is not a valid status\./);
                }
            });
        });

      describe('General functionality', function(){
        it('Should send larger (> 65k files)', function(done){
            var test
              , fiveHundredAndTwelveK = 512 * 1024;


            test = function(str){
                var cb;
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "should handle 512k", body: str}, function(id, reply){
                        reply.body.should.eql(str);
                        done();
                    });
                };

                setTimeout(cb, delay);
            };

            testUtils.testWithFile(fiveHundredAndTwelveK, test);
        });
      });
    });
});
