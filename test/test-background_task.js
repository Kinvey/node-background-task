/*global describe, it, beforeEach, afterEach */
"use strict";

var sinon = require('sinon')
  , background_task = require('../lib/background_task')
  , should = require('should')
  , redis = require('redis')
  , delay = 30; // This allows object creation to always finish


describe('node-background-task', function(){
    var bgTask, bgTaskWorker, rc;
    beforeEach(function(done){
        rc = redis.createClient();
        rc.flushall();
        bgTask = background_task.connect({taskKey: "kid"});
        bgTaskWorker = background_task.connect({isWorker: true});
        done();
    });

    afterEach(function(done){
        rc.flushall();
        bgTask.end();
        bgTaskWorker.end();
        done();
    });

    describe('Events', function(){
        describe('#emit()', function(){
            it('should invoke the callback', function(){
                var spy = sinon.spy()
                , bgTask = background_task.connect({});

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
                    reply.should.eql({kid: "kid1234", body: "test"});
                    done();
                });

                cb = function(){
                    bgTask.addTask({kid: "kid1234", body: "test"}, function(){});
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
                    bgTask.addTask({kid: "kid1234", body: "test"}, function(){});
                };
                setTimeout(cb, delay);

            });

            it('should emit TASK_ERROR if something goes wrong');
            // it('should emit TASK_ERROR if something goes wrong', function(done){
            //     var i;
            //     bgTaskWorker.on('TASK_AVAILABLE', function(id){
            //         bgTaskWorker.acceptTask(id, function(msg){
            //             bgTaskWorker.completeTask(id, 'SUCCESS', msg);
            //         });
            //     });

            //     bgTaskWorker.on('TASK_ERROR', done);

            //     for (i = 0; i < 10; i = i + 1){
            //             bgTask.addTask({kid: "kid123", body: "hi"}, function(id, reply){console.log(reply);});
            //     }

            // });
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
                bgTask.addTask({kid: "kid1234", body: "test"}, function(id, reply){
                    reply.should.be.an.instanceOf(Error);
                    reply.message.should.equal(mm);
                    reply.debugMessage.should.equal(dm);
                    done();
                });
            });

            it('should emit both TASK_ERROR and TASK_DONE if there was an error');
            it('should have the task when TASK_AVAILABLE is emitted', function(done){
                var cb;
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        should.exist(id);
                        msg.should.eql({kid: "kid1234", body: "test"});
                        done();
                    });
                });

                cb = function(){
                    bgTask.addTask({kid: "kid1234", body: "test"}, function(){});
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
                bgTask.addTask({kid: "kid1234", body: "test"}, function(){});
            };
            setTimeout(cb, delay);

        });

    });

    describe('BackgroundTask', function(){

        describe('#end', function(){
            it('should not allow more tasks to complete');
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
                    bgTask.addTask({kid: "kid1234", body: "test"}, function(id, reply){
                        done();
                    });
                    
                };
                setTimeout(cb, delay);

            });

            it('should timeout if timeout value exceeded', function(done){
               var cb, task = background_task.connect({taskKey: "kid", timeout: 200});
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        setTimeout(function(){
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay*10);
                    });
                });

                cb = function(){
                    task.addTask({kid: "kid1234", body: "test"}, function(id, reply){
                        reply.should.be.an.instanceOf(Error);
                        reply.message.should.equal('Task timed out');
                        task.end();
                        done();
                    });
                    
                };
                setTimeout(cb, delay);
            });

            it('should not call callback twice if timeout value exceeded', function(done){
                // TODO: Make this better
                var cb, task = background_task.connect({taskKey: "kid", timeout: delay});
                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        setTimeout(function(){
                            bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                        }, delay+1);
                    });
                });

                cb = function(){
                    task.addTask({kid: "kid1234", body: "test"}, function(id, reply){
                        reply.should.be.an.instanceOf(Error);
                        reply.message.should.equal('Task timed out');
                        
                        task.end();
                        done();
                    });
                    
                };
                setTimeout(cb, delay);
            });
            
            it('should reject tasks over key threshold');
            it('should allow for multiple tasks to be added', function(done){
              var totalMsgs = 10, i
                , count = 0
                , makeCallback = function(total){
                    return function(){
                        var id = Date()
                          , body = {kid: "kid1234", body: 'test_'+id};
                        bgTask.addTask(body, function(tid, reply){
                            reply.should.eql({kid: "kid1234", body: 'test_'+id});
                            count = count + 1;
                            if (count >= total){
                                done();
                            }
                        });
                    };
                };


                bgTaskWorker.on('TASK_AVAILABLE', function(id){
                    bgTaskWorker.acceptTask(id, function(msg){
                        bgTaskWorker.completeTask(id, 'SUCCESS', msg);
                    });
                });


                for (i = 0; i < totalMsgs; i = i + 1){
                    setTimeout(makeCallback(totalMsgs), delay);
                }

            });
        });
        describe('#completeTask', function(){
            var msg = {kid: "kid1234", body: "test"};
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
    });
});
