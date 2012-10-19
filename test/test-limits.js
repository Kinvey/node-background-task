/*global describe, it, beforeEach, afterEach, before */
"use strict";

var sinon = require('sinon')
  , limits = require('../lib/task_limit')
  , should = require('should')
  , redis = require('redis')
  , delay = 30; // This allows object creation to always finish


describe('Test Limits', function(){
    var taskLimit, rc, task;

    before(function(done){
        taskLimit = new limits.TaskLimit({taskKey: "a",  maxTasksPerKey: 5});
        rc = redis.createClient();
        rc.flushall();
        task = {a: "kid1234", msg: "Hi Mom!"};
        done();
    });

    beforeEach(function(done){
        rc.flushall();
        done();
    });

    afterEach(function(done){
        rc.flushall();
        done();
    });

    describe('Error Handling', function(){
        it('should not create an object without a task key', function(){
            (function(){
                var hi = new limits.TaskLimit();
            }).should.throw('I need a task key!');
        });

        it('should not start invalid tasks', function(){
            var tl = new limits.TaskLimit({taskKey: 'a'})
              , cb = function(res){
                  res.should.be.an.instanceOf(Error);
                  res.message.should.equal('Invalid task, not running.');
              };

            tl.startTask(null, cb);
            tl.startTask(undefined, cb);
            tl.startTask({}, cb);
            tl.startTask({b: "x"}, cb);

        });

        it('should not stop invalid tasks', function(){
            var tl = new limits.TaskLimit({taskKey: 'a'});
            (function(){tl.stopTask(null); }).should.throw("Invalid task, can't stop.");
            (function(){tl.stopTask(undefined); }).should.throw("Invalid task, can't stop.");
            (function(){tl.stopTask({}); }).should.throw("Invalid task, can't stop.");
            (function(){tl.stopTask({b: "x"}); }).should.throw("Invalid task, can't stop.");

        });

    });

    describe('#startTask', function(){
        it('should increment the task counter', function(done){
            var key = taskLimit.redisKeyPrefix+task.a;
            taskLimit.startTask(task, function(v){
                setTimeout(function(){
                    rc.llen(taskLimit.redisKeyPrefix+task.a, function(err, r){
                        r.should.eql(v);
                        done();
                    });
                }, delay);
            });
        });

        it('should max out at the max number of connections', function(done){
            var max = taskLimit.maxTasksPerKey, i, fn;

            fn = function(){
                taskLimit.startTask(task, function(v){
                    v.should.not.be.an.instanceOf(Error);
                });
            };

            for (i = 0; i < max; i = i + 1){
                setTimeout(fn, i*10);
            }

            setTimeout(function(){
                taskLimit.startTask(task, function(v){
                    v.should.be.an.instanceOf(Error);
                    v.message.should.equal('Too many tasks');
                    done();
                });
            }, (max+1)*10);
        });

        it('should handle 0 wrap-around', function(done){
            taskLimit.stopTask(task);
            taskLimit.stopTask(task);
            taskLimit.stopTask(task);

            setTimeout(function(){
                rc.llen(taskLimit.redisKeyPrefix+task.a, function(err, len){
                    len.should.equal(0);
                    done();
                });
            }, delay);

        });
    });

    describe('#stopTask', function(){
        it('should decrement the task counter', function(done){
            var key = taskLimit.redisKeyPrefix+task.a;
            taskLimit.startTask(task, function(v){
                taskLimit.stopTask(task);
                setTimeout(function(){
                    rc.llen(key, function(err, len){
                        len.should.equal(0);
                        done();
                    });
                }, delay);
            });
        });
        it('should decrement the task counter', function(done){
            var key = taskLimit.redisKeyPrefix+task.a;
            taskLimit.startTask(task, function(v){
                taskLimit.stopTask(task);
            });
            taskLimit.startTask(task, function(v){
                taskLimit.stopTask(task);
            });
            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task, function(v){
                taskLimit.stopTask(task);
            });

            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task);
            taskLimit.startTask(task, function(v){
                taskLimit.stopTask(task);
            });

            setTimeout(function(){
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
                taskLimit.stopTask(task);
            }, delay / 2);

            setTimeout(function(){
                rc.llen(key, function(err, len){
                    len.should.equal(0);
                    done();
                });
            }, delay);

        });

    });
});
