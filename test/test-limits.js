/*global describe, it, beforeEach, afterEach, before */

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
  , limits = require('../lib/task_limit')
  , should = require('should')
  , redis = require('redis')
  , async = require('async')
  , delay = 100; // This allows object creation to always finish


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

    describe('redis', function(){
        it('should verify that authentication works', function(done){
          // have to augment console.log as version 0.8.5 of redis no longer throws an error when a password
          // is supplied but none is required,
          var x = console.log;
          var messages = [];
          var warnMsg = "";
          console.log = function(args) {
            messages.push(args);
            x.call(this, args);
          };
            var limit = new limits.TaskLimit({taskKey: "auth",  password: "invalid", maxTasksPerKey: 5}, function() {
              for (var i = 0; i < messages.length; i++) {
                if (messages[i] = "Warning: Redis server does not require a password, but a password was supplied.") {
                  warnMsg = messages[i];
                }
              }
              warnMsg.should.eql("Warning: Redis server does not require a password, but a password was supplied.");
              console.log = x;
              done();
            });
        });
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
            var key = taskLimit.keyPrefix+task.a;
            taskLimit.startTask(task, function(v){
                setTimeout(function(){
                    rc.llen(taskLimit.keyPrefix+task.a, function(err, r){
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
            }, (max+20)*10);
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

    describe('#cleanupTasks', function(){
      it('should not throw error if no keys', function(done) {


          taskLimit.cleanupTasks(function() {

              done();
            });

      });
      it('should clean up key all tasks created by the current host', function(done){
        var key = taskLimit.redisKeyPrefix+task.a;
        async.times(5, function(n, next){
          taskLimit.startTask(task, function(){
            next();
          })
        }, function() {
          setTimeout(function(){
            taskLimit.cleanupTasks(function() {
              rc.llen(key, function(err, len){
                len.should.equal(0);
                done();
              });
            });
          }, delay);
        });
      });
      it('should clean up all tasks created by the current host only', function(done){
        var key = taskLimit.redisKeyPrefix+task.a;
        async.times(3, function(n, next){
          taskLimit.startTask(task, function() {
           next();
          })
        }, function() {
          var value = JSON.stringify({host: "abcd", date: Date()});
            rc.lpush(key, value, value, function(y) {
              taskLimit.cleanupTasks(function(x) {
                rc.llen(key, function(err, len){
                  len.should.equal(2);
                  done();
                });
              });
            });
          });
      });
      it('should not clean up any tasks', function(done){
        var key = taskLimit.redisKeyPrefix+task.a;
        var value = JSON.stringify({host: "abcd", date: Date()});
        rc.lpush(key, value, value, value, value, value);

        setTimeout(function(){
          taskLimit.cleanupTasks(function() {
            rc.llen(key, function(err, len){
              len.should.equal(5);
              done();
            });
          });
        }, delay);
      });
    });
  });

