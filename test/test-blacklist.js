/*global describe, it, beforeEach, afterEach, before */

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

var sinon = require('sinon')
  , bl = require('../lib/blacklist')
  , should = require('should')
  , redis = require('redis')
  , async = require('async')
  , delay = 500; // This allows object creation to always finish


describe('Blacklist', function(){
    var blacklist, rc, task;

    before(function(done){
        blacklist = new bl.Blacklist({taskKey: "a"});
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
            x(args);
            messages.push(args);
            x.call(this, args);
          };
          var tmpBL = new bl.Blacklist({taskKey: "auth",  password: "invalid"}, function() {
            for (var i = 0; i < messages.length; i++) {
              if (messages[i] === "Warning: Redis server does not require a password, but a password was supplied.") {
                warnMsg = messages[i];
              }
            }
            warnMsg.should.eql("Warning: Redis server does not require a password, but a password was supplied.");
            console.log = x;
            done();
          });

        });
    });

    describe('#blacklistStatus', function(){
        it('should report true if the taskKey is blacklisted', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "isBL";
            var reason = "Woot";
            var blacklistKey = b.globalBlacklistKeyPrefix + taskKey;
            rc.setex(blacklistKey, b.globalBlacklistTimeout, reason, function(e, r){
                b.blacklistStatus({kid: taskKey}, function(isBlacklisted, ttl, reason){
                    if (isBlacklisted){
                        done();
                    }
                });
            });
        });

        it('should report a reason for why the taskKey is blacklisted', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "reason";
            var reason = "Woot";
            var blacklistKey = b.globalBlacklistKeyPrefix + taskKey;
            rc.setex(blacklistKey, b.globalBlacklistTimeout, reason, function(e, r){
                b.blacklistStatus({kid: taskKey}, function(isBlacklisted, ttl, rea){
                    if (isBlacklisted){
                        rea.should.be.eql(reason);
                        done();
                    }
                });
            });
        });

        it('should report the remaining blacklist time', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "timeout";
            var reason = "Woot";
            var blacklistKey = b.globalBlacklistKeyPrefix + taskKey;
            rc.setex(blacklistKey, b.globalBlacklistTimeout, reason, function(e, r){
                b.blacklistStatus({kid: taskKey}, function(isBlacklisted, ttl, rea){
                    if (isBlacklisted){
                        rea.should.be.eql(reason);
                        ttl.should.be.within(0, b.globalBlacklistTimeout);
                        done();
                    }
                });
            });
        });

        it('should report false if the taskKey is not blacklisted', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "false";
            var reason = "Woot";
            var blacklistKey = b.globalBlacklistKeyPrefix + taskKey;
            b.blacklistStatus({kid: taskKey}, function(isBlacklisted, ttl, rea){
                isBlacklisted.should.be.false;
                done();
            });
        });
    });

    describe('#addFailure', function(){
        it('should reject a request with an empty reason', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "emptyReason";
            b.addFailure(taskKey, "", function(status){
                status.should.be.an.instanceOf(Error);
                status.message.should.be.eql("Must supply a reason for the failure");
                done();
            });
        });

        it('should create a new key with a ttl > 0 if no existing failure', function(done){
            var b = new bl.Blacklist({taskKey: "kid", failureInterval:10});
            var taskKey = "newFailure";
            var reason = "new failure";
            var blacklistKey = b.keyPrefix + taskKey + ":count";
            b.addFailure(taskKey, reason, function(status){
                status.should.be.eql("OK");
                rc.get(blacklistKey, function(e1, r1){
                    should.exist(r1);
                    r1.should.be.above(0);
                    rc.ttl(blacklistKey, function(e2, r2){
                        should.exist(r2);
                        r2.should.be.above(0);
                        done();
                    });
                });
            });
        });

        it('should increment the count on a reported failure', function(done){
            var b = new bl.Blacklist({taskKey: "kid", failureInterval:10});
            var taskKey = "incr";
            var reason = "incr";
            var blacklistKey = b.keyPrefix + taskKey + ":count";
            b.addFailure(taskKey, reason, function(status){
                status.should.be.eql("OK");
                b.addFailure(taskKey, reason, function(status2){
                    status2.should.be.eql("OK");
                    rc.get(blacklistKey, function(e1, r1){
                        should.exist(r1);
                        r1.should.be.above(1);
                        rc.ttl(blacklistKey, function(e2, r2){
                            should.exist(r2);
                            r2.should.be.above(0);
                            done();
                        });
                    });
                });
            });
        });

        it('should blacklist something that fails too ofter', function(done){
            var b = new bl.Blacklist({taskKey: "kid", failureInterval: 10, blacklistThreshold: 2});
            var taskKey = "blacklist";
            var reason = "Testing BL";

            b.addFailure(taskKey, reason, function(s1){
                s1.should.eql("OK");
                b.addFailure(taskKey, reason, function(s2){
                    s2.should.eql("OK");
                    b.addFailure(taskKey, reason, function(s3){
                        s3.should.eql("Blacklisted");
                        b.blacklistStatus({kid: taskKey}, function(isB, ttl, reason){
                            isB.should.be.true;
                            ttl.should.be.within(0, b.globalBlacklistTimeout);
                            reason.should.eql("Testing BL");
                            done();
                        });
                    });
                });
            });

        });

        it ('should let a blacklist be lifted', function(done){
            var b = new bl.Blacklist({
                taskKey: "kid",
                failureInterval: 10,
                blacklistThreshold: 1,
                globalBlacklistTimeout: 1
            });

            var taskKey = "release";
            var reason = "Testing lifting";

            b.addFailure(taskKey, reason, function(s1){
                s1.should.eql("OK");
                b.addFailure(taskKey, reason, function(s2){
                    s2.should.eql("Blacklisted");
                    b.blacklistStatus({kid: taskKey}, function(isB, ttl, reason){
                        isB.should.be.true;
                        reason.should.eql("Testing lifting");
                        setTimeout(function(){
                            b.blacklistStatus({kid: taskKey}, function(b, t, r){
                                b.should.be.false;
                                done();
                            });
                        }, b.globalBlacklistTimeout*1500);
                    });
                });
            });
        });

        it('should properly reset the blacklist counter', function(done){
            var b = new bl.Blacklist({taskKey: "kid"});
            var taskKey = "reset";
            var reason = "reset case";
            var blacklistKey = b.keyPrefix + taskKey + ":count";

            b.addFailure(taskKey, reason, function(status){
                status.should.be.eql("OK");
                setTimeout(function(){
                    rc.ttl(blacklistKey, function(e1, r1){
                        should.exist(r1);
                        r1.should.be.below(0);
                        rc.get(blacklistKey, function(e2, r2){
                            should.not.exist(r2);
                            done();
                        });
                    });
                }, b.failureInterval * 1500);
            });
        });
    });
    describe("Logging", function(){
        it('should not log when logging is false', function(done){
            var b = new bl.Blacklist({
                taskKey: "kid",
                failureInterval: 10,
                blacklistThreshold: 1
            });

            var taskKey = "log_false";
            var reason = "Testing lifting";

            b.addFailure(taskKey, reason, function(s1){
                s1.should.eql("OK");
                b.addFailure(taskKey, reason, function(s2){
                    s2.should.eql("Blacklisted");
                    rc.llen(b.blacklistLogKeyPrefix + taskKey, function(e, r){
                        r.should.be.eql(0);
                        done();
                    });
                });
            });

        });
        it('should log when logging is true', function(done){
            var b = new bl.Blacklist({
                taskKey: "kid",
                failureInterval: 10,
                blacklistThreshold: 1,
                logBlacklist: true
            });

            var taskKey = "log_true";
            var reason = "Testing lifting";

            b.addFailure(taskKey, reason, function(s1){
                s1.should.eql("OK");
                b.addFailure(taskKey, reason, function(s2){
                    s2.should.eql("Blacklisted");
                    rc.llen(b.blacklistLogKeyPrefix + taskKey, function(e, r){
                        r.should.eql(1);
                        done();
                    });
                });
            });
        });
        it('should log multiple blacklists', function(done){
            var b = new bl.Blacklist({
                taskKey: "kid",
                failureInterval: 10,
                blacklistThreshold: 1,
                logBlacklist: true
            });

            var taskKey = "log_multiple";
            var reason = "Testing lifting";

            b.addFailure(taskKey, reason, function(s1){
                s1.should.eql("OK");
                b.addFailure(taskKey, reason, function(s2){
                    s2.should.eql("Blacklisted");
                    rc.del(b.globalBlacklistKeyPrefix + taskKey, function(e, r){
                        b.addFailure(taskKey, reason, function(s3){
                            s3.should.eql("Blacklisted");
                            rc.llen(b.blacklistLogKeyPrefix + taskKey, function(e, r){
                                r.should.eql(2);
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('should log different task keys in different logs', function(done){
            var b = new bl.Blacklist({
                taskKey: "kid",
                failureInterval: 10,
                blacklistThreshold: 1,
                logBlacklist: true
            });

            var taskKey1 = "log_alpha";
            var taskKey2 = "log_beta";
            var reason = "Testing lifting";

            b.addFailure(taskKey1, reason, function(a1){
                a1.should.eql("OK");
                b.addFailure(taskKey1, reason, function(a2){
                    a2.should.eql("Blacklisted");
                    b.addFailure(taskKey2, reason, function(b1){
                        b1.should.eql("OK");
                        b.addFailure(taskKey2, reason, function(b2){
                            b2.should.eql("Blacklisted");
                            rc.llen(b.blacklistLogKeyPrefix + taskKey1, function(e, r){
                                r.should.eql(1);
                                rc.llen(b.blacklistLogKeyPrefix + taskKey2, function(e, r){
                                    r.should.eql(1);
                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    describe('getBlacklistCount', function() {
      it('should return the count of blacklisted taskKeys', function(done) {
        var b = new bl.Blacklist({
          taskKey: "testKey",
          failureInterval: 10,
          blacklistThreshold: 1,
          logBlacklist: true
        });

        var taskKey1 = "taskKey1";
        var taskKey2 = "taskKey2";
        var taskKey3 = "taskKey3";
        var reason = "Test Blacklist";

        b.addFailure(taskKey1, reason, function(err, result) {
          b.addFailure(taskKey1, reason, function(err, result) {
            b.addFailure(taskKey2, reason, function(err, result) {
              b.addFailure(taskKey2, reason, function(err, result) {
                b.addFailure(taskKey3, reason, function(err, result) {
                  b.addFailure(taskKey3, reason, function(err, result) {
                    b.getBlacklistCount(function(err, result) {
                      result.should.eql(3);
                      done();
                    });
                  });
                });
              });
            });
          });
        });

        b.getBlacklistCount(function(err, result) {
          result.should.eql(0);
        });
      });

      it('should return if no tasks are blacklisted', function(done) {
        var b = new bl.Blacklist({
          taskKey: "emptyKid",
          failureInterval: 10,
          blacklistThreshold: 1,
          logBlacklist: true
        });

        b.getBlacklistCount(function(err, result) {
          result.should.eql(0);
          done();
        });
      });

    });
});
