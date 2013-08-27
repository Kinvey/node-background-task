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
  , messaging = require('../lib/messaging')
  , should = require('should')
  , redis = require('redis')
  , util = require('util')
  , delay = 30 // This allows object creation to always finish
  , testUtils = require('./test-utils')
  , ll = function(m){
      var d = new Date()
      , t = d.toISOString();
      util.debug(t + ": " + m);
  };

describe('messaging', function(){
    describe('#connect', function(){
        it('should return a valid BackgroundTask with no options', function(){
          var msgBus = messaging.connect();
          msgBus.should.be.an.instanceOf(messaging.MessageBus);
        });

        it('should return a valid BackgroundTask with all options', function(){
          var msgBus = messaging.connect({
            queue: "someNewQueue",
            outputHash: "someOutputHash",
            host: "0.0.0.0",
            port: "6379",
            isResponder: true
          });
          msgBus.should.be.an.instanceOf(messaging.MessageBus);
          msgBus.shutdown();
        });


        it('should return a valid BackgroundTask with some options', function(){
            var msgBus = messaging.connect({
                queue: "newQueue",
                host: "localhost",
                isResponder: true
            });
            msgBus.should.be.an.instanceOf(messaging.MessageBus);
            msgBus.shutdown();
        });
        it('should be a worker when isWorker: true', function(done){
            var hashName = "testHash"
              , queueName = "testQueue"
              , message = '{"test":"message", "_messageId": "0xdeadbeef", "_listenChannel":"dummy"}'
              , opts = {outputHash: hashName, dataHash: queueName, isResponder: true}
              , rcPubSub = redis.createClient()
              , rcData = redis.createClient()
              , mBus = messaging.connect(opts)
              , cback = function(){
                  rcData.hset(queueName, "0xdeadbeef", message);
                  rcPubSub.publish("msgChannels:broadcast", "0xdeadbeef");
              };

            mBus.once('data_available', function(id){
                mBus.acceptMessage(id, function(msg){
                    mBus.shutdown();
                    msg.test.should.eql(JSON.parse(message).test);
                    done();
                });
            });

            setTimeout(cback, delay);

        });
        it('should verify that authentication works', function(done){
            var mBus = messaging.connect({password: 'hiFriends'});
            mBus.on('error', function(){mBus.shutdown();done();});

        });
    });

    describe('MessageBus', function(){
        var mBus
          , mBusWorker
          , rc = redis.createClient();

        beforeEach(function(done){
            rc.flushall();

            if (mBus){
                mBus.shutdown();
            }

            if (mBusWorker){
                mBusWorker.shutdown();
            }

            mBus = messaging.connect();
            mBusWorker = messaging.connect({isResponder: true});
            done();
        });

        describe('Error Handling', function(){
            it('should handle bad items on the worker queue', function(done){
                var mBus = messaging.connect({isResponder: true, dataHash: "biwq", broadcastChannel: "biwqC"})
                  , cback = function(){
                      rc.hset(mBus.dataHash, "0xdeadbeef", "this is not json");
                      rc.publish(mBus.broadcastChannel, "0xdeadbeef");
                  };

                mBus.once('data_available', function(id){
                    mBus.acceptMessage(id, function(rep){
                        rep.should.be.an.instanceOf(Error);
                        rep.message.should.match(/Bad data in sent message/);
                        done();
                    });
                });

                setTimeout(cback, delay + 10);

            });

            it('should handle no item on the worker queue', function(done){
                var mBus = messaging.connect({isResponder: true, dataQueue: "niwq", broadcastChannel: "niwqC"})
                  , cback = function(){
                      rc.publish(mBus.broadcastChannel, "0xdeadbeef");
                  };

                mBus.once('data_available', function(id){
                    mBus.acceptMessage(id, function(rep){
                        rep.should.be.an.instanceOf(Error);
                        rep.message.should.equal("DB doesn't recognize message");
                        done();
                    });
                });

                setTimeout(cback, delay + 50);

            });

            it('should handle a mal-formed message', function(done){
                var mBus = messaging.connect()
                  , cback = function(){
                    rc.publish(mBus.listenChannel, "NOSPACES");
                  };

                mBus.on('error', function(err){
                    err.should.be.an.instanceOf(Error);
                    err.message.should.equal('Invalid message received!');
                    done();
                });

                setTimeout(cback, delay);
            });

            it('should handle JSON that is corrupt', function(done){
                var mBus = messaging.connect()
                  , cback = function(){
                      rc.hset(mBus.responseHash, "0xdeadbeef", "this is not json");
                      rc.publish(mBus.listenChannel, "0xdeadbeef FAILED");
                  };

                mBus.on('error', function(err){
                    err.should.be.an.instanceOf(Error);
                    err.message.should.match(/^JSON parsing failed!/);
                    done();
                });

                setTimeout(cback, delay);

            });
            it('should handle when an empty item is pulled from the queue', function(done){
                var mBus = messaging.connect()
                  , cback = function(){
                      rc.publish(mBus.listenChannel, "0xdeadbeef FAILED");
                  };

                mBus.on('error', function(err){
                    err.should.be.an.instanceOf(Error);
                    err.message.should.match(/^No message for id/);
                    done();
                });

                setTimeout(cback, delay);

            });
        });

        describe('#shutdown', function(){
            it('should not allow more tasks to complete', function(done){
                var cback1, cback2;

                mBusWorker.once('data_available', function(id){
                    mBusWorker.acceptMessage(id, function(msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });

                cback1 = function(){
                    var cid = messaging.makeId();
                    mBus.sendMessage(cid, {body: 'test'}, function(reply){
                        reply.should.eql({body: 'test'});
                        mBus.shutdown();
                        cback2();
                    });
                };

                cback2 = function(){
                    var cid = messaging.makeId();
                    mBus.sendMessage(cid, {body: 'test'}, function(reply){
                        reply.should.be.an.instanceOf(Error);
                        reply.message.should.equal("Attempt to use shutdown MessageBus.");
                        done();
                    });
                };

                // Need to delay just a bit to let everything start-up
                setTimeout(cback1, delay);


            });
        });

        describe('#sendMessage', function(){
            it('should call callback', function(done){
                var cback;
                mBusWorker.once('data_available', function(id){
                    mBusWorker.acceptMessage(id, function(msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });

                cback = function(){
                    var cid = messaging.makeId();
                    mBus.sendMessage(cid, {'body': 'test'}, function(reply){
                        reply.should.eql({'body': 'test'});
                        done();
                    });
                };

                // Need to delay just a bit to let everything start-up
                process.nextTick(cback);
            });

            it('should allow for multiple tasks to be added', function(done){
                done();
            });

            it('should reject excessive payloads', function(done){
                var test
                  , twoMegs = 2 * 1024 * 1024;

                test = function(str){
                    var msg = {body: str}
                      , cback;

                    mBusWorker.once('data_available', function(){
                        mBusWorker.acceptMessage(function(id, msg){
                            mBusWorker.sendResponse(id, 'SUCCESS', msg);
                        });
                    });

                    cback = function(){
                        mBus.sendMessage(messaging.makeId(), msg, function(reply){
                            reply.should.be.instanceOf(Error);
                            reply.message.should.equal('Payload too large!');
                            done();
                        });
                    };

                    // Need to delay just a bit to let everything start-up
                    setTimeout(cback, delay);
                };

                testUtils.testWithFile(twoMegs, test);
            });

            it('should reject message that are not JSON', function(done){
                var cback;
                mBusWorker.once('data_available', function(){
                    mBusWorker.acceptMessage(function(id, msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });

                cback = function(){
                    mBus.sendMessage(messaging.makeId(), cback, function(reply){
                        reply.should.be.instanceOf(Error);
                        reply.message.should.equal('Error converting message to JSON.');
                        done();
                    });
                };

                // Need to delay just a bit to let everything start-up
                setTimeout(cback, delay);
            });
        });

        describe('#acceptMessage', function(){
            it('should reject no parameters', function(){
                (function() {
                    mBusWorker.acceptMessage(null, null);
                }).should.throw('Missing Message ID.');
            });

            it('should reject missing ids', function(){
                (function() {
                    mBusWorker.acceptMessage(null, function(id, msg){});
                }).should.throw('Missing Message ID.');
            });

            it('should reject invalid callbacks', function(){
                (function() {
                    mBusWorker.acceptMessage("0xdeadbeef");
                }).should.throw('Invalid callback.');

                (function() {
                    mBusWorker.acceptMessage("0xdeadbeef", function(){});
                }).should.throw('Missing parameters in callback.');

            });
            it('should reject messages not in redis', function(done){
                mBusWorker.acceptMessage("NOT FOUND", function(reply){
                    reply.should.be.an.instanceOf(Error);
                    reply.message.should.equal("DB doesn't recognize message");
                    done();
                });

            });
        });

        describe('#sendResponse', function(){
            it('should reject tasks without ids', function(done){
                try {
                    mBusWorker.sendResponse(null, 'SUCCESS', {body: "hi"});
                } catch(e) {
                    e.should.be.instanceOf(Error);
                    e.message.should.equal('Missing msgId, status or msg.');
                    done();
                }
            });

            it('should reject responding to tasks that were not accepted', function(){
                (function(){
                    mBusWorker.sendResponse(messaging.makeId(), 'FAILED', {body: "Yo!"});
                }).should.throw('Attempt to respond to message that was never accepted');
            });

            it('should reject tasks without a status', function(done){
                try {
                    mBusWorker.sendResponse(messaging.makeId(), undefined, {body: "hi"});
                } catch(e) {
                    e.should.be.instanceOf(Error);
                    e.message.should.equal('Missing msgId, status or msg.');
                    done();
                }
            });

            it('should reject empty messages', function(){
                try {
                    mBusWorker.sendResponse(messaging.makeId(), 'SUCCESS', null);
                } catch(e) {
                    e.should.be.instanceOf(Error);
                    e.message.should.equal('Missing msgId, status or msg.');
                }
            });

            it('should accept only SUCCESS, ERROR, FAILED, or PROGRESS for status', function(){
                var id = messaging.makeId()
                  , msg = {body: 'hi mom'}
                  , allowed = ['SUCCESS', 'ERROR', 'FAILED', 'PROGRESS']
                  , notAllowed = ['GOOD', 1, 21.2, {test: "object"}, 'S', true]
                  , i;

                for (i = 0; i < allowed.length; i = i + 1){
                    (function(){
                        mBusWorker.sendResponse(msg, allowed[i], msg, true);
                    }).should.not.throw();
                }

                for (i = 0; i < notAllowed.length; i = i + 1){
                    (function(){
                        mBusWorker.sendResponse(msg, notAllowed[i], msg);
                    }).should.throw(/is not a valid status\./);
                }

            });

            it('should publish only once on concurrent messaging.', function(done) {
                var stub     = sinon.stub(mBusWorker.pubClient, 'publish')
                  , body     = { body: 'hi mom' }
                  , msgId    = messaging.makeId()
                  , testMode = true;

                mBusWorker.sendResponse(msgId, 'PROGRESS', body, testMode);
                mBusWorker.sendResponse(msgId, 'PROGRESS', body, testMode);
                mBusWorker.sendResponse(msgId, 'SUCCESS',  body, testMode);

                setTimeout(function() {
                    stub.callCount.should.equal(1);
                    stub.restore();
                    done();
                }, 1000);
            });


            it('should publish only once on concurrent messaging, with errors.', function(done) {
                var pubStub  = sinon.stub(mBusWorker.pubClient, 'publish')
                  , msgId    = messaging.makeId()
                  , testMode = true;

                // Force `hset` to throw an error to verify that publish is
                // called even when one of the concurrent request fails.
                var errors   = 0;
                var dataStub = sinon.stub(mBusWorker.dataClient, 'hset', function(hash, msgId, msg, fn) {
                    try {
                        if(JSON.parse(msg).error) {
                            errors += 1;
                            fn(new Error('Test error'));
                        }
                        else {
                            fn(null, true);
                        }
                    }
                    catch(e) {
                        // Ignore exceptions.
                    }
                });

                mBusWorker.sendResponse(msgId, 'PROGRESS', { error: true },  testMode);
                mBusWorker.sendResponse(msgId, 'PROGRESS', { error: true },  testMode);
                mBusWorker.sendResponse(msgId, 'SUCCESS',  { error: false }, testMode);

                setTimeout(function() {
                    errors.should.equal(2);// Exceptions should have been thrown.
                    pubStub.callCount.should.equal(1);
                    pubStub.restore();
                    dataStub.restore();
                    done();
                }, 1000);
            });

        });

        describe('#makeId', function(){
            it('should make unique ids', function(){
                var a = messaging.makeId()
                  , b = messaging.makeId()
                  , c = messaging.makeId()
                  , i, id, last;

                a.should.not.equal(b);
                a.should.not.equal(c);
                b.should.not.equal(c);

                last = c;
                for (i = 0; i < 10000; i = i + 1){
                    id = messaging.makeId();
                    id.should.not.equal(last);
                    last = id;
                }
            });
        });
    });
});
