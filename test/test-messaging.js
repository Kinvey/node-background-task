/*global describe, it, beforeEach, afterEach */
"use strict";

var sinon = require('sinon')
  , messaging = require('../lib/messaging')
  , should = require('should')
  , redis = require('redis')
  , delay = 30; // This allows object creation to always finish


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
              , message = "{\"test\":\"message\"}"
              , opts = {outputHash: hashName, queue: queueName, isResponder: true}
              , rcPubSub = redis.createClient()
              , rcData = redis.createClient()
              , mBus = messaging.connect(opts)
              , cback = function(){
                  rcData.rpush(queueName, message);
                  rcPubSub.publish("msgChannels:broadcast", "0xdeadbeef");
              };

            mBus.on('data_available', function(){
                mBus.acceptMessage(function(id, msg){
                    mBus.shutdown();
                    JSON.stringify(msg).should.eql(message);
                    done();
                });
            });

            setTimeout(cback, delay);

        });
        it('should verify that authentication works');
    });

    describe('MessageBus', function(){
        var mBus
          , mBusWorker;

        beforeEach(function(done){
            var rc = redis.createClient();
            rc.flushall();
            mBus = messaging.connect();
            mBusWorker = messaging.connect({isResponder: true});
            done();
        });

        afterEach(function(done){
            mBus.shutdown();
            mBusWorker.shutdown();
            done();
        });

        describe('Error Handling', function(){
            var rc;
            beforeEach(function(done){
                rc = redis.createClient();
                rc.flushall();
                done();
            });

            afterEach(function(done){
                rc.end();
                done();
            });

            // TODO: FIXME
            // it('should handle bad items on the worker queue', function(done){
            //     var mBus = messaging.connect({isResponder: true, dataQueue: "biwq", broadcastChannel: "biwqC"})
            //       , cback = function(){
            //           rc.rpush(mBus.dataQueue, "this is not json");
            //           rc.publish(mBus.broadcastChannel, "0xdeadbeef");
            //       };

            //     mBus.on('error', function(err){
            //         mBus.shutdown();
            //         err.should.be.an.instanceOf(Error);
            //         err.message.should.match(/^Bad data in sent message/);
            //         done();
            //     });

            //     setTimeout(cback, delay + 10);

            // });

            // it('should handle no item on the worker queue', function(done){
            //     var mBus = messaging.connect({isResponder: true, dataQueue: "niwq", broadcastChannel: "niwqC"})
            //       , cback = function(){
            //           rc.publish(mBus.broadcastChannel, "0xdeadbeef");
            //       };
            //     mBus.on('error', function(err){
            //         mBus.shutdown();
            //         err.should.be.an.instanceOf(Error);
            //         err.message.should.equal('No data in sent message');
            //         done();
            //     });

            //     setTimeout(cback, delay + 10);

            // });

            it('should handle a mal-formed message', function(done){
                var mBus = messaging.connect()
                  , cback = function(){
                    rc.publish(mBus.listenChannel, "NOSPACES");
                  };

                mBus.on('error', function(err){
                    mBus.shutdown();
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
                    mBus.shutdown();
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
                    mBus.shutdown();
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

                mBusWorker.on('data_available', function(){
                    mBusWorker.acceptMessage(function(id, msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });

                cback1 = function(){
                    mBus.sendMessage(messaging.makeId(), {body: 'test'}, function(reply){
                        reply.should.eql({body: 'test'});
                        mBus.shutdown();
                        cback2();
                    });
                };

                cback2 = function(){
                    mBus.sendMessage(messaging.makeId(), {body: 'test'}, function(reply){
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
                mBusWorker.on('data_available', function(){
                    mBusWorker.acceptMessage(function(id, msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });

                cback = function(){
                    mBus.sendMessage(messaging.makeId(), {'body': 'test'}, function(reply){
                        reply.should.eql({'body': 'test'});
                        done();
                    });
                };

                // Need to delay just a bit to let everything start-up
                setTimeout(cback, delay);
            });

            it('should allow for multiple tasks to be added', function(done){
                var totalMsgs = 20, i
                  , count = 0
                  , makeCallback = function(total){
                      return function(){
                          var id = messaging.makeId()
                            , body = {body: 'test_'+id};
                          mBus.sendMessage(id,body, function(reply){
                              reply.should.eql({body: 'test_'+id});
                              count = count + 1;
                              if (count >= total){
                                  done();
                              }
                          });
                      };
                  };


                mBusWorker.on('data_available', function(){
                    mBusWorker.acceptMessage(function(id, msg){
                        mBusWorker.sendResponse(id, 'SUCCESS', msg);
                    });
                });


                for (i = 0; i < totalMsgs; i++){
                    setTimeout(makeCallback(totalMsgs), delay);
                }
            });

            it('should reject excessive payloads', function(done){
                var fs = require('fs')
                , sz = 1024 * 1024 * 2
                , buf = new Buffer(sz);


                fs.open('/dev/urandom', 'r', '0666', function(err, fd){
                    fs.read(fd, buf, 0, sz, 0, function(err, bytesRead, buf){
                        var str = buf.toString('base64')
                          , msg = {body: str}
                          , cback;

                        mBusWorker.on('data_available', function(){
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

                    });
                });
            });
            it('should reject message that are not JSON', function(done){
                var cback;
                mBusWorker.on('data_available', function(){
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

            it('should accept only SUCCESS, ERROR or FAILED for status', function(){
                var id = messaging.makeId()
                  , msg = {body: 'hi mom'}
                  , allowed = ['SUCCESS', 'ERROR', 'FAILED']
                  , notAllowed = ['GOOD', 1, 21.2, {test: "object"}, 'S', true]
                  , i;

                for (i = 0; i < allowed.length; i = i + 1){
                    (function(){
                        mBusWorker.sendResponse(msg, allowed[i], msg);
                    }).should.not.throw();
                }

                for (i = 0; i < notAllowed.length; i = i + 1){
                    (function(){
                        mBusWorker.sendResponse(msg, notAllowed[i], msg);
                    }).should.throw(/is not a valid status\./);
                }

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
