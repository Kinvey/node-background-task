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
  , notification = require('../lib/notification_bus')
  , should = require('should')
  , redis = require('redis')
  , util = require('util')
  , delay = 60 // This allows object creation to always finish
  , testUtils = require('./test-utils')
  , ll = function(m){
    var d = new Date()
      , t = d.toISOString();
    util.debug(t + ": " + m);
  };

describe('messaging', function(){
  describe('#initialize', function(){
    it('should return a valid NotificationBus with or without options', function(){
      var notificationBus = notification.initialize();

      notificationBus.should.have.property('subClient');
      notificationBus.should.have.property('dataClient');
      notificationBus.should.have.property('pubClient');

      notificationBus.shutdown();
    });

    it('should return a valid NotificationBus with all options', function(){
      var notificationBus = notification.initialize({
        baseHash: ":someBaseHash",
        hashMap: "someHashMap",
        host: "0.0.0.0",
        port: "6379",
        isResponder: true
      });
      notificationBus.should.have.property('subClient');
      notificationBus.should.have.property('dataClient');
      notificationBus.should.have.property('pubClient');
      notificationBus.baseHash.should.eql(':someBaseHash');
      notificationBus.hashMap.should.eql('someHashMap');
      notificationBus.pubClient.client.host.should.eql('0.0.0.0');
      notificationBus.pubClient.client.port.should.eql('6379');

      notificationBus.shutdown();
    });


    it('should return a valid NotificationBus with some options', function(){
      var notificationBus = notification.initialize({
        host: "localhost",
        isResponder: true
      });
      notificationBus.should.have.property('subClient');
      notificationBus.should.have.property('pubClient');
      notificationBus.should.have.property('dataClient');

      notificationBus.shutdown();
    });

    it('should be a worker when isWorker: true', function(done){
      var hashName = ":testHash"
        , status = 'SUCCESS'
        , message = '{"test":"message", "_messageId": "0xdeadbeef", "_listenChannel":"dummy"}'
        , opts = {baseHash: hashName, isWorker: true}
        , rcPubSub = redis.createClient()
        , rcData = redis.createClient()
        , notificationBus = notification.initialize(opts);

      notificationBus.once('notification_received', function(id){
        notificationBus.processNotification(id._id, status, function(err, msg){
          notificationBus.shutdown();
          msg.test.should.eql(JSON.parse(message).test);
          done();
        });
      });

      testUtils.waitForSetup(notificationBus, function() {
        rcData.hset(status.toLowerCase() + hashName, "0xdeadbeef", message);
        rcPubSub.publish("msgChannels:broadcast", JSON.stringify({_id:"0xdeadbeef", status: status}));
      });
    });

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
      var notificationBus = notification.initialize({password: 'hiFriends'}, function() {
        for (var i = 0; i < messages.length; i++) {
          if (messages[i] = "Warning: Redis server does not require a password, but a password was supplied.") {
            warnMsg = messages[i];
          }
        }
        warnMsg.should.eql("Warning: Redis server does not require a password, but a password was supplied.");
        console.log = x;
        notificationBus.shutdown();
        done();
      });

    });
  });

  describe('NotificationBus', function(){
    var notificationBus
      , notificationBusWorker
      , rc = redis.createClient();

    beforeEach(function(done){
      rc.flushall();

      if (notificationBus){
        notificationBus.shutdown();
      }

      if (notificationBusWorker){
        notificationBusWorker.shutdown();
      }

      notificationBus       = notification.initialize();
      notificationBusWorker = notification.initialize({isWorker: true});

      // Wait until setup is complete.
      var pending = 2;
      var next    = function() {
        pending -= 1;
        if(0 === pending) {
          done();
        }
      };
      testUtils.waitForSetup(notificationBus, next);
      testUtils.waitForSetup(notificationBusWorker, next);
    });

    describe('Error Handling', function(){
      it('should handle bad items on the worker queue', function(done){
        var status= "biwq";
        var nBus = notification.initialize({isWorker: true, broadcastChannel: "biwqC"})

        nBus.once('notification_received', function(id){
          nBus.processNotification(id, status, function(err,rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/No message for key/);
            done();
          });
        });

        testUtils.waitForSetup(nBus, function() {
          rc.hset(status + nBus.baseHash, "0xdeadbeef", "this is not json");
          rc.publish(nBus.broadcastChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
        });
      });

      it('should handle no item on the worker queue', function(done){
        var status = "niwq";
        var nBus = notification.initialize({isWorker: true, broadcastChannel: "niwqC"})

        nBus.once('notification_received', function(id){
          nBus.processNotification(id, status, function(err, rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/No message for key/);
            done();
          });
        });

        testUtils.waitForSetup(nBus, function() {
          rc.publish(nBus.broadcastChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
        });
      });

      it('should handle a mal-formed message', function(done){
        var nBus = notification.initialize();

        nBus.on('error', function(err){     console.log(err);
          err.should.be.an.instanceOf(Error);
          err.message.should.equal('Invalid message received!');
          done();
        });

        testUtils.waitForSetup(nBus, function() {
          rc.publish(nBus.listenChannel, "NOSPACES");
        });
      });

      it('should handle JSON that is corrupt', function(done){
        var nBus = notification.initialize();
        var status="plkj";

        nBus.once('notification_received', function(id) {
          nBus.processNotification(id.id, status, function(err, rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/^Bad data in sent message/);
            done();
          });
        });


        testUtils.waitForSetup(nBus, function() {
          rc.hset(status + nBus.baseHash, "0xdeadbeef", "this is not json");
          rc.publish(nBus.listenChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
        });

      });

      it('should handle when an empty item is pulled from the queue', function(done){
        var  nBus = notification.initialize();
        var status="plkj";

        nBus.once('notification_received', function(id) {
          nBus.processNotification(id.id, status, function(err, rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/^No message for key/);
            done();
          });
        });

        testUtils.waitForSetup(nBus, function() {
          rc.publish(nBus.listenChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
        });
      });
    });

    describe('#shutdown', function(){
      it('should not allow more tasks to complete', function(done){
        var cback;
        var status="plmk";

        notificationBusWorker.once('notification_received', function(id){
          notificationBusWorker.processNotification(id, status, function(err, msg){
            notificationBusWorker.sendNotification(notificationBus.listenChannel, id, msg, 'SUCCESS');
          });
        });

        cback = function(){
          var cid = "abcdefg";
          notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {body: 'test'}, "NEWTASK", function(err, reply){
            err.should.be.an.instanceOf(Error);
            err.message.should.equal("Attempt to use shutdown Notification Bus.");
            done();
          });
        };

        var cid = "poiut";
        notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {body: 'test'}, "NEWTASK", function(err, reply){
          reply.should.eql(notificationBus.listenChannel);
          notificationBus.shutdown();
          cback();
        });
      });
    });
    /*
    describe('#sendMessage', function(){
      it('should call callback', function(done){
        mBusWorker.once('data_available', function(id){
          mBusWorker.acceptMessage(id, function(msg){
            mBusWorker.sendResponse(id, 'SUCCESS', msg);
          });
        });

        var cid = messaging.makeId();
        mBus.sendMessage(cid, {'body': 'test'}, function(reply){
          reply.should.eql({'body': 'test'});
          done();
        });
      });

      it('should allow for multiple tasks to be added', function(done){
        done();
      });

      it('should reject excessive payloads', function(done){
        var test
          , twoMegs = 2 * 1024 * 1024;

        test = function(str){
          var msg = {body: str}

          mBusWorker.once('data_available', function(){
            mBusWorker.acceptMessage(function(id, msg){
              mBusWorker.sendResponse(id, 'SUCCESS', msg);
            });
          });

          mBus.sendMessage(messaging.makeId(), msg, function(reply){
            reply.should.be.instanceOf(Error);
            reply.message.should.equal('Payload too large!');
            done();
          });
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
        cback();
      });
    });

    describe('#processMessage', function(){
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
        }, delay);
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
        }, delay);
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
    });  */
  });
});