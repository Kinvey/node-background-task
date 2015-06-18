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
  , testUtils = require('./test-utils')
  , async = require('async');

describe('messaging', function(){
  describe('#initialize', function(){
    var notificationBus;

    afterEach(function(done) {
      notificationBus.shutdown();
      done();
    });

    it('should return a valid NotificationBus with or without options', function(done){
        notificationBus = notification.initialize(null, function() {
          notificationBus.should.have.property('subClient');
          notificationBus.should.have.property('dataClient');
          notificationBus.should.have.property('pubClient');

          done();
    });

    });

    it('should return a valid NotificationBus with all options', function(done){
      notificationBus = notification.initialize({
        baseHash: ":someBaseHash",
        hashMap: "someHashMap",
        host: "0.0.0.0",
        port: "6379",
        isResponder: true
      }, function() {
        notificationBus.should.have.property('subClient');
        notificationBus.should.have.property('dataClient');
        notificationBus.should.have.property('pubClient');
        notificationBus.baseHash.should.eql(':someBaseHash');
        notificationBus.hashMap.should.eql('someHashMap');
        notificationBus.pubClient.client.host.should.eql('0.0.0.0');
        notificationBus.pubClient.client.port.should.eql('6379');
        done();
      });
    });


    it('should return a valid NotificationBus with some options', function(done){
      notificationBus = notification.initialize({
        host: "localhost",
        isResponder: true
      }, function() {
        notificationBus.should.have.property('subClient');
        notificationBus.should.have.property('pubClient');
        notificationBus.should.have.property('dataClient');

        done();
      });

    });

    it('should be a worker when isWorker: true', function(done){
      var hashName = ":testHash"
        , status = 'SUCCESS'
        , message = '{"test":"message", "_messageId": "0xdeadbeef", "_listenChannel":"dummy"}'
        , opts = {baseHash: hashName, isWorker: true}
        , rcPubSub = redis.createClient()
        , rcData = redis.createClient();

      notificationBus = notification.initialize(opts, function() {
        notificationBus.once('notification_received', function(id){
          notificationBus.processNotification(id._id, status, function(err, msg){
            msg.test.should.eql(JSON.parse(message).test);
            done();
          });
        });

        rcData.hset(status.toLowerCase() + hashName, "0xdeadbeef", message);
        rcPubSub.publish(notificationBus.broadcastChannel, JSON.stringify({_id:"0xdeadbeef", status: status}));
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

      notificationBus = notification.initialize({password: 'hiFriends'}, function() {
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

      notificationBus       = notification.initialize(null, function() {
        notificationBusWorker = notification.initialize({isWorker: true}, function() {
          done();
        });
      });
    });

    afterEach(function(done) {
      notificationBus.shutdown();
      notificationBusWorker.shutdown();
      rc.flushall();
      done();
    });

    describe('Error Handling', function(){
      it('should handle bad items on the worker queue', function(done){
        var status= "biwq";
        var nBus = notification.initialize({isWorker: true, broadcastChannel: "biwqC"}, function() {
          nBus.once('notification_received', function(id){
            nBus.processNotification(id, status, function(err,rep){
              err.should.be.an.instanceOf(Error);
              err.message.should.match(/No message for key/);
              done();
            });
          });
          rc.hset(status + nBus.baseHash, "0xdeadbeef", "this is not json");
          rc.publish(nBus.broadcastChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
        });
      });

      it('should handle no item on the worker queue', function(done){
        var status = "niwq";
        var nBus = notification.initialize({isWorker: true, broadcastChannel: "niwqC"}, function() {
          nBus.once('notification_received', function(id){
            nBus.processNotification(id, status, function(err, rep){
              err.should.be.an.instanceOf(Error);
              err.message.should.match(/No message for key/);
              done();
            });
          });
          rc.publish(nBus.broadcastChannel, JSON.stringify({id: "0xdeadbeef", status: status}));

        });
      });

      it('should handle a mal-formed message', function(done){
        notificationBus.on('error', function(err){
          err.should.be.an.instanceOf(Error);
          err.message.should.equal('Invalid message received!');
          done();
        });
        rc.publish(notificationBus.listenChannel, "NOSPACES");
      });

      it('should handle JSON that is corrupt', function(done){
        var status="plkj";

        notificationBus.once('notification_received', function(id) {
          notificationBus.processNotification(id.id, status, function(err, rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/^Bad data in sent message/);
            done();
          });
        });

        rc.hset(status + notificationBus.baseHash, "0xdeadbeef", "this is not json");
        rc.publish(notificationBus.listenChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
      });

      it('should handle when an empty item is pulled from the queue', function(done){
        var status="plkj";

        notificationBus.once('notification_received', function(id) {
          notificationBus.processNotification(id.id, status, function(err, rep){
            err.should.be.an.instanceOf(Error);
            err.message.should.match(/^No message for key/);
            done();
          });
        });

        rc.publish(notificationBus.listenChannel, JSON.stringify({id: "0xdeadbeef", status: status}));
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

    describe('#sendMessage', function(){

      it('should reject notifications without ids', function(done){
        notificationBusWorker.sendNotification(notificationBus.broadcastChannel, null, {body: "hi"}, 'SUCCESS', function (err, result) {
          err.should.be.instanceOf(Error);
          err.message.should.match(/^Invalid Argument/);
          done();
        });
      });

      it('should reject notifications without a status', function(done){
        notificationBusWorker.sendNotification(notificationBus.broadcastChannel, notification.makeId(), {body: "hi"}, undefined, function (err, result) {
          err.should.be.instanceOf(Error);
          err.message.should.match(/^Invalid Argument/);
          done();
        });
      });

      it('should reject empty messages', function(done){
        notificationBusWorker.sendNotification(notificationBus.broadcastChannel, notification.makeId(), "SOMESTATUS", undefined, function (err, result) {
          err.should.be.instanceOf(Error);
          err.message.should.match(/^Invalid Argument/);
          done();
        });
      });

      it('should call callback', function(done){
        notificationBusWorker.once('notification_received', function(notification){
          notificationBusWorker.processNotification(notification.id, 'NEWTASK', function(msg){
            notificationBusWorker.sendNotification(notification._listenChannel, notification.id, msg, 'SUCCESS');
          });
        });

        var cid = notification.makeId();
        notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {'body': 'test'}, "NEWTASK", function(err, reply){
          reply.should.eql(notificationBus.listenChannel);
          done();
        });
      });

      it('should allow metadata to be passed', function(done) {
        notificationBusWorker.once('notification_received', function(notification) {
          notification.id.should.eql(cid);
          notification.metadata.should.eql({some:'metadata'});
          done();
        });

        var cid = notification.makeId();
        notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {'body':'test'}, 'NEWTASK', {some:'metadata'}, function(err, reply){});
      });

      it('should send message without metadata', function(done) {
        notificationBusWorker.once('notification_received', function(notification) {
          notification.id.should.eql(cid);
          should.not.exist(notification.metadata);
          done();
        });

        var cid = notification.makeId();
        notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {'body':'test'}, 'NEWTASK', function(err, reply){});
      });

      it('should allow for multiple tasks to be added', function(done){
        var count = 5,
          tasksToAdd = [];

        var task = function(cb) {
          var cid = notification.makeId();
          notificationBus.sendNotification(notificationBus.broadcastChannel, cid, {'body': 'test'}, "NEWTASK", function(err, reply){
            cb();
          });
        };

        var callback = function(err, reply) {
          should.not.exist(err);
          should.exist(reply);
          rc.hlen("newtask" + notificationBus.baseHash, function(err, result) {
            should.not.exist(err);
            result.should.eql(5);
            done();
          });
        };

        for (var i = 0; i < count; i++) {
          tasksToAdd.push(task);
        }

        async.parallel(tasksToAdd, callback);
      });

      it('should reject excessive payloads', function(done){
        var test
          , twoMegs = 2 * 1024 * 1024;

        test = function(str){
          var msg = {body: str}

          notificationBusWorker.once('notification_received', function(task){
            notificationBusWorker.processNotification(task.id, "NEWTASK", function(err, msg){
              notificationBusWorker.sendNotification(task._listenChannel, task.id, msg, 'SUCCESS');
            });
          });

          notificationBus.sendNotification(notificationBus.broadcastChannel, notification.makeId(), msg, "NEWTASK", function(err, reply){
            err.should.be.instanceOf(Error);
            err.message.should.match(/^The message has exceeded the payload limit of/);
            done();
          });
        };

        testUtils.testWithFile(twoMegs, test);
      });

      it('should allow larger payloads to be set', function(done){
        var test
          , twoMegs = 2 * 1024 * 1024
          , threeMegs = 3 * 1024 * 1024;

        notificationBus = notification.initialize({maxPayloadSize: threeMegs});
        notificationBusWorker = notification.initialize({isWorker: true, maxPayloadSize: threeMegs});

        test = function(str){
          var msg = {body: str}

          notificationBusWorker.once('notification_received', function(task){
            notificationBusWorker.processNotification(task.id, "NEWTASK", function(err, msg){
              notificationBusWorker.sendNotification(task._listenChannel, task.id, msg, 'SUCCESS');
            });
          });

          notificationBus.sendNotification(notificationBus.broadcastChannel, notification.makeId(), msg, "NEWTASK", function(err, reply){
            should.not.exist(err);
            should.exist(reply);
            reply.should.match(/^msgChannels:/);
            done();
          });
        };

        testUtils.testWithFile(twoMegs, test);
      });

      it('should allow client and server payload limits to be different', function(done){
        var test
          , twoMegs = 2 * 1024 * 1024
          , threeMegs = 3 * 1024 * 1024;


        notificationBusWorker = notification.initialize({isWorker: true, maxPayloadSize: threeMegs});

        test = function(str){
          var msg = {body: str}
          var smallMsg = {body: "Small"};

          notificationBusWorker.on('notification_received', function(task){
            notificationBusWorker.processNotification(task.id, "NEWTASK", function(err, result){
              notificationBusWorker.sendNotification(result._listenChannel, task.id, msg, 'SUCCESS');
            });
          });

          notificationBus.on('notification_received', function(taskResult) {
            notificationBus.processNotification(taskResult.id, "SUCCESS", function(err, result) {
              should.not.exist(err);
              should.exist(result);
              JSON.stringify(result).length.should.be.greaterThan(twoMegs);
              done();
            });
          });

          notificationBus.sendNotification(notificationBus.broadcastChannel, notification.makeId(), msg, "NEWTASK", function(err, reply){
            err.should.be.instanceOf(Error);
            err.message.should.match(/^The message has exceeded the payload limit of/);
            notificationBus.sendNotification(notificationBus.broadcastChannel, notification.makeId(), smallMsg, "NEWTASK", function(err2, result){
              should.not.exist(err2);
              should.exist(result);
              result.should.match(/^msgChannels:/);
            });
          });

        };

        testUtils.testWithFile(twoMegs, test);
      });

      it('should reject message that are not JSON', function(done){
        var cback;
        notificationBusWorker.once('notification_received', function(task){
          notificationBusWorker.processNotification(task.id, "NEWTASK", function(err, msg){
            notificationBusWorker.sendNotification(task._listenChannel, task.id, msg, 'SUCCESS');
          });
        });

        cback = function(){
          notificationBus.sendNotification(notificationBus.broadcastChannel, notification.makeId(), cback, "NewTask", function(err, reply){
            err.should.be.instanceOf(Error);
            err.message.should.equal('Error converting message to JSON.');
            done();
          });
        };
        cback();
      });

      it('should allow to set the broadcast channel at send', function(done) {
        var nBus = notification.initialize({isWorker: true, broadcast: "biwqC"}, function() {
          var stdReceived = false;
          var customReceived = false;


          var verifyCompletion = function() {
            if (stdReceived && customReceived) {
              return done();
            }
            return;
          };

          notificationBusWorker.once('notification_received', function(task) {
            task.id.should.eql('123456');
            task.id.should.not.eql('654321');
            stdReceived = true;
            verifyCompletion();
            notificationBusWorker.processNotification(task.id, "NEWTASK", function(err, msg) {
              notificationBusWorker.sendNotification(task._listenChannel, task.id, msg, 'SUCCESS');
            });
          });

          nBus.once('notification_received', function(task) {
            task.id.should.eql('654321');
            task.id.should.not.eql('123456');
            customReceived = true;
            verifyCompletion();
            nBus.processNotification(task.id, "NEWTASK", function(err, msg) {
              nBus.sendNotification(task._listenChannel, task.id, msg, 'SUCCESS');
            });
          });

          notificationBus.sendNotification('biwqC', '654321', {task: 'message'}, "NEWTASK", function(err, result) {
            notificationBus.sendNotification(notificationBus.broadcastChannel, '123456', {task: 'message'}, "NEWTASK", function(err, result2) {
            });
          });
        });


      });
    });

    describe('#processNotification', function(){
      it('should reject no parameters', function(){
        (function() {
          notificationBusWorker.processNotification(null, null, function(err, result) {
            err.should.be.instanceOf(Error);
            err.message.should.equal("Invalid Arguments");
          });
        })();
      });

      it('should reject missing ids', function(){
        (function() {
          notificationBusWorker.processNotification(null, "status", function(err, result){
            err.should.be.instanceOf(Error);
            err.message.should.equal("Invalid Arguments");
          });
        })();
      });

      it('should reject messages not in redis', function(done){
        notificationBusWorker.processNotification("NOT FOUND", "ANYSTATUS",function(err, reply){
          err.should.be.an.instanceOf(Error);
          err.message.should.match(/^No message for key/);
          done();
        });

      });
    });

    describe('#makeId', function(){
      it('should make unique ids', function(){
        var a = notification.makeId()
          , b = notification.makeId()
          , c = notification.makeId()
          , i, id, last;

        a.should.not.equal(b);
        a.should.not.equal(c);
        b.should.not.equal(c);

        last = c;
        for (i = 0; i < 10000; i = i + 1){
          id = notification.makeId();
          id.should.not.equal(last);
          last = id;
        }
      });
    });
  });
});