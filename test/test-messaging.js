/*global describe, it */
"use strict";

var sinon = require('sinon')
  , message = require('../lib/messaging')
  , should = require('should');


describe('MessageBus', function(){
    describe('#emit()', function(){
        it('should invoke the callback', function(){
            var spy = sinon.spy()
              , msgBus = new message.MessageBus();

            msgBus.on('foo', spy);
            msgBus.emit('foo');
            spy.called.should.equal.true;
        });
    });
});
