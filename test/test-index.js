/*global describe, it */
"use strict";

var sinon = require('sinon')
  , background_task = require('../')
  , should = require('should');


describe('node-background-task', function(){
    describe('Events', function(){
        describe('#emit()', function(){
            it('should invoke the callback', function(){
                var spy = sinon.spy()
                , bgTask = background_task.connect({});

                bgTask.on('foo', spy);
                bgTask.emit('foo');
                spy.called.should.equal.true;
            });

            it('should');

        });
    });
    describe('#connect', function(){
        it('should accept no options');
        it('should accept all options');
        it('should accept some options');
        it('should accept the option to be a worker');
    });
    describe('BackgroundTask', function(){
        describe('#end', function(){
            it('should complete task');
        });
        describe('#addTask', function(){
            it('should add a task');
        });
        describe('#completeTask', function(){
            it('should finish a task');
        });
    });
});
