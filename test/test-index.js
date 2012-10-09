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

            it('should emit TASK_DONE when a task completes');
            it('should emit TASK_AVAILABLE when a task is added');
            it('should emit TASK_ERROR if something goes wrong');
            it('should emit both TASK_ERROR and TASK_DONE if there was an error');
            it('should have response when TASK_DONE is emitted');
            it('should have respone when TASK_ERROR is emitted');
            it('should have the task when TASK_AVAILABLE is emitted');
        });
    });

    describe('#connect', function(){
        it('should return a valid BackgroundTask with no options');
        it('should return a valid BackgroundTask with all options');
        it('should return a valid BackgroundTask with some options');
        it('should be a worker when isWorker: true');
    });

    describe('BackgroundTask', function(){
        describe('#end', function(){
            it('should not allow more tasks to complete');
        });
        describe('#addTask', function(){
            it('should call callback');
            it('should return correct status');
            it('should timeout if timeout value exceeded');
            it('should not call callback twice if timeout value exceeded');
            it('should allow custom timeout');
            it('should reject tasks over key threshold');
            it('should allow for multiple tasks to be added');
            it('should reject excessive payloads');
        });
        describe('#completeTask', function(){
            it('it should reject tasks without ids');
            it('it should reject tasks without a status');
            it('should accept only SUCCESS, ERROR or FAILED for status');
            it('should cause task to be sent to "caller"');
        });
    });
});
