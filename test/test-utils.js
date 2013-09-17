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

var testWithFile;

exports.testWithFile = function(size, test){
    var fs = require('fs')
    , buf = new Buffer(size);

    fs.open('/dev/urandom', 'r', '0666', function(err, fd){
        fs.read(fd, buf, 0, size, 0, function(err, bytesRead, buf){
            var str = buf.toString('base64');
            test(str);
        });
    });
};

exports.waitForSetup = function(bgTaskOrBus, cb) {
    var pending = bgTaskOrBus.msgBus ? 6 : 3;
    var next    = function() {
        pending -= 1;
        if(0 === pending) {
            cb()
        }
    };
    ['dataClient', 'pubClient', 'subClient'].forEach(function(client) {
        if(bgTaskOrBus.msgBus) {// Background Task.
          bgTaskOrBus.msgBus[client].on('ready', next);
          bgTaskOrBus.progressBus[client].on('ready', next);
        }
        else {// Message.
          bgTaskOrBus[client].on('ready', next);
        }
    });
};