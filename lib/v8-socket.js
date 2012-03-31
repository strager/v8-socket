#!/usr/bin/env node

var DEFAULT_PORT = 5858;

var events = require('events');
var util = require('util');

function V8Socket(stream) {
    events.EventEmitter.call(this);

    this.seqCallbacks = { };
    this.seqID = 1;

    this.stream = stream;

    this.buffer = '';

    this.stream.setEncoding('utf8');

    var self = this;
    this.stream.on('data', function on_data(data) {
        self.buffer += data;

        self.checkBuffer();
    });
    this.stream.on('error', function on_error(err) {
        self.emit('error', err);
    });
    this.stream.on('end', function on_end() {
        self.close();
    });
    this.stream.on('close', function on_close() {
        self.emit('close');
    });
}

util.inherits(V8Socket, events.EventEmitter);

V8Socket.prototype.close = function close() {
    if (this.stream.writable) {
        this.request({ 'command': 'disconnect' });
    }

    this.stream.end();
};

V8Socket.prototype.checkBuffer = function checkBuffer() {
    // Pretty inefficient
    var headerEndIndex = this.buffer.indexOf('\r\n\r\n');
    if (headerEndIndex < 0) {
        // All headers not yet received
        return;
    }
    var dataStartIndex = headerEndIndex + 4;

    var headers = this.buffer.slice(0, headerEndIndex).split('\r\n');
    var contentLength = headers.reduce(function (acc, header) {
        var match = /^content-length:\s*(\d+)$/i.exec(header);
        if (match) {
            return Number(match[1]);
        } else {
            return acc;
        }
    }, NaN);

    if (isNaN(contentLength)) {
        this.emit('error', new Error("Could not determine content length"));
        this.close();
        return;
    }

    var dataEndIndex = dataStartIndex + contentLength;
    if (this.buffer.length < dataEndIndex) {
        // Not all data yet received
        return;
    }

    var rawData = this.buffer.slice(dataStartIndex, dataEndIndex);

    if (rawData) {
        try {
            var data = JSON.parse(rawData);
            this.receiveData(data);
        } catch (e) {
            this.emit('error', e);
            // continue
        }
    }

    this.buffer = this.buffer.slice(dataEndIndex);
    this.checkBuffer();
};

V8Socket.prototype.addSeqCallback = function addSeqCallback(fn) {
    var seqID = this.seqID;
    ++this.seqID;

    var callbacks = this.seqCallbacks;
    callbacks[seqID] = function on_seq(/* ... */) {
        delete callbacks[seqID];
        return fn.apply(this, arguments);
    };

    return seqID;
};

V8Socket.prototype.request = function request(data, callback) {
    var obj = { 'seq': 0, 'type': 'request' };

    if (typeof callback === 'function') {
        obj['seq'] = this.addSeqCallback(callback);
    }

    Object.keys(data).forEach(function (key) {
        obj[key] = data[key];
    });

    var json = JSON.stringify(obj);

    // Headers
    this.stream.write('Content-Length: ' + json.length + '\r\n');

    // Data
    this.stream.write('\r\n');
    this.stream.write(json);
};

V8Socket.prototype.receiveData = function receiveData(data) {
    this.emit('data', data);

    var body = data['body'];

    switch (data['type']) {
    case 'response':
        var seqID = Number(data['request_seq']);
        var err = data['success'] === true ? null : new Error(data['message'])

        if (Object.prototype.hasOwnProperty.call(this.seqCallbacks, seqID)) {
            this.seqCallbacks[seqID].call(null, err, body, data);
        } else if (err) {
            this.emit('error', err);
        }

        break;

    case 'event':
        this.emit('event', body);
        break;

    default:
        this.emit('error', new Error("Unknown data type: " + data['type']));
        break;
    }
};

function attach(stream) {
    return new V8Socket(stream);
}

function connect(port, host) {
    var net = require('net');
    var socket = net.connect(port || DEFAULT_PORT, host);
    return attach(socket);
}

exports.attach = attach;
exports.connect = connect;
