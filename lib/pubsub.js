"use strict";

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var HashMap = require("hashmap");

/**
 *
 * @constructor
 */
function PubSub(onMessage, onUnsubscribe, onPunsubscribe) {
    var _pool = null; // Redis connection pool.
    var _subClient = null; // Redis client for subscribing
    var _pubClient = null; // Redis client for publishing
    var _channels = [];
    var _publishedQueue = new HashMap();
    
    var initSub = function(client) {

        client.on('message', function (channnel, message) {
            onMessage(channnel, message);
        });
        
        client.on('unsubscribe', function (channnel, count) {
            onUnsubscribe(channnel, count);
        });
        
        client.on('punsubscribe', function (channnel, count) {
            onPunsubscribe(channnel, count);
        });

        client.on('end', function () {
            _pool.release(client);
            _subClient = null;
        });


        _channels.map(function(channel) {
            client.subscribe(channel, function(err, res) {
                // nothing to do.
            });
        });
        
        releaseSubClient(function() {
            // Do something? ...
        });
        
        _subClient = client;
    };

    var initPub = function(client) {

        client.on('end', function() {
            _pool.release(client);
            _pubClient = null;
        });

        releasePubClient(); // release old one if exists.
        _pubClient = client;
        
        _publishedQueue.forEach(function(queue, channel) {
            var msgAndCallback = null;
            while(!!(msgAndCallback = queue.pop())) {
                _pubClient.publish([channel, msgAndCallback[0]], msgAndCallback[1]);
            }
        });
    };

    var enqueueMessage = function(channel, message, callback) {
        if(_publishedQueue.has(channel)) {
            _publishedQueue.get(channel).push([message, callback]);
        } else {
            _publishedQueue.set(channel, [[message, callback]]);
        }
    };

    var releaseSubClient = function(done) {
        if(_subClient) {
            _subClient.unsubscribe(function() {
                _subClient.quit();
                _subClient.end();

                if(_pool) {
                    _pool.release(_subClient);
                }

                if(done) {
                    done();
                }
            });
        } else {
            if(done) {
                done();
            }
        }
    };

    var releasePubClient = function(done) {
        if(_pubClient) {
            _pubClient.quit();
            _pubClient.end();

            if(_pool) {
                _pool.release(_pubClient);
            }

            if(done) {
                done();
            }
        } else {
            if(done) {
                done();
            }
        }
    };

    /**
     *
     * @param pool
     */
    this.setPool = function(pool) {
        if(_pool === pool) return;

        pool.on('close', function() {
            _pool = null;
            _subClient = null;
            _pubClient = null;
        });

        releaseSubClient(function() {
            _subClient = null;
            releasePubClient(function() {
                _pubClient = null;
                _pool = pool;
            });
        });
    };

    /**
     * Subscribe message from the channel.
     *
     * @param {String} channel The message topic.
     * @param {Function} onMessage Callback function on receive new message.
     */
    this.subscribe = function(channel, done) {
        
        if (_channels.indexOf(channel) >= 0) {
            if (done) {
                done(null, ['subscribe', channel, _channels.length]);
            }
            return;
        }
        
        _channels.push(channel);
        
        if (_subClient) {
            _subClient.subscribe(channel, done);
            return;
        }
        
        if (_pool) {
            _pool.acquire(function(err, client) {
                initSub(client);
            });
        }
        if (done) {
            done(null, ['subscribe', channel, _channels.length]);
        }
    };

    /**
     *
     * @param {Array} channels
     * @param {Function} done (err, res)
     */
    this.unsubscribe = function(channel, done) {
        var idx = _channels.indexOf(channel);
        if (idx >= 0) {
            _channels.splice(idx, 1);
        }
        
        if(!_subClient) {
            done(null, channel);
            return;
        }
        
        _subClient.unsubscribe(channel, done);
    };

    /**
     * Publish a message to the target channel.
     *
     * @param {String} channel
     * @param {String} message
     * @param {Function} done (err, res)
     */
    this.publish = function(channel, message, done) {
        if (!_pool) {
            enqueueMessage(channel, message, done);
            return;
        }

        if (!_pubClient) {
            enqueueMessage(channel, message, done);
            _pool.acquire(function (err, client) {
                initPub(client);
            });
            return;
        }

        _pubClient.publish(channel, message, done);
    };

    this.close = function(cb) {
        if(_pool === null) {
            if(cb) {
                cb();
            }
            return;
        }

        releaseSubClient(function() {
            _subClient = null;
            releasePubClient(function() {
                _pubClient = null;
                _pool = null;
                if(cb) {
                    cb();
                }
            });
        });
    };
}

util.inherits(PubSub, EventEmitter);

module.exports = PubSub;
