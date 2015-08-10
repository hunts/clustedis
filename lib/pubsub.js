"use strict";

/**
 *
 * @constructor
 */
function PubSub() {
    var _pool = null; // Redis connection pool.
    var _subClient = null; // Redis client for subscribing
    var _pubClient = null; // Redis client for publishing
    var _subscriptions = {};
    var _publishedQueue = {};

    var initSub = function(client) {

        client.on('message', function (channnel, message) {
            if(_subscriptions[channnel]) {
                _subscriptions[channnel](message);
            }
        });

        client.on('end', function () {
            _pool.release(client);
            _subClient = null;
        });

        for (var channel in _subscriptions) {
            client.subscribe(channel);
        }

        releaseSubClient(function() {
            // TODO ...
        });
        
        _subClient = client;
    };

    var initPub = function(client) {

        client.on('end', function() {
            _pool.release(client);
            _pubClient = null;
        });

        for(var channel in _publishedQueue) {
            var msg = _publishedQueue[channel].pop();
            while(msg) {
                client.publish(channel, msg);
                msg = _publishedQueue[channel].pop();
            }
        }

        releasePubClient();

        _pubClient = client;
    };

    var enqueueMessage = function(channel, message) {
        if(channel in _publishedQueue) {
            _publishedQueue[channel].push(message);
        } else {
            _publishedQueue[channel] = [message];
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
    this.subscribe = function(channel, onMessage) {
        _subscriptions[channel] = onMessage;

        // Redis connection pool not set yet.
        if(!_pool) return;

        if(!_subClient) {
            _pool.acquire(function(err, client) {
                initSub(client);
            });
        } else {
            _subClient.subscribe(channel);
        }
    };

    /**
     *
     * @param {String} channel
     * @param {Function} done (err, channel)
     */
    this.unsubscribe = function(channel, done) {
        if(!_subClient || !_subscriptions.hasOwnProperty(channel)) {
            done(null, channel);
            return;
        }

        delete _subscriptions[channel];
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
            enqueueMessage(channel, message);
            if (done) {
                done(null, 1);
            }
            return;
        }

        if (!_pubClient) {
            enqueueMessage(channel, message);
            _pool.acquire(function (err, client) {
                initPub(client);
            });
            if (done) {
                done(null, 1);
            }
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

module.exports = PubSub;