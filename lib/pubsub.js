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
            _subscriptions[channnel](message);
        });

        client.on('end', function () {
            _subClient = null;
            _pool.release(client);
        });

        for (var channel in _subscriptions) {
            client.subscribe(channel);
        }

        if (!!_subClient) {
            _subClient.quit();
            _subClient.end();
            _pool.release(_subClient);
        }
        _subClient = client;
    };

    var initPub = function(client) {

        client.on('end', function() {
            _pubClient = null;
            _pool.release(client);
        });

        for(var channel in _publishedQueue) {
            var msg = _publishedQueue[channel].pop();
            while(msg) {
                client.publish(channel, msg);
                msg = _publishedQueue[channel].pop();
            }
        }

        if(!!_pubClient) {
            _pubClient.quit();
            _pubClient.end();
            _pool.release(_pubClient);
        }

        _pubClient = client;
    };

    var enqueueMessage = function(channel, message) {
        if(channel in _publishedQueue) {
            _publishedQueue[channel].push(message);
        } else {
            _publishedQueue[channel] = [message];
        }
    };

    /**
     *
     * @param pool
     */
    this.setPool = function(pool) {

        if (_subClient) {
            _subClient.quit();
            _subClient.end();
            _pool.release(_subClient);
        }

        if (_pubClient) {
            _pubClient.quit();
            _pubClient.end();
            _pool.release(_pubClient);
        }

        _pool = pool;
        _subClient = null;
        _pubClient = null;
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
        if(!_pool) {
            return;
        }

        if(!_subClient) {
            _pool.acquire(function(err, client) {
                initSub(client);
            });
        } else {
            _subClient.subscribe(channel);
        }
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
}

module.exports = PubSub;