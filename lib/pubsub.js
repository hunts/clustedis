"use strict";

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');

/**
 *
 * @constructor
 */
function PubSub(onMessage, onUnsubscribe, onPunsubscribe, logger) {
    var _self = this;
    var _serverGetter = null;
    var _redisOptions = null;
    var _subClient = null; // Redis client for subscribing
    var _pubClient = null; // Redis client for publishing
    var _channels = [];
    var _publishedQueue = new Map();
    
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
            _subClient = null;
        });

        _channels.map(function(channel) {
            client.subscribe(channel, function(err, res) {
                // nothing to do.
            });
        });
        
        closeSubClient();
        _subClient = client;
    };

    var initPub = function(client) {

        client.on('end', function() {
            _pubClient = null;
        });

        closePubClient(); // release old one if exists.
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

    var closeSubClient = function() {
        if(_subClient) {
            logger.debug('pubsub: subscription client closed');
            _subClient.quit();
            _subClient.end();
            _subClient = null;
        }
    };
    
    var closePubClient = function() {
        if(_pubClient) {
            logger.debug('pubsub: publish client closed');
            _pubClient.quit();
            _pubClient.end();
            _pubClient = null;
        }
    };

    this.LOG = logger;

    /**
     *
     * @param port
     * @param host
     */
    this.setServerGetter = function(redisOptions, serverGetter) {
        
        _serverGetter = serverGetter;
        _redisOptions = redisOptions;
        
        closeSubClient();
        closePubClient();
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
        
        if (_serverGetter) {
            this.createRedisClient(function(err, client) {
                if (!!err) {
                    _self.LOG.error(err);
                } else {
                    initSub(client);
                }
            });
        }
        if (done) {
            done(null, ['subscribe', channel, _channels.length]);
        }
    };
    
    this.createRedisClient = function(done) {
        var endpoint = _serverGetter();
        var client = redis.createClient(endpoint.port, endpoint.host, _redisOptions);
        client.on('ready', function() {
            done(null, client);
        });
        client.once('error', function(err) {
            done(err);
        });
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
        if (!_serverGetter) {
            enqueueMessage(channel, message, done);
            return;
        }

        if (!_pubClient) {
            enqueueMessage(channel, message, done);
            this.createRedisClient(function (err, client) {
                initPub(client);
            });
            return;
        }

        _pubClient.publish(channel, message, done);
    };

    this.close = function(cb) {
        
        this.LOG.info('pubsub: closing ...');

        closeSubClient();
        closePubClient();
        
        this.LOG.info('pubsub: closed');
        
        if(cb) {
            cb();
        }
    };
}

util.inherits(PubSub, EventEmitter);

module.exports = PubSub;