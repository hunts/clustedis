"use strict";

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Pool = require('generic-pool').Pool;
var redis = require('redis');

/**
 *
 * @param {Object} netAddress
 * @param {Object} options
 * @constructor
 */
var RedisPool = function(netAddress, options) {
    var self = this;
    options = options || {};
    var poolOptions = options.pool || {};
    delete options.pool;

    poolOptions.max = poolOptions.max || 5;
    poolOptions.min = poolOptions.min || 2;
    poolOptions.idleTimeoutMillis = poolOptions.idleTimeoutMillis || 10000;
    poolOptions.reapIntervalMillis = poolOptions.reapIntervalMillis || 1000;
    poolOptions.refreshIdle = poolOptions.refreshIdle || false;
    poolOptions.log = ('log' in poolOptions) ? poolOptions.log : false;

    poolOptions.name = poolOptions.name || ('redis://' + netAddress.host + ':' + netAddress.port);

    poolOptions.create = function(callback) {
        var client = redis.createClient(netAddress.port, netAddress.host, options);
        client.on('error', function(err) {
            self.emit('error', err);
        });

        client.on('ready', function() {
            if(options.auth_pass) {
                client.auth(options.auth_pass);
            }
            self.emit('ready');
            callback(null, client);
        });
    };

    poolOptions.destroy = function(client) {
        client.quit();
        client.end();
        self.emit('destroy');
    };

    this._pool = new Pool(poolOptions);

    this.getName = function() {
        return this._pool.getName();
    };

    EventEmitter.call(this);

    return this;
};

util.inherits(RedisPool, EventEmitter);

/**
 * Request a new redis client. The callback will be called,
 * when a new client will be availabe, passing the client to it.
 *
 * @param {Function} callback
 *   Callback function to be called after the acquire is successful.
 *   The function will receive the acquired item as the first parameter.
 *
 * @param {Number} priority
 *   Optional.  Integer between 0 and (priorityRange - 1).  Specifies the priority
 *   of the caller if there are no available resources.  Lower numbers mean higher
 *   priority.
 *
 * @returns {Object} `true` if the pool is not fully utilized, `false` otherwise.
 */
RedisPool.prototype.acquire = function(callback, priority) {
    this._pool.acquire(callback, priority);
};

/**
 * Return the client to the pool, in case it is no longer required.
 *
 * @param {Object} client
 *   The acquired client to be put back to the pool.
 */
RedisPool.prototype.release = function(client) {
    this._pool.release(client);
};

// Drains the connection pool and call the callback if provided.
RedisPool.prototype.drain = function(cb) {
    var self = this;
    self._pool.drain(function() {
        self._pool.destroyAllNow();
        return cb && cb();
    });
};

// Returns factory.name for this pool
RedisPool.prototype.getName = function() {
    return this._pool.getName();
};

/**
 * Returns number of resources in the pool regardless of
 * whether they are free or in use
 * @returns {Number}
 */
RedisPool.prototype.getPoolSize = function() {
    return this._pool.getPoolSize();
};

/*
 * Returns number of unused resources in the pool
 *
 * @returns {Number}
 */
RedisPool.prototype.availableObjectsCount = function() {
    return this._pool.availableObjectsCount();
};

/**
 * Returns number of callers waiting to acquire a resource
 *
 * @returns {Number}
 */
RedisPool.prototype.waitingClientsCount = function() {
    return this._pool.waitingClientsCount();
};

/**
 *
 * @param {Object} netAddress {host: string, port: number}
 * @param {Object} options
 *  Optional. Redis connection setting and connection pool setting.
 * @returns {Object} an instance of RedisPool
 */
module.exports = function(netAddress, options) {
    return new RedisPool(netAddress, options);
};