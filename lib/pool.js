'use strict';

var poolModule = require('generic-pool');
var redis = require('redis');

/**
 *
 * @param host
 * @param port
 * @param poolOptions
 * @param redisOptions
 * @constructor
 */
var RedisPool = function(host, port, options) {
    host = host || 'localhost';
    port = port || 6379;
    options = options || {};
    var poolOptions = options.pool || {};
    delete options['pool'];
    poolOptions.max = poolOptions.max || 5;
    poolOptions.min = poolOptions.min || 2;
    poolOptions.idleTimeoutMillis = poolOptions.idleTimeoutMillis || 10000;
    poolOptions.reapIntervalMillis = poolOptions.reapIntervalMillis || 1000;
    poolOptions.refreshIdle = poolOptions.refreshIdle || false;
    poolOptions.log = ('log' in poolOptions) ? poolOptions.log : false;

    this.pool = poolModule.Pool({
        name: 'redis://' + host + ':' + port,
        create: function(callback) {
            var client = redis.createClient(port, host, options);
            client.on('ready', function() {
                callback(client);
            });
        },
        destroy: function(client) {
            return client.quit();
        },
        max: poolOptions.maxConnections,
        min: poolOptions.minConnections,
        idleTimeoutMillis: poolOptions.idleTimeoutMillis,
        reapIntervalMillis: poolOptions.reapIntervalMillis,
        refreshIdle: poolOptions.refreshIdle,
        log: poolOptions.log
    });
    this.name = this.pool.name;
    this.options = poolOptions;
}

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
    return this.pool.acquire(callback, priority);
};

/**
 * Return the client to the pool, in case it is no longer required.
 *
 * @param {Object} obj
 *   The acquired object to be put back to the pool.
 */
RedisPool.prototype.release = function(obj) {
    this.pool.release(obj);
};

/**
 *
 * @param host
 * @param port
 * @param options
 * @returns {RedisPool}
 */
exports.create = function(host, port, options) {
    return new RedisPool(host, port, options);
}