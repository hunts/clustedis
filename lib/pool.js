"use strict";

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Pool = require('generic-pool').Pool;
var redis = require('redis');

/**
 *
 * @param {Object} redisOptions
 * @param {Object} poolOptions
 * @constructor
 */
function RedisPool(redisOptions, poolOptions) {
    var self = this;
    var po = poolOptions ? util._extend({}, poolOptions) : {};
    po.name = po.name || ('redis://' + redisOptions.host + ':' + redisOptions.port);
    po.max = po.max || 10;
    po.min = po.min || 5;
    po.idleTimeoutMillis = po.idleTimeoutMillis || 15000;
    po.reapIntervalMillis = po.reapIntervalMillis || 1000;
    po.refreshIdle = po.refreshIdle || false;
    po.log = ('log' in po) ? po.log : false;

    po.create = function(callback) {
        var client = redis.createClient(redisOptions.port, redisOptions.host, redisOptions);

        client.on('error', function(err) {
            self.emit('error', err, client);
        });

        client.on('ready', function() {
            if(redisOptions.auth_pass) {
                client.auth(redisOptions.auth_pass);
            }
            self.emit('ready');
            callback(null, client);
        });
    };

    po.destroy = function(client) {
        client.quit();
        client.end();
        self.emit('destroy');
    };

    this._pool = new Pool(po);

    /**
     * Returns the name of this pool.
     *
     * @returns {String}
     */
    this.getName = function() {
        return this._pool.getName();
    };

    return this;
}

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

/**
 * Request the client to be destroyed. The factory's destroy handler
 * will also be called.
 * 
 * This should be called within an acquire() block as an alternative to release().
 * 
 * @param {Object} client
 *   The acquired item to be destoyed
 */
RedisPool.prototype.destroy = function(client) {
    this._pool.destroy(client);   
}

// Drains the connection pool and call the callback if provided.
RedisPool.prototype.drain = function(cb) {
    var self = this;
    self._pool.drain(function() {
        self._pool.destroyAllNow();
        self.emit('close');
        if(cb) {
            cb();
        }
    });
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
 * @param {Object} redisOptions
 *  Redis connection setting
 * @param {Object} poolOptions
 *  Optional. Connection pool setting.
 * @returns {Object} an instance of RedisPool
 */
module.exports = RedisPool;