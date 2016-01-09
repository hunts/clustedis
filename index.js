"use strict";

var ClusterClient = require('./lib/client');

var DEFAULT_PORT = 6379;
var DEFAULT_HOST = '127.0.0.1';

exports.Client = ClusterClient;

/**
 * [
 *  {host:string, port:number}
 *  ...
 *  {host:string, port:number}
 * ], redisOptions
 * OR
 * [
 *  host:port
 *  ...
 *  host:port
 * ], redisOptions
 * OR
 * {host:string, port:number}, redisOptions
 * OR
 * host, port, redisOptions
 * OR
 * port, redisOptions
 *
 * @param host
 * @param port
 * @param redisOptions
 * @param slaveMode
 * @param logger
 */
exports.createClient = function(host, port, redisOptions, slaveMode, logger) {
    var addressArray;
    if(Array.isArray(host)) {
        addressArray = normalizeNetAddress(host);
        logger = slaveMode;
        slaveMode = redisOptions;
        redisOptions = port;
    } else if(typeof host === 'string') {
        if(typeof port === 'number' || typeof port === 'string') {
            addressArray = [{host:host, port: +port}];
        } else {
            addressArray = normalizeNetAddress([host]);
            logger = redisOptions;
            slaveMode = port;
            redisOptions = host;
        }
    } else if(typeof host === 'number') {
        addressArray = [{host: DEFAULT_HOST, port: host}];
        redisOptions = port;
    } else if(typeof host === 'object' && host.host && host.port) {
        addressArray = [host];
        logger = slaveMode;
        slaveMode = redisOptions;
        redisOptions = port;
    } else {
        logger = redisOptions;
        slaveMode = port;
        redisOptions = host;
        addressArray = [{host: DEFAULT_HOST, port: DEFAULT_PORT }];
    }

    redisOptions = redisOptions || {};
    redisOptions.debug_mode = !!redisOptions.debug_mode;
    redisOptions.return_buffers = !!redisOptions.return_buffers;
    redisOptions.auth_pass = (redisOptions.auth_pass || '') + '';
    redisOptions.keep_alive = redisOptions.keep_alive === null ? true : !!redisOptions.keep_alive;

    return new ClusterClient(addressArray, redisOptions, slaveMode, logger);
};

/**
 *
 * @param {Array} array
 * @returns {Object|Array}
 */
function normalizeNetAddress(array) {
    return array.map(function(addr) {
        if(typeof addr === 'string') {
            var arr = addr.split(':');
            return {host: arr[0], port: +arr[1]};
        } else if(Array.isArray(addr)) {
            return {host: addr[0], port: +addr[1]};
        }

        return {host: addr.host, port: +addr.port};
    });
}