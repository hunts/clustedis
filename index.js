'use strict';

var ClusterClient = require('client');

var DEFAULT_PORT = 6379;
var DEFAULT_HOST = '127.0.0.1';

/**
 * [
 *  {host:string, port:number}
 *  ...
 *  {host:string, port:number}
 * ], options
 * OR
 * [
 *  host:port
 *  ...
 *  host:port
 * ], options
 * OR
 * {host:string, port:number}, options
 * OR
 * host, port, options
 * OR
 * port, options
 *
 * @param host
 * @param port
 * @param options
 */
exports.createClient = function(host, port, options) {
    var addressArray;
    if(Array.isArray(host)) {
        addressArray = normalizeNetAddress(host);
        options = port;
    } else if(typeof host === 'string') {
        if(typeof port === 'number' || typeof port === 'string') {
            addressArray = [{host:host, port: +port}];
        } else {
            addressArray = normalizeNetAddress([host]);
            options = host;
        }
    } else if(typeof host === 'number') {
        addressArray = [{host: DEFAULT_HOST, port: host}];
        options = port;
    } else if(typeof host === 'object' && host.host && host.port) {
        addressArray = [host];
        options = port;
    } else {
        options = host;
        addressArray = [{host: DEFAULT_HOST, port: DEFAULT_PORT }];
    }

    options = options || {};
    options.debugMode = !!options.debugMode;
    options.returnBuffers = !!options.returnBuffers;
    options.authPass = (options.authPass || '') + '';
    options.keepAlive = options.keepAlive == null ? true : !!options.keepAlive;

    return new ClusterClient(addressArray, options);
}

/**
 *
 * @param array
 * @returns {*|Array}
 */
function normalizeNetAddress(array) {
    return array.map(function(addr) {
        if(typeof addr === 'string') {
            var arr = addr.split(':');
            return {host: arr[0], port: +arr[1]};
        }

        return {host: addr.host, port: +addr.port};
    });
}