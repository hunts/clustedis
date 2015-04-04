"use strict";

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');
var commands = require('./commands');
var slot = require('./slot');
var PubSub = require('./pubsub');

/**
 *
 *
 * @param {Object} netAddressArray {host: string, port: number}
 * @param options
 * @constructor
 */
function ClusterClient(netAddressArray, options) {
    options = options || {};
    var self = this;
    var _slotProxyArray = [];
    var _client = redis.createClient(netAddressArray[0].port, netAddressArray[0].host);
    _client.on('ready', function() {
        _client.send_command('CLUSTER', ['SLOTS'], function(err, definitions) {
            var numberOfSlots = definitions.length;
            var numberOfReady = 0;
            for(var i = 0; i < numberOfSlots; i++) {
                var slotDef = definitions[i];
                var slotProxy = slot.createProxy(
                    slotDef[0], // Start slot range
                    slotDef[1] // End slot range
                );

                slotProxy.on('ready', function() {
                    numberOfReady += 1;
                    if(numberOfReady == numberOfSlots) {
                        self.on_ready();
                    }
                });

                _slotProxyArray.push(slotProxy);

                for(var nodeIndex = 2; nodeIndex < slotDef.length; nodeIndex++) {
                    var node = slotDef[nodeIndex];
                    if(node[0] == '127.0.0.1') {
                         node[0] = _client.connectionOption.host;
                    }
                    if(nodeIndex === 2) {
                        slotProxy.setMaster(node[0], node[1]);
                    } else {
                        slotProxy.addSlave(node[0], node[1]);
                    }
                }

                slotProxy.initPool(options);
            }

            _client.quit();
            _client.end();
            _client = null;
        });
    });

    this.log = function() {
        if (options && options.debug_mode) {
            console.log.apply(null, arguments);
        }
    };

    /**
     *
     * @param {Array} keys
     * @returns {RedisPool}
     */
    this.getPool = function(keys) {

        // do not have key for command
        if(!keys || (Array.isArray(keys) && keys.length === 0)) {
            return _slotProxyArray[0].getPool();
        }

        if(!Array.isArray(keys)) {
            keys = [keys];
        }

        var _slot = null;

        for(var k=0; k<keys.length; k++) {
            var tmpSlot = slot.calculateSlot(keys[k]);
            this.log('slot of key "%s" is %d', keys[k], tmpSlot);
            if(_slot === null) {
                _slot = tmpSlot;
            } else if(_slot != tmpSlot) {
                throw new Error('Keys are in different slots');
            }
        }

        for(var s=0; s<_slotProxyArray.length; s++) {
            if(_slotProxyArray[s].accept(_slot)) {
                return _slotProxyArray[s].getPool();
            }
        }

        throw new Error('No slot serving for key(s) ' + keys);
    };

    this.status = 'init';
    this.pubsub = new PubSub();
    init_commands(this);
}
util.inherits(ClusterClient, EventEmitter);


ClusterClient.prototype.on_ready = function() {
    this.log('cluster client is ready');
    this.pubsub.setPool(this.getPool('PUBSUB'));
    this.status = 'ready';
    this.emit('ready');
};

function init_commands(self) {
    var _prototype = ClusterClient.prototype;
    for(var command in commands) {
        (function (cmd) {
            _prototype[cmd] = function () {
                var argsAndFn = extractArgsAndCallback(arguments);
                var keys = findKeysFromArgs(commands[cmd], argsAndFn[0]);
                try {
                    var _pool = self.getPool(keys);
                    _pool.acquire(function (err, client) {
                        client.send_command(cmd, argsAndFn[0], function (err, res) {
                            _pool.release(client);
                            if (argsAndFn[1]) {
                                argsAndFn[1](err, res);
                            }
                        });
                    });
                } catch(err) {
                    if(argsAndFn[1]) {
                        argsAndFn[1](err);
                    }
                }
            };
        })(command);

        _prototype[command.toUpperCase()] = _prototype[command];
    }

    _prototype.subscribe = function(channel, onMessage) {
        self.pubsub.subscribe(channel, onMessage);
    };

    _prototype.publish = function(channel, message, done) {
        self.pubsub.publish(channel, message, done);
    };
}



/**
 * Extract command args and callback function from arguments object.
 *
 * @param {Object} args The arguments object passed to function.
 * @returns {Array} [commandArgs, callbackFunction]
 */
function extractArgsAndCallback(args) {

    if(!Array.isArray(args)) {
        var cmdArgs = [];
        var callbackFn = null;
        for(var index in args) {
            if(!isFunction(args[index])) {
                if(Array.isArray(args[index])) {
                    Array.prototype.push.apply(cmdArgs, args[index]);
                } else {
                    cmdArgs.push(args[index]);
                }
            } else {
                callbackFn = args[index];
                break;
            }
        }
        return [cmdArgs, callbackFn];
    } else {

        if (Array.isArray(args[0])) {
            return [args[0], args[1]];
        }

        if (!isFunction(args[args.length - 1])) {
            return [args, null];
        } else {
            return [args, args.pop()];
        }
    }
}

/**
 * Returns true if the input obj is a function.
 *
 * @param {Object} obj
 * @returns {boolean}
 */
function isFunction(obj) {
    return !!(obj && obj.constructor && obj.call && obj.apply);
}

/**
 * Finds keys from command args.
 *
 * @param {Object} commandMeta
 * @param {Array} args Command args
 * @returns {Array|String} A list of string.
 */
function findKeysFromArgs(commandMeta, args) {
    var keyPosition = commandMeta[2];

    // command do not need a key
    if(!keyPosition) {
        return null;
    }

    // init key array with first key.
    // command name is not included in args, so -1.
    var keys = [args[keyPosition - 1]];

    var lastKeyPosition = commandMeta[3];
    var keyStep = commandMeta[4];

    // only have one key
    if(keyPosition === lastKeyPosition || !keyStep) {
        return keys;
    }

    // command name is not included in args, so -1.
    keyPosition += keyStep - 1;
    while(args[keyPosition]) {
        keys.push(args[keyPosition]);
        keyPosition += keyStep;
    }

    return keys;
}

module.exports = ClusterClient;