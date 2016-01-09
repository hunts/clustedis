'use strict';

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');
var Shard = require('./shard');


/**
 *
 * @param {Shard} shard
 * @param {Object} redisOptions 
 * @param {Number} slaveMode
 *  0 or null: Do not send reads to slave in any case.
 *  1: Sends reads to slave only when master is not available.
 *  2: Sends all reads to slave.
 * @param {Object} logger
 * @constructor
 */
function ShardProxy(shard, redisOptions, slaveMode, logger) {
    var _self = this;
    var _redisOptions = redisOptions;
    var _master = null;
    var _masterClient = null;
    var _slaveMap = new Map();
    var _readSlaveOnFailed = (slaveMode === 1);
    var _readSlaveAlways = (slaveMode === 2);
    var LOG = logger;
    
    this.start = shard.slotStart;
    this.end = shard.slotEnd;

    /**
     * Returns a value indicating whether or not this proxy can serve the given slot.
     * @param {Number} slot
     */
    this.accept = function(slot) {
        return slot >= this.start && slot <= this.end;
    };

    this.toString = function() {
        return 'shard proxy of [' + this.start + ', ' + this.end + ']';
    };

    this.setMaster = function(endpoint) {
        
        if(_master && _masterClient && _masterClient.connected && _master.host == endpoint.host && _master.port === endpoint.port) {
            return;
        }
        
        _master = endpoint;
        
        var oldClient = _masterClient;
        var masterName = `${endpoint.host}:${endpoint.port}`;
        
        LOG.debug(`${masterName} begin serving at ${new Date()}`);
        
        if (_slaveMap.has(masterName)) {
            _masterClient = _slaveMap.get(masterName);
            _slaveMap.delete(masterName);
            this.onReady();
        } else {
            _masterClient = this.createRedisClient(_master.port, _master.host, false, _redisOptions);
        }
            
        if (oldClient) {
            oldClient.end();
        }
    };

    this.getMaster = function() {
        return _master;
    };

    this.setSlaves = function(endpointMap) {
        
        endpointMap.forEach(function(endpoint, name) {
            if (!_readSlaveOnFailed && !_readSlaveAlways) {
                return;
            }
            
            var client = _self.createRedisClient(endpoint.port, endpoint.host, true, _redisOptions);
            if (!_slaveMap.has(name)) {
                _slaveMap.set(name, client);
            }
        });
        
        var names2delete = new Set();
        _slaveMap.forEach(function(endpoint, name) {
            if (!endpointMap.has(name)) {
                names2delete.add(name);
            }
        });
        names2delete.forEach(function(name) {
            _slaveMap.delete(name);
        });
    };

    /**
     * 
     */
    this.getClient = function(isReadCommand) {
        
        if (!_masterClient.connected) {
            _self.emit('connection_error', _master);
        }
        
        if (!isReadCommand || _slaveMap.size === 0) {
            return _masterClient;
        }
        
        var slaveClients = _slaveMap.values();
        
        if (_readSlaveOnFailed && !_masterClient.connected) {
            return slaveClients.next().value;
        }
        
        if (_readSlaveAlways) {
            var random = Math.random() * _slaveMap.size;
            for (var idx = 0; idx < random; idx++) {
                slaveClients.next();
            }
            return _slaveMap.get(slaveClients.next().value);
        }
        
        return _masterClient;
    };

    this.close = function(done) {
        
        LOG.debug(`${this.toString()} closing ...`);
        
        this.start = -1;
        this.end = -1;
        
        _master = null;
        if(_masterClient) {
            if (_masterClient.connected) {
                var endpoint = `${_masterClient.connection_options.host}:${_masterClient.connection_options.port}`;
                LOG.debug(`-- connection to master ${endpoint} closing ...`);
                _masterClient.quit(function() {
                    LOG.debug(`-- connection to master ${endpoint} closed`);
                });
            } else {
                _masterClient.end();
            }
            _masterClient = null;
        }
       
        _slaveMap.forEach(function(client, name) {
            if (client.connected) {
                LOG.debug(`-- connection to slave ${name} closing ...`);
                client.quit(function() {
                    LOG.debug(`-- connection to slave ${name} closed`);
                });
            } else {
                client.end();
            }
        });
        _slaveMap.clear();
        
        if(done) {
            done();
        }
    };
    
    /**
    * 
    */
    this.createRedisClient = function(port, host, isSlave, options) {

        var client = redis.createClient(port, host, options);
        
        client.on('ready', function() {
            if(options && !!options.auth_pass) {
                client.auth(options.auth_pass);
            }

            if (isSlave) {
                client.send_command('readonly', [], function(err, res) {
                    //
                });
            }
        });

        client.on('error', function(err) {
            _self.emit('connection_error', { host: host, port: port });
        });
        
        return client;
    };
    
    this.setMaster(shard.master);
    this.setSlaves(shard.slaveMap);
}

util.inherits(ShardProxy, EventEmitter);

/**
 * Creates a shard proxy.
 *
 * @param {Object} shard
 * @param {Object} redisOptions
 * @param {Number} slaveMode
 * @param {Object} logger
 * 
 * @returns {ShardProxy} A instance of shard proxy.
 */
exports.create = function(shard, redisOptions, slaveMode, logger) {
    return new ShardProxy(shard, redisOptions, slaveMode, logger);
};

/*
 * http://redis.io/topics/cluster-spec#appendix
 *
 * Copyright 2001-2010 Georges Menie (www.menie.org)
 * Copyright 2010 Salvatore Sanfilippo (adapted to Redis coding style)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *
 * CRC16 implementation according to CCITT standards.
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

var TABLE = new Int16Array([
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
]);

/**
 *
 * @param {Buffer} buf
 * @returns {Number}
 */
function crc16(buf) {
    var crc = 0;
    for(var i = 0, len = buf.length; i < len; i++) {
        crc = TABLE[((crc >> 8) ^ buf[i]) & 0x00ff] ^ ((crc << 8) & 0xffff);
    }
    return crc;
}
exports.crc16 = crc16;

/**
 * Returns the slot of a given key.
 *
 * @param {String} key
 * @returns {Number} Slot
 */
exports.calculateSlot = function(key) {
    
    var slot;
    
    if (Buffer.isBuffer(key)) {
        var hashBuffer;
        var hashStart = -1;
        var hashEnd = -1;
        
        for(var i=0; i<key.length; i++) {
            var ch = String.fromCharCode(key[i]);
            if (hashStart < 0 && ch == '{') {
                hashStart = i;
            } else if(hashEnd < hashStart && ch == '}') {
                hashEnd = i;
                break;
            }
        }
        
        if (hashStart < 0 || hashEnd <= hashStart) {
            hashBuffer = key;
        } else {
            hashBuffer = key.slice(hashStart + 1, hashEnd);
        }
        
        slot = crc16(hashBuffer) % 16384;
        
    } else {
        key = (key === null) ? '' : key;
        // finds hash tag in key
        var groups = key.match(/^[^{]*{([^}]+).*$/);
        slot = crc16(new Buffer((!!groups && groups[1]) || key)) % 16384;
    }
    
    return slot >= 0 ? slot : 16384 + slot;
};

function compareRanges(rangeA, rangeB) {
    return rangeA[0] - rangeB[0];
}

/**
 *
 * @constructor
 */
exports.ProxyList = function ProxyList() {

    var _ranges = [];
    var _map = new Map();

    /**
     * Returns number of proxies.
     *
     * @returns {Number}
     */
    this.size = function() {
        return _ranges.length;
    };

    /**
     *
     * @param {ShardProxy} proxy
     */
    this.add = function(proxy) {
        _ranges.push([proxy.start, proxy.end]);
        _ranges.sort(compareRanges);
        _map.set(proxy.start, proxy);
    };

    /**
     * Remove an instance from proxy list.
     * The removed instance will be closed.
     * 
     * @param {ShardProxy} proxy
     */
    this.remove = function(proxy) {

        if(!proxy) return;

        var newRanges = [];
        _ranges.map(function(range, index) {
            if(range[0] === proxy.start) {
                // remove from pointers
            } else {
                newRanges.push(range);
            }
        });
        newRanges.sort(compareRanges);
        _ranges = newRanges;

        _map.delete(proxy.start);
        proxy.close();
    };

    this.getFirst = function() {
        return _map.get(_ranges[0][0]);
    };

    this.getLast = function() {
        return _map.get(_ranges[_ranges.length - 1][0]);
    };

    this.getBySlot = function(slot) {
        for(var kv of _map) {
            if (kv[1].accept(slot)) {
                return kv[1];
            }
        }
        return null;
    };
    
    this.get4Endpoint = function(endpoint) {
        for(var kv of _map) {
            if (kv[1].getMaster().host == endpoint.host && kv[1].getMaster().port == endpoint.port) {
                return kv[1];
            }
        }
        
        return null;
    };

    /**
     *
     * @param handler (proxy, index)
     */
    this.map = function(handler) {
        _ranges.map(function(range, index) {
            handler(_map.get(range[0]), index);
        });
    };
};