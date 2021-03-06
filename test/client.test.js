'use strict';
/*global describe, it, before, beforeEach, after, afterEach */
/*jshint -W030 */
/*jshint loopfunc:true */

var expect = require('chai').expect;
var redis = require('../index');

var entryNodes = [
    {host: "127.0.0.1", port: 7000}, // One live node in redis cluster
    {host: "127.0.0.1", port: 7009}, // One non-existing node in redis cluster
];

describe('create client in different ways: ', function() {

    it('single live node', function (done) {
        var client = redis.createClient('127.0.0.1', 7000);
        client.on('ready', function () {
            client.close(done);
        });
    });

    it('multi nodes array-array', function (done) {
        var client = redis.createClient([['127.0.0.1', 7000], ['127.0.0.1', 7009]]);
        client.on('ready', function () {
            client.close(done);
        });
    });

    it('multi nodes object-array', function (done) {
        var client = redis.createClient([{host: '127.0.0.1', port: 7000}, {host: '127.0.0.1', port: 7009}]);
        client.on('ready', function () {
            client.close(done);
        });
    });

    it('multi nodes string-array', function (done) {
        var client = redis.createClient(['127.0.0.1:7000', '127.0.0.1:7001', '127.0.0.1:7009']);
        client.on('ready', function () {
            client.close(done);
        });
    });
});

describe('cluster command tests: ', function() {

    var client;

    before(function(done) {
        client = redis.createClient(entryNodes,
            {
                debug_mode: false
            },
            1
        );
        
        client.on('ready', function() {
            done();
        });
    });
    
    describe('client helper functions', function() {
        it('should get key table from args', function(done) {
            var kt = client.findKeyTable('mset', ['key1', 'value1', 'key2', 'value2']);
            expect(kt.size).to.be.equal(2);
            expect(kt.get(0)).to.be.equal('key1');
            expect(kt.get(2)).to.be.equal('key2');
            done();
        });
    });

    describe('redis server info', function() {

        it('should get redis info', function(done) {
            client.info(function(err, res) {
                expect(res).to.have.string('redis_version:');
                done();
            });
        });

        it('should get redis cluster info', function(done) {
            client.cluster('info', function(err, res) {
                expect(res).to.have.string('cluster_state:ok');
                done();
            });
        });
    });
    
    describe('check whether a command is readonly or not', function() {
        it('should get correct command type', function(done) {
            for(var cmd in client.commands) {
                if(client.commands[cmd][1][0] == 'readonly') {
                    expect(client.isReadOnly(cmd)).to.be.true;
                } else {
                    expect(client.isReadOnly(cmd)).to.be.false;
                }
            }
            done();
        });
    });

    describe('commands: get / set / del', function() {

        it('should returns null for non-exists key', function(done) {
            client.get('non-exists-key', function(err, res) {
                expect(res).to.be.null;
                done();
            });
        });

        it('should set a single key-value success', function(done) {
            client.set('key1', 'value1', function(err, res) {
                expect(res).to.be.equal('OK');
                done();
            });
        });

        it('should get a key success', function(done) {
            client.get('key1', function(err, res) {
                expect(res).to.be.equal('value1');
                done();
            });
        });

        it('should del a key success', function(done) {
            client.del('key1', function(err, res) {
                expect(res).to.be.equal(1);
                done();
            });
        });
    });

    describe('commands: mget / mset', function() {

        it('should mset multi keys in different slots failed', function (done) {
            client.mset('key1', 'value1', 'key2', 'value2', function (err, res) {
                expect(err).to.be.an.instanceOf(Error);
                expect(res).to.not.exist;
                done();
            });
        });

        it('should mset multi key-value in same slots success', function (done) {
            client.mset('{1}key1', 'value1', '{1}key2', 'value2', function (err, res) {
                expect(res).to.be.equal('OK');
                done();
            });
        });

        it('should mget keys in same slots success', function (done) {
            client.mget('{1}key1', '{1}key2', function (err, res) {
                expect(err).to.not.exist;
                expect(res[0]).to.be.equal('value1');
                expect(res[1]).to.be.equal('value2');
                done();
            });
        });

        it('should del keys with hash tag success', function (done) {
            client.del('{1}key1', function (err, res) {
                expect(res).to.be.equal(1);
                client.del('{1}key2', function (err, res) {
                    expect(res).to.be.equal(1);
                    done();
                });
            });
        });

        it('should key "key_redirection" cause redirection but finally success', function (done) {
            var steps = 2;
            var key = 'key_redirection';
            client.keySlotMap.set(key, 0);
            client.once('redirection', function(err) {
                client.keySlotMap.delete(key);
                expect(err.message).to.have.string('MOVED');
                steps -= 1;
            });
            client.get(key, function (err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.null;
                steps -= 1;
                if(steps === 0) {
                    done();
                }
            });
        });
    });
    
    describe('command: publish & subscribe', function() {

        it('should publish success', function(done) {
            client.publish('test topic', 'test message', function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal(0);
                done();
            });
        });

        it('should subscribe success', function(done) {
            var topic = 'test topic';
            var msg = 'test message';
            
            client.subscribe(topic, function(err, res) {
                expect(res[0]).to.be.equal('subscribe');
                expect(res[1]).to.be.equal(topic);
                expect(res[2]).to.be.equal(1);
            });
            
            client.on('message', function(channel, message) {
                expect(channel).to.be.equal(topic);
                expect(message).to.be.equal(msg);
                client.unsubscribe(topic, function(err, res) {
                    expect(res).to.be.equal(topic);
                    done();
                });
            });
            
            setTimeout(function() {
                client.publish(topic, msg);
            }, 30);
        });
    });
    
    describe('command: trasactions', function() {

        it('should explicit exec works', function(done) {
            client.multi()
                .set('key1','value1')
                .set('key2','value2')
                .set('key3','value3')
                .get('key1')
                .get('key2')
                .get('key3')
                .exec(function(err, replies) {
                    expect(err).to.not.exist;
                    expect(replies[0]).to.be.equal('OK');
                    expect(replies[1]).to.be.equal('OK');
                    expect(replies[2]).to.be.equal('OK');
                    expect(replies[3]).to.be.equal('value1');
                    expect(replies[4]).to.be.equal('value2');
                    expect(replies[5]).to.be.equal('value3');
                    done();
                });
        });
        
        it('should implicit exec works', function(done) {
            client.multi(function(m) {
                m.set('key1','value1')
                .set('key2','value2')
                .set('key3','value3')
                .get('key1')
                .get('key2')
                .get('key3', function(err, res) {
                    done();
                });
            });
        });
    });
    
    describe('command: pipelining', function() {

        it('should implicit exec works', function(done) {
            
            var numberOfCallbacks = 4;
            var ensureDone = function() {
                numberOfCallbacks -= 1;
                if (numberOfCallbacks === 0) {
                    done();
                }
            };
            
            client.pipelined(function(pipeline) {
                
                pipeline.ping(function(err, res) {
                    expect(err).to.not.exist;
                    expect(res).to.be.equal('PONG');
                    ensureDone();
                })
                .ping()
                .ping(function(err, res) {
                    expect(err).to.not.exist;
                    expect(res).to.be.equal('PONG');
                    ensureDone();
                });
                
                pipeline.set('key1', 'value1', function(err, res) {
                    expect(err).to.not.exist;
                    expect(res).to.be.equal('OK');
                    ensureDone();
                });
                
                pipeline.set('key2', 'value2', function(err, res) {
                    expect(err).to.not.exist;
                    expect(res).to.be.equal('OK');
                    ensureDone();
                });
                
                // no callback
                pipeline.set('key3', 'value3');
            });
        });
        
        it('should explicit exec works', function(done) {
            
            var pipeline = client.pipelined();
                
            pipeline.ping(function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal('PONG');
            })
            .ping()
            .ping(function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal('PONG');
            });
            
            pipeline.set('key1', 'value1', function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal('OK');
            });
            
            pipeline.set('key2', 'value2', function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal('OK');
            });
            
            // no callback
            pipeline.set('key3', 'value3');
            
            pipeline.exec(function(err, replies) {
                expect(err).to.not.exist;
                expect(replies[0]).to.be.equal('PONG');
                expect(replies[1]).to.be.equal('PONG');
                expect(replies[2]).to.be.equal('PONG');
                expect(replies[3]).to.be.equal('OK');
                expect(replies[4]).to.be.equal('OK');
                expect(replies[5]).to.be.equal('OK');
                done();
            });
        });
    });

    /**
     * cleanup connections.
     */
    after(function(done) {
        client.close(done);
    });
});