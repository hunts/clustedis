'use strict';
/*global describe, it, before, beforeEach, after, afterEach */
/*jshint -W030 */

var expect = require('chai').expect;
var redis = require('../index');
var bareRedis = require('redis');

describe('Cluster Client Tests', function() {

    var client;

    before(function(done) {
        client = redis.createClient('192.168.139.132', 30001, {
            debug_mode: false
        });

        client.on('ready', function() {
            done();
        });
    });

    describe('bare node redis', function() {
        it('MOVED Redirection', function() {
            var redis = bareRedis.createClient(30001, '192.168.139.132');
            redis.on('ready', function() {
                redis.get('key-redirection', function(err) {
                    expect(err).is.exist;
                    expect(err.message).to.have.string('MOVED');
                });
            });
        });
    });

    describe('redis server info', function() {

        it('should get redis info', function(done) {
            client.info(function(err, res) {
                expect(res).to.have.string('redis_version:3');
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
            client.keySlotMap['key_redirection'] = 0;
            client.once('redirection', function(err) {
                delete client.keySlotMap['key_redirection'];
                expect(err.message).to.have.string('MOVED');
                steps -= 1;
                if(steps === 0) {
                    done();
                }
            });
            client.get('key_redirection', function (err, res) {
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
                expect(res).to.be.equal(1);
                done();
            });
        });

        it('should subscribe success', function(done) {
            var channel = 'test topic';
            client.subscribe(channel, function(message) {
                expect(message).to.be.equal('test message');
                client.unsubscribe(channel, done);
            });
            client.publish(channel, 'test message');
        });
    });

    after(function(done) {
        client.close(done);
    });
});