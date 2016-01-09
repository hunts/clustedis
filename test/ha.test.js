'use strict';
/*global describe, it, before, beforeEach, after, afterEach */
/*jshint -W030 */

var expect = require('chai').expect;
var redis = require('../index');

var entryNodes = [
    {host: "127.0.0.1", port: 7000}, // One live node in redis cluster
    {host: "127.0.0.1", port: 7009}, // One non-existing node in redis cluster
];

describe('H/A tests: ', function() {

    var client;

    before(function(done) {
        
        client = redis.createClient(entryNodes,
            {
                debug_mode: false
            },
            1
        );
        
        client.on('ready', function() {
            process.nextTick(function() {
                client.set('a key', 'a value', done);
            });
        });
    });
    /*
    describe('server unavailable tests: ', function() {
        
        it('master segfault', function(done) {
            this.timeout(2000);
            client.getSlotClient(false, 'a key', function(err, slotClient) {
                slotClient.debug('SEGFAULT', function(err, res) {        
                    expect(err).to.be.an.instanceOf(Error);
                    client.set('a key', 'another value', function(err, res) {
                        expect(err).to.be.an.instanceOf(Error);
                        done();
                    });
                });
            });
        });
    
        it('read through slave', function(done) {
            client.get('a key', function(err, res) {
                expect(err).to.not.exist;
                expect(res).to.be.equal('a value');
                done();
            });
        });
        
        it('write data to new master', function(done) {
            this.timeout(5000);
            var time2Recover = 0;
            
            function finish(handle) {
                clearInterval(handle);
                done();
            }
            
            var handle = setInterval(function() {
                client.set('a key', 'another value', function(err, res) {
                    time2Recover += 300;
                    if (!err) {
                        expect(time2Recover).to.be.lessThan(1000);
                        finish(handle);
                    }
                });
            }, 300);
        });
    });
    */
    /**
     * cleanup connections.
     */
    after(function(done) {
        client.close(done);
    });
});