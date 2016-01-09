'use strict';
/*global describe, it, before, after, Buffer */
/*jshint -W030 */

var expect = require('chai').expect;
var consolelogger = require('../../lib/consolelogger');
var Shard = require('../../lib/shard');
var shardproxy = require('../../lib/shardproxy');

var crc16 = function(buf) {
    var slot = shardproxy.crc16(buf) % 16384;
    return slot >= 0 ? slot : 16384 + slot;
}

describe('lib/shardproxy.js', function() {
    
    describe('calculate slot tests:', function() {
        it('should hash entire string key', function(done) {
            var key = 'a string key';
            var slot = shardproxy.calculateSlot(key);
            expect(slot).to.be.equal(crc16(new Buffer(key)));
            done();
        });
        
        it('should hash hashtag in string', function(done) {
            var key = 'a str{tag}ing key';
            var slot = shardproxy.calculateSlot(key);
            expect(slot).to.be.equal(crc16(new Buffer('tag')));
            done();
        });
        
        it('should hash entire buffer key', function(done) {
            var buf = new Buffer([0x12, 0x13, 0x14]);
            var slot = shardproxy.calculateSlot(buf);
            expect(slot).to.be.equal(crc16(buf));
            done();
        });
        
        it('should hash hashtag in buffer', function(done) {
            var buf = new Buffer([0x12, , 0x7B, 0x13, 0x14, 0x7D, 0x15]);
            var slot = shardproxy.calculateSlot(buf);
            expect(slot).to.be.equal(crc16(new Buffer([0x13, 0x14])));
            done();
        });
    });
    
    describe('proxy object methods\' tests:', function() {
        var shard = new Shard(101, 200, {}, [{}]);
        var proxy;
        
        before(function(done) {
            proxy = shardproxy.create(shard, {}, 1, consolelogger);
            done();
        });
        
        it('should accept slot in range', function(done) {
            expect(proxy.accept(101)).to.be.true;
            expect(proxy.accept(150)).to.be.true;
            expect(proxy.accept(200)).to.be.true;
            done();
        });
        
        it('should not accept slot out of range', function(done) {
            expect(proxy.accept(0)).to.be.false;
            expect(proxy.accept(100)).to.be.false;
            expect(proxy.accept(201)).to.be.false;
            done();
        });
        
        after(function(done) {
            proxy.close(done);
        });
    });
});