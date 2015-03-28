'use strict';

var expect = require('chai').expect;
var redis = require('../index');

describe('Cluster Client Tests', function() {

    var client;

    before(function(done) {
        client = redis.createClient('10.16.40.54', 7000, {
            debugMode: true
        });
        done();
    });

    describe('initial connection', function() {
        it('should acquire correctly client', function(done) {
            client.get()
            done();
        });

        it('should acquire client after wait resource', function(done) {
             done();
        });
    });
});