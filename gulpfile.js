'use strict';

var gulp = require('gulp');
var jshint = require('gulp-jshint');
var mocha = require('gulp-mocha');
var gulpSequence = require('gulp-sequence');

gulp.task('jshint', function () {
    return gulp.src(['*.js', 'lib/*.js', 'test/*.js'])
        .pipe(jshint())
        .pipe(jshint.reporter('default'));
});

gulp.task('client', function () {
    return gulp.src('test/client.test.js', {read: false})
        .pipe(mocha({
            timeout: 8000,
        }));
});

gulp.task('ha', function () {
    return gulp.src('test/ha.test.js', {read: false})
        .pipe(mocha({
            timeout: 8000,
        }));
});

gulp.task('test', gulpSequence('jshint', 'client', 'ha'));