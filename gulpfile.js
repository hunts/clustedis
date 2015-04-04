"use strict";

var gulp = require('gulp'),
    jshint = require('gulp-jshint'),
    mocha = require('gulp-mocha'),
    gulpSequence = require('gulp-sequence');

gulp.task('jshint', function () {
    return gulp.src(['*.js', 'lib/*.js', 'test/*.js'])
        .pipe(jshint())
        .pipe(jshint.reporter('default'));
});

gulp.task('mocha', function () {
    return gulp.src('test/index.js', {read: false})
        .pipe(mocha({
            timeout: 8000
        }));
});

gulp.task('test', gulpSequence('jshint', 'mocha'));