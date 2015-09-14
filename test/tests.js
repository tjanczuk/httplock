var assert = require('assert')
    , httplock = require('../')
    , http = require('http')
    , async = require('async');

http.globalAgent.maxSockets = 20000;

var no_log = {
    info: function () {},
    warn: function () {},
    error: function () {},
};

describe('APIs', function () {

    it('are present', function () {
        assert.equal(typeof httplock.create_server, 'function');
        assert.equal(typeof httplock.take_lowr, 'function');
    });

});

describe('server', function () {

    it('fails to start without port', function (done) {
        try {
            httplock.create_server({}, function (error) {
                done(new Error('unexpected callback'));
            });
        }
        catch (e) {       
            assert.ok(e.message.match(/options\.port must be provided/));
            return done();
        }
        done(new Error('unexpected success'));
    });


    it('starts with port', function (done) {
        var server = httplock.create_server({
            port: 31419,
            logger: no_log
        }, function (error) {
            assert.equal(typeof server.close, 'function');
            server.close();
            assert.ifError(error);
            done();
        });
    });

});

describe('lowr', function () {

    var server;
    beforeEach(function (done) {
        server = httplock.create_server({
            port: 31419,
            logger: no_log
        }, done);
    });

    afterEach(function (done) {
        server.close(function () { done(); });
    });

    it('successfuly takes a lock with 1 client and releases immediately', function (done) {
        httplock.take_lowr({
            url: 'http://localhost:31419',
            name: 'my_lock',
            logger: no_log
        }, function (err, ctx) {
            assert.ifError(err);
            assert.ok(ctx);
            assert.equal(typeof ctx.release, 'function');
            ctx.release(null, done);
        });
    });

    it('successfuly waits on a lock taken and released by another client', function (done) {
        async.series([
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, ctx) {
                    assert.ifError(err);
                    assert.ok(ctx);
                    assert.equal(typeof ctx.release, 'function');
                    setTimeout(function () {
                        ctx.release({ some: 'data' }, function (err) {
                            assert.ifError(err);
                        });
                    }, 500)
                    cb();
                });
            },
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, ctx) {
                    assert.ifError(err);
                    assert.ok(ctx);
                    assert.ok(ctx.data);
                    assert.equal(typeof ctx.data, 'object');
                    assert.equal(ctx.data.some, 'data');
                    cb();
                });                
            }
        ], done);
    });

    it('successfully takes one lock while another is taken', function (done) {
        async.each(['foo', 'bar', 'baz'], function (lock_name, cb) {
            httplock.take_lowr({
                url: 'http://localhost:31419',
                name: lock_name,
                logger: no_log
            }, function (err, ctx) {
                assert.ifError(err);
                assert.ok(ctx);
                assert.equal(typeof ctx.release, 'function');
                cb();
            });
        }, done);
    });

    it('successfully releases 10 clients waiting on a lock', function (done) {
        async.series([
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, ctx) {
                    assert.ifError(err);
                    assert.ok(ctx);
                    assert.equal(typeof ctx.release, 'function');
                    setTimeout(function () {
                        ctx.release({ some: 'data' }, function (err) {
                            assert.ifError(err);
                        });
                    }, 500)
                    cb();
                });
            },
            function (cb) {
                async.each([1,2,3,4,5,6,7,8,9,10], function (lock_name, cb) {
                    httplock.take_lowr({
                        url: 'http://localhost:31419',
                        name: 'my_lock',
                        logger: no_log
                    }, function (err, ctx) {
                        assert.ifError(err);
                        assert.ok(ctx);
                        assert.ok(ctx.data);
                        assert.equal(typeof ctx.data, 'object');
                        assert.equal(ctx.data.some, 'data');
                        cb();
                    });
                }, cb);
            }
        ], done);
    });

    it('successfuly times out waiting for lock', function (done) {
        async.series([
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, ctx) {
                    assert.ifError(err);
                    assert.ok(ctx);
                    assert.equal(typeof ctx.release, 'function');
                    cb();
                });
            },
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    ttl: 200,
                    logger: no_log
                }, function (err, ctx) {
                    assert.ok(err);
                    assert.equal(err.code, 'ETIMEDOUT');
                    cb();
                });                
            }
        ], done);
    });

});

describe('mini stress', function () {

    var server;
    beforeEach(function (done) {
        server = httplock.create_server({
            port: 31419,
            logger: no_log
        }, done);
    });

    afterEach(function (done) {
        server.close(function () { done(); });
    });

    it('successfuly takes and releases 100 different locks', function (done) {
        var count = 0;
        var max = 100;
        for (var i = 0; i < max; i++) {
            httplock.take_lowr({
                url: 'http://localhost:31419',
                name: 'my_lock' + i,
                logger: no_log
            }, function (err, ctx) {
                assert.ifError(err);
                assert.ok(ctx);
                assert.equal(typeof ctx.release, 'function');
                ctx.release(null, function (err1) {
                    assert.ifError(err1);
                    if (++count === max) done();
                });
            });
        }
    });

    it('successfully releases 100 clients waiting on the same lock', function (done) {
        async.series([
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, ctx) {
                    assert.ifError(err);
                    assert.ok(ctx);
                    assert.equal(typeof ctx.release, 'function');
                    setTimeout(function () {
                        ctx.release({ some: 'data' }, function (err) {
                            assert.ifError(err);
                        });
                    }, 500)
                    cb();
                });
            },
            function (cb) {
                // return cb();
                var count = 0;
                var max = 100;
                for (var i = 0; i < max; i++) {
                    httplock.take_lowr({
                        url: 'http://localhost:31419',
                        name: 'my_lock',
                        logger: no_log
                    }, function (err, ctx) {
                        assert.ifError(err);
                        assert.ok(ctx);
                        assert.ok(ctx.data);
                        assert.equal(typeof ctx.data, 'object');
                        assert.equal(ctx.data.some, 'data');
                        if (++count === max) cb();
                    });
                }
            }
        ], done);
    });

});
