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

describe('status', function () {

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

    it('works on freshly started server', function (done) {
        httplock.get_status({
            url: 'http://localhost:31419'
        }, function (err, stats) {
            assert.ifError(err);
            assert.ok(stats);
            assert.equal(typeof stats, 'object');
            assert.ok(stats.lowr);
            assert.equal(typeof stats.lowr, 'object');
            assert.equal(Object.keys(stats.lowr).length, 0);
            assert.ok(!isNaN(stats.uptime));
            assert.ok(stats.memory);
            assert.equal(typeof stats.memory, 'object');
            assert.ok(!isNaN(stats.memory.rss));
            assert.ok(!isNaN(stats.memory.heapTotal));
            assert.ok(!isNaN(stats.memory.heapUsed));
            assert.ok(stats.lowr_stats);
            assert.equal(typeof stats.lowr_stats, 'object');
            assert.equal(stats.lowr_stats.grant, 0);
            assert.equal(stats.lowr_stats.wait, 0);
            assert.equal(stats.lowr_stats.release, 0);
            assert.equal(stats.lowr_stats.renew, 0);
            assert.equal(stats.lowr_stats.expire, 0);
            done();
        });
    });

    it('works with one lock taken and released', function (done) {
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
                    ctx.release(null, cb);
                });
            },
            function (cb) {
                httplock.get_status({
                    url: 'http://localhost:31419'
                }, function (err, stats) {
                    assert.ifError(err);
                    assert.ok(stats);
                    assert.equal(typeof stats, 'object');
                    assert.ok(stats.lowr);
                    assert.equal(typeof stats.lowr, 'object');
                    assert.equal(Object.keys(stats.lowr).length, 0);
                    assert.ok(!isNaN(stats.uptime));
                    assert.ok(stats.memory);
                    assert.equal(typeof stats.memory, 'object');
                    assert.ok(!isNaN(stats.memory.rss));
                    assert.ok(!isNaN(stats.memory.heapTotal));
                    assert.ok(!isNaN(stats.memory.heapUsed));
                    assert.ok(stats.lowr_stats);
                    assert.equal(typeof stats.lowr_stats, 'object');
                    assert.equal(stats.lowr_stats.grant, 1);
                    assert.equal(stats.lowr_stats.wait, 0);
                    assert.equal(stats.lowr_stats.release, 1);
                    assert.equal(stats.lowr_stats.renew, 0);
                    assert.equal(stats.lowr_stats.expire, 0);
                    cb();
                });
            }
        ], done);
    });

    it('works with one lock taken and 1 waiting', function (done) {
        var ctx;
        async.series([
            function (cb) {
                httplock.take_lowr({
                    url: 'http://localhost:31419',
                    name: 'my_lock',
                    logger: no_log
                }, function (err, c) {
                    ctx = c;
                    assert.ifError(err);
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
                });
                setTimeout(cb, 100);
            },
            function (cb) {
                httplock.get_status({
                    url: 'http://localhost:31419'
                }, function (err, stats) {
                    assert.ifError(err);
                    assert.ok(stats);
                    assert.equal(typeof stats, 'object');
                    assert.ok(stats.lowr);
                    assert.equal(typeof stats.lowr, 'object');
                    assert.equal(Object.keys(stats.lowr).length, 1);
                    assert.ok(stats.lowr.my_lock);
                    assert.equal(typeof stats.lowr.my_lock, 'object');
                    assert.ok(!isNaN(stats.lowr.my_lock.created));
                    assert.ok(!isNaN(stats.lowr.my_lock.age));
                    assert.equal(stats.lowr.my_lock.wait_queue, 1);
                    assert.equal(stats.lowr.my_lock.renew_count, 0);
                    assert.equal(typeof stats.lowr.my_lock.owner, 'string');
                    assert.ok(!isNaN(stats.uptime));
                    assert.ok(stats.memory);
                    assert.equal(typeof stats.memory, 'object');
                    assert.ok(!isNaN(stats.memory.rss));
                    assert.ok(!isNaN(stats.memory.heapTotal));
                    assert.ok(!isNaN(stats.memory.heapUsed));
                    assert.ok(stats.lowr_stats);
                    assert.equal(typeof stats.lowr_stats, 'object');
                    assert.equal(stats.lowr_stats.grant, 1);
                    assert.equal(stats.lowr_stats.wait, 1);
                    assert.equal(stats.lowr_stats.release, 0);
                    assert.equal(stats.lowr_stats.renew, 0);
                    assert.equal(stats.lowr_stats.expire, 0);
                    cb();
                });
            }
        ], function (error) {
            if (ctx && ctx.release) {
                ctx.release(null, function () {
                    done(error);
                });
            }
            else
                done(error);
        });
    });


});


describe('lowr', function () {

    var server;
    beforeEach(function (done) {
        server = httplock.create_server({
            port: 31419,
            logger: no_log,
            ttl: 1000
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

    it('successfuly takes a lock with 1 client and releases after renewal', function (done) {
        httplock.take_lowr({
            url: 'http://localhost:31419',
            name: 'my_lock',
            logger: no_log
        }, function (err, ctx) {
            assert.ifError(err);
            assert.ok(ctx);
            assert.equal(typeof ctx.release, 'function');
            setTimeout(function () {
                ctx.release(null, done);
            }, 1500);
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

    it('successfuly waits on a lock taken, renewed, and released by another client', function (done) {
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
                    }, 1500)
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
