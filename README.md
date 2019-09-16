# racket-redis

Fast, idiomatic redis bindings for Racket.

## Installation

    $ raco pkg install redis-rkt  # NOT "redis", that's a different, older package!
    $ raco doc redis

## Basic Usage

```racket
(require redis)

(define c (make-redis))
(redis-bytes-set! c "some-key" "hello, world!")
(redis-bytes-get c "some-key")  ;; => #"hello, world!"
(redis-bytes-get c "some-key" "some-other-key")  ;; => '(#"hello, world!" #f)

;; or, with a connection pool:

(define pool (make-redis-pool))
(call-with-redis-client pool
  (lambda (c)
    (redis-bytes-set! c "some-key" "hello, world!")
    (redis-bytes-get c "some-key")))

;; or, using the simplified API:

(require redis/easy)

(current-redis-pool (make-redis-pool))
(redis-bytes-set! "some-key" "hello, world!")
(redis-bytes-get "some-key")
```

## Missing commands

The commands below are yet to be implemented:


### Bytestrings

* [ ]  BITFIELD
* [ ]  BITPOS
* [ ]  GETBIT
* [ ]  GETRANGE
* [ ]  GETSET
* [ ]  MSET
* [ ]  MSETNX
* [ ]  PSETEX
* [ ]  SETBIT
* [ ]  SETEX
* [ ]  SETNX
* [ ]  SETRANGE
* [ ]  STRLEN

### Geo

* [ ]  GEORADIUS
* [ ]  GEORADIUSBYMEMBER

### Hashes

* [ ]  HSETNX

### Lists

* [ ]  LPUSHX
* [ ]  RPUSHX
* [ ]  SORT

### Sets

* [ ]  SRANDMEMBER
* [ ]  SSCAN

### Sorted Sets

* [ ]  BZPOPMAX
* [ ]  BZPOPMIN
* [ ]  ZADD
* [ ]  ZCARD
* [ ]  ZCOUNT
* [ ]  ZINCRBY
* [ ]  ZINTERSTORE
* [ ]  ZLEXCOUNT
* [ ]  ZPOPMAX
* [ ]  ZPOPMIN
* [ ]  ZRANGE
* [ ]  ZRANGEBYLEX
* [ ]  ZRANGEBYSCORE
* [ ]  ZRANK
* [ ]  ZREM
* [ ]  ZREMRANGEBYLEX
* [ ]  ZREMRANGEBYRANK
* [ ]  ZREMRANGEBYSCORE
* [ ]  ZREVRANGE
* [ ]  ZREVRANGEBYLEX
* [ ]  ZREVRANGEBYSCORE
* [ ]  ZREVRANK
* [ ]  ZSCAN
* [ ]  ZSCORE
* [ ]  ZUNIONSTORE

## Acknowledgements

This project was originally based on [rackdis], but has since been
rewritten from the ground up for increased performance and safety.

[rackdis]: https://github.com/eu90h/rackdis
