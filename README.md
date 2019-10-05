# racket-redis

[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FBogdanp%2Fracket-redis%2Fbadge&style=flat)](https://actions-badge.atrox.dev/Bogdanp/racket-redis/goto)

Fast, idiomatic redis bindings for Racket.

## Installation

    $ raco pkg install redis-rkt  # NOT "redis", that's a different, older package!
    $ raco doc redis

## Usage

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

;; or, using the "easy" API:

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
* [ ]  GETSET
* [ ]  MSET
* [ ]  MSETNX
* [ ]  PSETEX
* [ ]  SETBIT
* [ ]  SETEX
* [ ]  SETNX
* [ ]  SETRANGE

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

* [ ]  SSCAN

### Sorted Sets

* [ ]  ZREMRANGEBYLEX
* [ ]  ZREMRANGEBYRANK
* [ ]  ZREMRANGEBYSCORE
* [ ]  ZSCAN

## Acknowledgements

This project was originally based on [rackdis], but has since been
rewritten from the ground up for increased performance and safety.

[rackdis]: https://github.com/eu90h/rackdis
