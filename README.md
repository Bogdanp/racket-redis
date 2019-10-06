# racket-redis

[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FBogdanp%2Fracket-redis%2Fbadge&style=flat)](https://actions-badge.atrox.dev/Bogdanp/racket-redis/goto)

Fast, idiomatic redis bindings for Racket.

## Installation

    $ raco pkg install redis-rkt  # NOT "redis".  That's a different package!
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

Additional documentation is available on [docs.racket-lang.org][docs].

## Missing commands

The commands below are yet to be implemented.  If you need any of
these, or others not listed here, then feel free to get the ball
rolling by creating a PR.

### Bytestrings

* [ ] `BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]`
* [ ] `BITPOS key bit [start] [end]`
* [ ] `GETSET key value`
* [ ] `MSET key value [key value ...]`
* [ ] `MSETNX key value [key value ...]`

### Cluster

* [ ] `CLUSTER ADDSLOTS slot [slot ...]`
* [ ] `CLUSTER COUNT-FAILURE-REPORTS node-id`
* [ ] `CLUSTER COUNTKEYSINSLOT slot`
* [ ] `CLUSTER DELSLOTS slot [slot ...]`
* [ ] `CLUSTER FAILOVER [FORCE|TAKEOVER]`
* [ ] `CLUSTER FORGET node-id`
* [ ] `CLUSTER GETKEYSINSLOT slot count`
* [ ] `CLUSTER INFO`
* [ ] `CLUSTER KEYSLOT key`
* [ ] `CLUSTER MEET ip port`
* [ ] `CLUSTER NODES`
* [ ] `CLUSTER REPLICATE node-id`
* [ ] `CLUSTER RESET [HARD|SOFT]`
* [ ] `CLUSTER SAVECONFIG`
* [ ] `CLUSTER SET-CONFIG-EPOCH config-epoch`
* [ ] `CLUSTER SETSLOT slot IMPORTING|MIGRATING|STABLE|NODE [node-id]`
* [ ] `CLUSTER SLAVES node-id`
* [ ] `CLUSTER REPLICAS node-id`
* [ ] `CLUSTER SLOTS`
* [ ] `READONLY`
* [ ] `READWRITE`

### Geo

* [ ] `GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]`
* [ ] `GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]`

### Hashes

* [ ] `HSETNX key field value`

### Keys

* [ ] `MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [KEYS key [key ...]]`
* [ ] `OBJECT subcommand [arguments [arguments ...]]`
* [ ] `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]`

### Lists

* [ ] `LPUSHX key value`
* [ ] `RPUSHX key value`
* [ ] `SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]`

### PubSub

* [ ] `PUBSUB subcommand [argument [argument ...]]`

### Script

* [ ] `SCRIPT DEBUG YES|SYNC|NO`

### Server

* [ ] `CLIENT KILL [ip:port] [ID client-id] [TYPE normal|master|slave|pubsub] [ADDR ip:port] [SKIPME yes/no]`
* [ ] `CLIENT LIST [TYPE normal|master|replica|pubsub]`
* [ ] `CLIENT REPLY ON|OFF|SKIP`
* [ ] `CLIENT UNBLOCK client-id [TIMEOUT|ERROR]`
* [ ] `COMMAND GETKEYS`
* [ ] `COMMAND INFO command-name [command-name ...]`
* [ ] `DEBUG OBJECT key`
* [ ] `DEBUG SEGFAULT`
* [ ] `MEMORY DOCTOR`
* [ ] `MEMORY HELP`
* [ ] `MEMORY MALLOC-STATS`
* [ ] `MEMORY PURGE`
* [ ] `MEMORY STATS`
* [ ] `MEMORY USAGE key [SAMPLES count]`
* [ ] `MONITOR`


## Acknowledgements

This project was originally based on [rackdis], but has since been
rewritten from the ground up for increased performance and safety.


[docs]: https://docs.racket-lang.org/redis@redis-doc/index.html
[rackdis]: https://github.com/eu90h/rackdis
