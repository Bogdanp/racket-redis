#lang scribble/manual

@(require (for-label racket/base
                     racket/contract
                     racket/dict
                     racket/sequence
                     racket/serialize
                     racket/string
                     redis
                     (only-in redis/easy
                              current-redis-client
                              current-redis-pool))
          scribble/example
          "redis.rkt")

@title{@exec{redis}: bindings for Redis}
@author[(author+email "Bogdan Popa" "bogdan@defn.io")]
@defmodule[redis]

This package provides up-to-date bindings to the Redis database that
are idiomatic and fast.  Every exposed function has a contract (some
are even quite complex!) and the library is about as fast as hiredis
(written in C) + redis-py.

Here is a microbenchmark in Python:

@verbatim{
$ cat <<EOF >test.py
import redis
import timeit

c = redis.Redis()

print(timeit.timeit("c.set('a', '1')", number=10000, globals=globals()))
EOF

$ python test.py
0.5442340160000001
}

and the equivalent benchmark in Racket CS:

@verbatim{
$ cat <<EOF >test.rkt
#lang racket/base

(require redis)

(define c (make-redis))

(time
 (for ([_ (in-range 10000)])
   (redis-bytes-set! c "a" "1")))
EOF

$ racket test.rkt
cpu time: 367 real time: 565 gc time: 21
}

Obviously, real world use cases will have different characteristics,
but the point is that the library won't get in your way.

The functions in this package are named differently from their Redis
counterparts to avoid confusion as much as possible.  The rule is that
when a function name is ambiguous with regards to the type of value it
operates on, then it must contain the type in its name.

For example, rather than exposing a function called @exec{redis-get}
for looking up keys, we expose @racket[redis-bytes-get] so that it is
clear to the user that they're about to receive one or more byte
strings.  On the other hand, @racket[redis-rename!] doesn't need to be
prefixed, because the operation can only refer to renaming a key.

If you're looking to run a particular command but are not sure what
the associated function's name is, simply search this documentation
for that command.  The documentation for each function names the
commands said function relies on.


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "client"]{Clients}

Each client represents a single TCP connection to the Redis server.

@defproc[(parse-redis-url [s string?]) (values (or/c #f string?)
                                               (or/c #f string?)
                                               string?
                                               (integer-in 0 65535)
                                               (integer-in 0 16))]{

  Parses a Redis connection URL into 5 values:

  @itemlist[
    @item{an optional username (defaults to @racket[#f]),}
    @item{an optional password (defaults to @racket[#f]),}
    @item{a host name,}
    @item{an optional port (defaults to @racket[6379]) and}
    @item{an optional database number (defaults to @racket[0]).}
  ]

  @examples[
  (require redis)
  (parse-redis-url "redis://127.0.0.1")
  (parse-redis-url "redis://127.0.0.1/1")
  (parse-redis-url "redis://bogdan@127.0.0.1/1")
  (parse-redis-url "redis://bogdan:pass@127.0.0.1/1")
  ]
}


@defproc[(make-redis [#:client-name client-name (or/c #f string?) "racket-redis"]
                     [#:unix-socket socket-path (or/c #f path-string?) #f]
                     [#:host host string? "127.0.0.1"]
                     [#:port port (integer-in 0 65536) 6379]
                     [#:timeout timeout exact-nonnegative-integer? 5]
                     [#:db db (integer-in 0 16) 0]
                     [#:username username (or/c #f non-empty-string?) #f]
                     [#:password password (or/c #f non-empty-string?) #f]) redis?]{

  Creates a Redis client that lazily connects to to the database
  at @racket[#:host] and @racket[#:port] or the UNIX socket at
  @racket[#:unix-socket]. If a UNIX socket is provided, it takes
  precedence over the @racket[#:host] and @racket[#:port] arguments.

  The @racket[#:timeout] parameter controls the maximum amount of time
  (in milliseconds) the client waits for a response to a command from
  the database.

  If the @racket[#:username] argument is provided, then Redis 6.0 is
  assumed and an @tt{AUTH username password} command will be sent to the
  server after a connection is established.

  If the @racket[#:password] argument is provided without a
  @racket[#:username], then an @tt{AUTH password} command is emitted.
  This is compatible with all versions of Redis since 1.0.

  Each client maps to an individual connection, therefore clients
  @emph{are not} thread safe! See @secref["pooling"].

  @history[#:changed "1.0" @elem{Added support for @racket[#:unix-socket] connections.}]
  @history[#:changed "1.2" @elem{The @racket[#:client-name] argument accepts @racket[#f].}]
}

@defproc[(redis? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a Redis client.
}

@defproc[(redis-connected? [client redis?]) boolean?]{
  Returns @racket[#t] when @racket[client] appears to be connected to
  the database.  Does not detect broken pipes.
}

@defproc[(redis-connect! [client redis?]) void?]{
  Initiates a connection to the database.  If one is already open,
  then the client is first disconnected before the new connection is
  made.
}

@defproc[(redis-disconnect! [client redis?]) void?]{
  Disconnects from the server immediately and without sending a
  @exec{QUIT} command.  Does nothing if the client is already
  disconnected.
}

@defthing[redis-key/c (or/c bytes? string?)]{
  The contract for valid Redis keys.
}

@defthing[redis-key-type/c (or/c 'string 'list 'set 'zset 'hash 'stream)]{
  The contract for valid Redis key types.
}

@defthing[redis-string/c (or/c bytes? string?)]{
  The contract for valid Redis byte strings.  Anywhere you see this
  contract, keep in mind that any string value you provide will be
  converted to @racket[bytes?] via @racket[string->bytes/utf-8].
}

@defthing[
  redis-value/c (or/c #f bytes? string? exact-integer? (listof redis-value/c))]{

  The contract for Redis response values.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "pooling"]{Connection Pooling}

@defproc[(redis-pool? [v any/c]) boolean?]{
  Returns @racket[#t] if @racket[v] is a pool of Redis connections.
}

@defproc[(make-redis-pool [#:client-name client-name (or/c #f non-empty-string?) "racket-redis"]
                          [#:unix-socket socket-path (or/c #f path-string?) #f]
                          [#:host host non-empty-string? "127.0.0.1"]
                          [#:port port (integer-in 0 65536) 6379]
                          [#:timeout timeout exact-nonnegative-integer? 5000]
                          [#:db db (integer-in 0 15) 0]
                          [#:username username (or/c #f non-empty-string?) #f]
                          [#:password password (or/c #f non-empty-string?) #f]
                          [#:pool-size pool-size exact-positive-integer? 4]
                          [#:idle-ttl idle-ttl exact-nonnegative-integer? 3600000]) redis-pool?]{

  Creates a lazy pool of Redis connections that will contain at most
  @racket[pool-size] connections. Idle connections are closed after
  @racket[idle-ttl].

  All other parameters are passed directly to @racket[make-redis]
  whenever a new connection is initiated.
}

@defproc[(redis-pool-shutdown! [pool redis-pool?]) void?]{

  Shuts down @racket[pool] and closes any of its open connections.
  Once shut down, a pool can no longer be used.
}

@defproc[(call-with-redis-client [pool redis-pool?]
                                 [proc (-> redis? any)]
                                 [#:timeout timeout (or/c #f exact-nonnegative-integer?) #f]) any]{

  Grabs a connection from the @racket[pool] and calls @racket[proc]
  with it, ensuring that the connection is returned to the pool upon
  completion.

  Holding on to a connection past the execution of @racket[proc] is a
  bad idea so don't do it.

  This function blocks until either a connection becomes available or
  the @racket[timeout] (milliseconds) is reached.  If @racket[timeout]
  is @racket[#f], then the function will block indefinitely.  Upon
  timeout an @racket[exn:fail:redis:pool:timeout] exception is raised.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "exceptions"]{Exceptions}

@deftogether[
  (@defstruct[(exn:fail:redis exn:fail) ()]
   @defstruct[(exn:fail:redis:timeout exn:fail:redis) ()]
   @defstruct[(exn:fail:redis:type exn:fail:redis) ()]
   @defstruct[(exn:fail:redis:script exn:fail:redis) ()]
   @defstruct[(exn:fail:redis:script:missing exn:fail:redis) ()]
   @defstruct[(exn:fail:redis:pool exn:fail:redis) ()]
   @defstruct[(exn:fail:redis:pool:timeout exn:fail:redis:pool) ()])]{

  The various Redis-related exceptions that functions in this package
  can raise.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "transactions"]{Transactions}

You may notice that transaction-related commands are conspicuously
missing from this library.  That's intentional!  Redis transactions
were created before the introduction of lua scripting and the
guarantees they offer are weaker than the guarantees offered by script
execution, therefore I have decided to forego implementing the
transaction commands for the time being.

Check out @secref["scripts"] for a nice bridge between the lua
scripting world and Racket.


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "scripts"]{Scripts}

@defthing[redis-script/c (->* (redis?)
                              (#:keys (listof redis-key/c)
                               #:args (listof redis-string/c))
                              redis-value/c)]{

  The contract for lua-backed Redis scripts.
}

@defproc[(make-redis-script [client redis?]
                            [lua-script redis-string/c]) redis-script/c]{

  Returns a function that will execute @racket[lua-script] via
  @exec{EVALSHA} every time it's called.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Connection Commands}

@defcmd*[
  ((AUTH)
   ([(redis-auth! [client redis?] [password redis-string/c]) boolean?]
    [(redis-auth! [client redis?] [username redis-string/c] [password redis-string/c]) boolean?]))]{

  Authenticates the @racket[client] using @racket[password]. Raises
  an exception if authentication is not set up or if the password is
  invalid.

  The second variant may be used with Redis version 6.0 and later to
  emit @tt{AUTH username password} commands.
}

@defcmd[
  ((ECHO)
   (echo [message string?]) string?)]{

  Returns @racket[message].
}

@defcmd[
  ((PING)
   (ping) string?)]{

  Pings the server and returns @racket["PONG"].
}

@defcmd[
  ((QUIT)
   (quit!) void?)]{

  Gracefully disconnects from the server.
}

@defcmd[
  ((SELECT)
   (select-db! [db (integer-in 0 16)]) boolean?)]{

  Selects the current database.
}

@defcmd[
  ((SWAPDB)
   (swap-dbs! [a (integer-in 0 16)]
              [b (integer-in 0 16)]) boolean?)]{

  Swaps the given databases.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Geo Commands}

@deftogether[
  (@defthing[#:kind "contract" redis-latitude/c (real-in -90 90)]
   @defthing[#:kind "contract" redis-longitude/c (real-in -180 180)]
   @defthing[#:kind "contract" redis-geo/c (list/c redis-longitude/c redis-latitude/c redis-string/c)]
   @defthing[#:kind "contract" redis-geo-unit/c (or/c 'm 'km 'mi 'ft)])]{

  The geo-related contracts.
}

@defcmd[
  ((GEOADD)
   (geo-add! [key redis-key/c]
             [geo redis-geo/c] ...+) exact-nonnegative-integer?)]{

  Adds the geohashes of each @racket[geo] to the geo hash set at
  @racket[key] and returns the number of new items that were added.
}

@defcmd[
  ((GEODIST)
   (geo-dist [key redis-key/c]
             [member1 redis-string/c]
             [member2 redis-string/c]
             [#:unit unit (or/c #f redis-geo-unit/c) #f]) (or/c #f real?))]{

  Returns the geo distance between @racket[member1] and
  @racket[member2].
}

@defcmd[
  ((GEOHASH)
   (geo-hash [key redis-key/c]
             [mem redis-string/c] ...+) (listof (or/c #f bytes?)))]{

  Returns the geohash of each @racket[mem].
}

@defcmd[
  ((GEOPOS)
   (geo-pos [key redis-key/c]
            [mem redis-string/c] ...+) (listof (or/c #f (list/c redis-longitude/c redis-latitude/c))))]{

  Returns the longitude and latitude of each @racket[mem].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Hash Commands}

@defcmd*[
  ((HGETALL HMGET)
   ([(redis-hash-get [client redis?] [key redis-key/c]) hash?]
    [(redis-hash-get [client redis?] [key redis-key/c] [fld redis-string/c] ...+) hash?]))]{

  The first form grabs the entire hash at @racket[key].

  The second form grabs the given sub@racket[fld]s from the hash at
  @racket[key].
}

@defcmd[
  ((HEXISTS)
   (hash-has-key? [key redis-key/c]
                  [fld redis-string/c]) boolean?)]{

  Returns @racket[#t] when the hash at @racket[key] has a key named
  @racket[fld].
}

@defcmd[
  ((HINCRBY HINCRBYFLOAT)
   (hash-incr! [key redis-key/c]
               [fld redis-string/c]
               [amt real?]) real?)]{

  Increments the field @racket[fld] belonging to the hash at
  @racket[key] by @racket[amt] and returns the result.
}

@defcmd[
  ((HKEYS)
   (hash-keys [key redis-key/c]) (listof bytes?))]{

  Returns all the keys belonging to the hash at @racket[key].
}

@defcmd[
  ((HLEN)
   (hash-length [key redis-key/c]) exact-nonnegative-integer?)]{

  Returns the length of the hash at @racket[key].
}

@defcmd[
  ((HGET)
   (hash-ref [key redis-key/c]
             [fld redis-string/c]) redis-value/c)]{

  Grabs a single field value from the hash at @racket[key].
}

@defcmd[
  ((HDEL)
   (hash-remove! [key redis-key/c]
                 [fld redis-string/c] ...+) exact-nonnegative-integer?)]{

  Removes one or more @racket[fld]s from the hash at @racket[key] and
  returns the total number of fields that were removed.
}

@defcmd[
  ((HSCAN)
   (hash-scan [key redis-key/c]
              [#:cursor cursor exact-nonnegative-integer? 0]
              [#:pattern pattern (or/c #f redis-string/c) #f]
              [#:limit limit (or/c #f exact-positive-integer?) #f]) (values exact-nonnegative-integer? (listof redis-key/c)))]{

  Efficiently iterates through the set of keys in the hash at
  @racket[key].

  The @racket[limit] parameter serves as a hint for the
  implementation, but the server may return more items than
  @racket[limit] per iteration.
}

@defcmd*[
  ((HSET HMSET)
   ([(redis-hash-set! [client redis?] [key redis-key/c] [fld redis-string/c] [value redis-string/c]) boolean?]
    [(redis-hash-set! [client redis?] [key redis-key/c] [fld redis-string/c] [value redis-string/c] ...+ ...+) boolean?]
    [(redis-hash-set! [client redis?] [key redis-key/c] [d dict?]) boolean?]))]{

  The first form sets @racket[fld] to @racket[value] within the hash
  at @racket[key].

  The second form sets each pair of @racket[fld] and @racket[value]s
  within the hash at @racket[key].

  The third form stores @racket[d] at @racket[key].
}

@defcmd[
  ((HSTRLEN)
   (hash-string-length [key redis-key/c]
                       [fld redis-string/c]) exact-nonnegative-integer?)]{

  Returns the length of the byte string value of @racket[fld]
  belonging to the hash at @racket[key].
}

@defcmd[
  ((HVALS)
   (hash-values [key redis-key/c]) (listof bytes?))]{

  Returns all the values of the hash at @racket[key].
}

@defproc[
  (in-redis-hash [client redis?]
                 [key redis-key/c])
  (sequence/c (cons/c bytes? bytes?))]{

  Returns a sequence that can be used to efficiently iterate through
  the hash at @racket[key].

  Any keyword arguments that are passed to this function will be
  forwarded to @racket[redis-hash-scan].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{HyperLogLog Commands}

@defcmd[
  ((PFADD)
   (hll-add! [key redis-key/c]
             [value redis-string/c] ...+) boolean?)]{

  Adds all the @racket[value]s to the HyperLogLog struct at
  @racket[key].
}

@defcmd[
  ((PFCOUNT)
   (hll-count [key redis-key/c] ...+) exact-nonnegative-integer?)]{

  Returns the approximate cardinality of the union of the given
  HyperLogLog structs at each @racket[key].
}

@defcmd[
  ((PFMERGE)
   (hll-merge! [dest redis-key/c]
               [key redis-key/c] ...+) boolean?)]{

  Writes the union of the given HyperLogLog structs at each
  @racket[key] into the @racket[dest] key.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Key Commands}

@defcmd[
  ((EXISTS)
   (count-keys [key redis-key/c] ...) exact-nonnegative-integer?)]{

  Returns how many of the given @racket[key]s exist.  Keys are counted
  as many times as they are provided.
}

@defcmd[
  ((PEXPIREAT)
   (expire-at! [key redis-key/c]
               [ms exact-nonnegative-integer?]) boolean?)]{

  Marks @racket[key] so that it will expire at the UNIX timestamp
  represented by @racket[ms] milliseconds.  Returns @racket[#f] if the
  key is not in the database.
}

@defcmd[
  ((PEXPIRE)
   (expire-in! [key redis-key/c]
               [ms exact-nonnegative-integer?]) boolean?)]{

  Marks @racket[key] so that it will expire in @racket[ms]
  milliseconds.  Returns @racket[#f] if the key is not in the
  database.
}

@defcmd[
  ((EXISTS)
   (has-key? [key redis-key/c]) boolean?)]{

  Returns @racket[#t] when @racket[key] is in the database.
}

@defcmd[
  ((PTTL)
   (key-ttl [key redis-key/c]) (or/c 'missing 'persisted exact-nonnegative-integer?))]{

  Returns the number of milliseconds before @racket[key] expires.

  If @racket[key] is not present on the server, then @racket['missing]
  is returned.

  If @racket[key] exists but isn't marked for expiration, then
  @racket['persisted] is returned.
}

@defcmd[
  ((TYPE)
   (key-type [key redis-key/c]) (or/c 'none redis-key-type/c))]{

  Returns @racket[key]'s type.
}

@defcmd[
  ((KEYS)
   (keys [pattern redis-string/c]) (listof bytes?))]{

  Returns a list of all the keys in the database that match the given
  @racket[pattern].
}

@defcmd[
  ((MOVE)
   (move-key! [key redis-key/c]
              [db (integer-in 0 16)]) boolean?)]{

  Move @racket[key] from the current database into @racket[db].
}

@defcmd[
  ((PERSIST)
   (persist! [key redis-key/c]) boolean?)]{

  Removes @racket[key]'s expiration.
}

@defcmd[
  ((RANDOMKEY)
   (random-key) (or/c #f bytes?))]{

  Returns a random key from the database or @racket[#f] if the
  database is empty.
}

@defcmd[
  ((DEL UNLINK)
   (remove! [key redis-key/c] ...+
            [#:async? async? boolean? #f]) exact-nonnegative-integer?)]{

  Removes each @racket[key] from the database and returns the number
  of keys that were removed.

  When @racket[async?] is @racket[#t] the command returns immediately
  and the keys are removed asynchronously.
}

@defcmd[
  ((RENAME)
   (rename! [src redis-key/c]
            [dest redis-key/c]
            [#:unless-exists? unless-exists? boolean? #f]) boolean?)]{

  Renames @racket[src] to @racket[dest].

  If @racket[unless-exists?] is @racket[#t], then the key is only
  renamed if a key named @racket[dest] does not already exist.
}

@defcmd[
  ((SCAN)
   (scan [#:cursor cursor exact-nonnegative-integer? 0]
         [#:pattern pattern (or/c #f redis-string/c) #f]
         [#:limit limit (or/c #f exact-positive-integer?) #f]
         [#:type type (or/c #f redis-key-type/c) #f])
   (values exact-nonnegative-integer? (listof redis-key/c)))]{

  Efficiently iterates through all the keys in the database.

  The @racket[limit] parameter serves as a hint for the
  implementation, but the server may return more items than
  @racket[limit] per iteration.
}

@defcmd[
  ((TOUCH)
   (touch! [key redis-key/c] ...+) exact-nonnegative-integer?)]{

  Updates the last modification time for each @racket[key] and returns
  the number of keys that were updated.
}

@defproc[
  (in-redis [client redis?]) (sequence/c bytes?)]{

  Returns a sequence that can be used to efficiently iterate through
  the Redis database.

  Any keyword arguments that are passed to this function will be
  forwarded to @racket[redis-scan].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{List Commands}

@defcmd[
  ((RPUSH)
   (list-append! [key redis-key/c]
                 [value redis-string/c]) (or/c #f exact-nonnegative-integer?))]{

  Appends @racket[value] to the list at @racket[key], returning the
  new length of the list.
}

@defcmd[
  ((LRANGE)
   (list-get [key redis-key/c]) redis-value/c)]{

  An alias for @racket[redis-sublist] that retrieves the whole list at
  @racket[key].
}

@defcmd[
  ((LINSERT)
   (list-insert! [key redis-key/c]
                 [value redis-string/c]
                 [#:after pivot/after redis-string/c]
                 [#:before pivot/before redis-string/c]) (or/c #f exact-nonnegative-integer?))]{

  Inserts @racket[value] into the list at @racket[key]
  @racket[#:before] or @racket[#:after] the first occurrence of
  @racket[pivot/before] or @racket[pivot/after], respectively,
  returning the new size of the list.

  If @racket[key] is not a list, then @racket[#f] is returned.

  If both @racket[#:after] and @racket[#:before] are provided, an
  @racket[exn:fail:contract] error is raised.
}

@defcmd[
  ((LLEN)
   (list-length [key redis-key/c]) exact-nonnegative-integer?)]{

  Returns the length of the list at @racket[key].
}

@defcmd[
  ((LPOP BLPOP)
   (list-pop-left! [key redis-key/c] ...+
                   [#:block? block? boolean? #f]
                   [#:timeout timeout exact-nonnegative-integer? 0]) redis-value/c)]{

  Removes and then returns the first value from the list at @racket[key].

  @racketblock[
    (code:comment "LPOP a")
    (redis-list-pop-left! client "a")
  ]

  When @racket[block?] is @racket[#t], you can supply multiple
  @racket[key]s to retrieve a value from.  The function will wait up
  to @racket[timeout] seconds for a value and the result will contain
  a list containing the popped key and its value.

  @racketblock[
    (code:comment "BLPOP a b 0")
    (redis-list-pop-left! client "a" "b" #:block? #t)

    (code:comment "BLPOP a b 1")
    (redis-list-pop-left! client "a" "b" #:block? #t #:timeout 1)
  ]
}

@defcmd[
  ((RPOP RPOPLPUSH BRPOP BRPOPLPUSH)
   (list-pop-right! [key redis-key/c] ...+
                    [#:dest dest redis-key/c #f]
                    [#:block? block? boolean? #f]
                    [#:timeout timeout exact-nonnegative-integer? 0]) redis-value/c)]{

  Removes and then returns the last value from the list at @racket[key].

  @racketblock[
    (code:comment "RPOP a")
    (redis-list-pop-right! client "a")
  ]

  When a @racket[dest] is provided, the popped value is prepended to
  the list at @racket[dest].  If multiple @racket[key]s are provided
  along with a @racket[dest], then a contract error is raised.

  @racketblock[
    (code:comment "RPOPLPUSH a b")
    (redis-list-pop-right! client "a" #:dest "b")

    (code:comment "BRPOPLPUSH a b 0")
    (redis-list-pop-right! client "a" #:dest "b" #:block? #t)

    (code:comment "BRPOPLPUSH a b 1")
    (redis-list-pop-right! client "a" #:dest "b" #:block? #t #:timeout 1)
  ]

  When @racket[block?] is @racket[#t], you can supply multiple
  @racket[key]s to retrieve a value from or you can specify a
  @racket[dest] into which the popped value should be prepended.  The
  former operation maps to an @exec{BRPOP} and the latter to a
  @exec{BRPOPLPUSH} command.

  @racketblock[
    (code:comment "BRPOP a b 0")
    (redis-list-pop-right! client "a" "b" #:block? #t)

    (code:comment "BRPOP a b 1")
    (redis-list-pop-right! client "a" "b" #:block? #t #:timeout 1)
  ]

  In blocking mode, the function will wait up to @racket[timeout]
  seconds for a value and the result will contain a list containing
  the popped key and its value.
}

@defcmd[
  ((LPUSH)
   (list-prepend! [key redis-key/c]
                  [value redis-string/c]) (or/c #f exact-nonnegative-integer?))]{

  Prepends @racket[value] to the list at @racket[key], returning the
  new length of the list.
}

@defcmd[
  ((LINDEX)
   (list-ref [key redis-key/c] [index exact-integer?]) redis-value/c)]{

  Returns the item at @racket[index] in @racket[key] or @racket[#f].
}

@defcmd[
  ((LREM)
   (list-remove! [key redis-key/c]
                 [count exact-integer?]
                 [value redis-string/c]) exact-nonnegative-integer?)]{

  Removes up to @racket[count] @racket[value]s from the list at
  @racket[key] and returns the total number of items that were
  removed.
}

@defcmd[
  ((LSET)
   (list-set! [key redis-key/c]
              [index exact-integer?]
              [value redis-string/c]) boolean?)]{

  Sets the value at @racket[index] in the list at @racket[key] to
  @racket[value].
}

@defcmd[
  ((LTRIM)
   (list-trim! [key redis-key/c]
               [#:start start exact-integer? 0]
               [#:stop stop exact-integer? -1]) boolean?)]{

  Removes any elements from the list not included in the inclusive
  range between @racket[start] and @racket[end].
}

@defcmd[
  ((LRANGE)
   (sublist [key redis-key/c]
            [#:start start exact-integer? 0]
            [#:stop stop exact-integer? -1]) redis-value/c)]{

  Returns the sublist between the inclusive indices @racket[start] and
  @racket[end] of the list at @racket[key].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{PubSub Commands}

@(define (sync-tech . pre-content)
   (apply tech #:doc '(lib "scribblings/reference/reference.scrbl") pre-content))

@defproc[(make-redis-pubsub [client redis?]) redis-pubsub?]{
  Puts @racket[client] into PubSub mode and returns a value that can
  be used to subscribe to channels and receive messages.

  PubSub values are @sync-tech{synchronizable events} whose
  synchronization results are the messages received via the channels
  the value is subscribed to.

  Messages from channel subscriptions are represented as lists of two
  elements, where the first element is the name of the channel and the
  second the value of the message.

  Messages from pattern subscriptions are represented as lists of three
  elements, where the first element is the pattern, the second the
  channel and the third the value of the message.

  While in PubSub mode, any commands you try to send to the original
  connection will fail.

  Here's an example of how the pieces fit together:

  @racketblock[
    (require redis)

    (define pool
      (make-redis-pool))

    (thread
     (lambda ()
       (call-with-redis-client pool
         (lambda (c)
           (sleep 5)
           (redis-pubsub-publish! c "a" "hello")
           (sleep 5)
           (redis-pubsub-publish! c "a" "goodbye")))))

    (call-with-redis-client pool
      (lambda (c)
        (call-with-redis-pubsub c
          (lambda (p)
            (redis-pubsub-subscribe! p "a")
            (displayln (~v (sync p)))
            (displayln (~v (sync p)))))))
  ]
}

@defproc[(call-with-redis-pubsub [client redis?]
                                 [proc (-> redis-pubsub? any)]) any]{

  Puts @racket[client] into PubSub mode and calls @racket[proc] with
  the resulting value, ensuring that the @racket[client] is returned
  to a non-PubSub state once @racket[proc] exits.
}

@defproc[(redis-pubsub? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a pubsub value.
}

@defproc[(redis-pubsub-kill! [pubsub redis-pubsub?]) void?]{
  Unsubscribes @racket[pubsub] from all channels and takes the
  underlying connection out of PubSub mode.
}

@defcmd[
  ((PUBLISH)
   (pubsub-publish! [channel redis-string/c]
                    [message redis-string/c]) exact-nonnegative-integer?)]{

  Publishes @racket[message] to @racket[channel] and returns the total
  number of clients that received that message.
}

@defproc[(redis-pubsub-subscribe! [pubsub redis-pubsub?]
                                  [channel-or-pattern redis-string/c] ...+
                                  [#:patterns? patterns? boolean? #f]) void?]{

  Subscribes @racket[pubsub] to one or more @racket[channel-or-pattern]s.

  When @racket[patterns?] is @racket[#t], @racket[channel-or-pattern]
  values are treated as patterns, otherwise they're treated as
  channels.
}

@defproc[(redis-pubsub-unsubscribe! [pubsub redis-pubsub?]
                                    [channel-or-pattern redis-string/c] ...
                                    [#:patterns? patterns? boolean? #f]) void?]{

  Unsubscribes @racket[pubsub] from one or more @racket[channel-or-pattern]s.

  When @racket[patterns?] is @racket[#t], @racket[channel-or-pattern]
  values are treated as patterns, otherwise they're treated as
  channels.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Script Commands}

@defcmd[
  ((EVAL)
   (script-eval! [lua-script redis-string/c]
                 [#:keys keys (listof redis-key/c) null]
                 [#:args args (listof redis-string/c) null]) redis-value/c)]{

  Evaluate the @racket[lua-script] on the fly within the database.
}

@defcmd[
  ((EVALSHA)
   (script-eval-sha! [script-sha1 redis-string/c]
                     [#:keys keys (listof redis-key/c) null]
                     [#:args args (listof redis-string/c) null]) redis-value/c)]{

  Evaluate the lua script represented by the given
  @racket[script-sha1] on the fly within the database.
}

@defcmd[
  ((SCRIPT_EXISTS)
   (script-exists? [sha redis-string/c]) boolean?)]{

  Returns @racket[#t] when a script with the given @racket[sha] has
  previously been registered with the server.
}

@defcmd[
  ((SCRIPT_KILL)
   (script-kill!) boolean?)]{

  Stops the currently-running lua script.
}

@defcmd[
  ((SCRIPT_LOAD)
   (script-load! [script redis-string/c]) string?)]{

  Registers the given lua script with the server, returning its sha-1 hash.
}

@defcmd[
  ((SCRIPT_FLUSH)
   (scripts-flush!) boolean?)]{

  Removes all the registered lua scripts from the server.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Server Commands}

@defcmd[
  ((CLIENT_ID)
   (client-id) exact-integer?)]{

  Returns the current client id.
}

@defcmd[
  ((CLIENT_GETNAME)
   (client-name) (or/c #f string?))]{

  Returns the current client name.
}

@defcmd[
  ((FLUSHALL)
   (flush-all!) #t)]{

  Deletes everything in all the databases.
}

@defcmd[
  ((FLUSHDB)
   (flush-db!) #t)]{

  Deletes everything in the current database.
}

@defcmd[
  ((DBSIZE)
   (key-count) exact-nonnegative-integer?)]{

  Returns the number of keys in the database.
}

@defcmd[
  ((REWRITEAOF)
   (rewrite-aof!) #t)]{

  Starts the AOF-rewrite process on the server.
}

@defcmd[
  ((BGREWRITEAOF)
   (rewrite-aof/async!) #t)]{

  Starts a background AOF-rewrite on the server.
}

@defcmd[
  ((SAVE)
   (save!) #t)]{

  Initiates a save to disk of the database.
}

@defcmd[
  ((BGSAVE)
   (save/async!) #t)]{

  Starts the background save process on the server.
}

@defcmd[
  ((CLIENT_SETNAME)
   (set-client-name! [name redis-string/c]) boolean?)]{

  Sets the current client name on the server.
}

@defcmd[
  ((TIME)
   (time) real?)]{

  The current server time in milliseconds.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Set Commands}

@defcmd[
  ((SADD)
   (set-add! [key redis-key/c]
             [val redis-string/c] ...+) exact-nonnegative-integer?)]{

  Adds @racket[val]s to the set at @racket[key].
}

@defcmd[
  ((SCARD)
   (set-count [key redis-key/c]) exact-nonnegative-integer?)]{

  Returns the number of elements in the set at @racket[key].
}

@defcmd[
  ((SDIFF)
   (set-difference [key redis-key/c] ...+) (listof bytes?))]{

  Returns the set difference between the sets represented by each
  @racket[key].
}

@defcmd[
  ((SDIFFSTORE)
   (set-difference! [target redis-key/c] [key redis-key/c] ...+) exact-nonnegative-integer?)]{

  Computes the set difference between the sets at each @racket[key]
  and stores the result in @racket[target], returning the number of
  elements in the result.
}

@defcmd[
  ((SINTER)
   (set-intersect [key redis-key/c] ...+) (listof bytes?))]{

  Returns the set intersection between the sets represented by each
  @racket[key].
}

@defcmd[
  ((SINTERSTORE)
   (set-intersect! [target redis-key/c] [key redis-key/c] ...+) exact-nonnegative-integer?)]{

  Computes the set intersection between the sets at each @racket[key]
  and stores the result in @racket[target], returning the number of
  elements in the result.
}

@defcmd[
  ((SMEMBER)
   (set-member? [key redis-key/c] [val redis-string/c]) boolean?)]{

  Returns @racket[#t] when @racket[val] is a member of the set at
  @racket[key].
}

@defcmd[
  ((SMEMBERS)
   (set-members [key redis-key/c]) (listof bytes?))]{

  Returns a list containing every member of the set at @racket[key].
}

@defcmd[
  ((SMOVE)
   (set-move! [src redis-key/c]
              [dst redis-key/c]
              [val redis-key/c]) boolean?)]{

  Moves @racket[val] from the set at @racket[src] into the set at
  @racket[dst], returning @racket[#t] on success.
}

@defcmd[
  ((SPOP)
   (set-pop! [key redis-key/c]
             [#:count count exact-positive-integer? 1]) (listof bytes?))]{

  Removes and subsequently returns at most @racket[count] elements
  from the set at @racket[key].
}

@defcmd[
  ((SRANDMEMBER)
   (set-random-ref [key redis-key/c]
                   [count exact-integer? 1]) (or/c #f bytes? (listof bytes?)))]{

  Retrieves one or more random elements from the set at @racket[key].

  When @racket[count] is provided, a list of results whose size is
  @racket[(abs count)] or less will be returned.
}

@defcmd[
  ((SREM)
   (set-remove! [key redis-key/c]
                [val redis-string/c] ...+) exact-nonnegative-integer?)]{

  Removes each @racket[val] from the set at @racket[key], returning
  the total number of removed elements.
}

@defcmd[
  ((SSCAN)
   (set-scan [key redis-key/c]
             [#:cursor cursor exact-nonnegative-integer? 0]
             [#:pattern pattern (or/c #f redis-string/c) #f]
             [#:limit limit (or/c #f exact-positive-integer?) #f]) (values exact-nonnegative-integer? (listof redis-string/c)))]{

  Efficiently iterates through the set of values in the set at
  @racket[key].

  The @racket[limit] parameter serves as a hint for the
  implementation, but the server may return more items than
  @racket[limit] per iteration.
}

@defcmd[
  ((SUNION)
   (set-union [key redis-key/c] ...+) (listof bytes?))]{

  Returns the set union between the sets represented by each
  @racket[key].
}

@defcmd[
  ((SUNIONSTORE)
   (set-union! [target redis-key/c] [key redis-key/c] ...+) exact-nonnegative-integer?)]{

  Computes the set union between the sets at each @racket[key] and
  stores the result in @racket[target], returning the number of
  elements in the result.
}

@defproc[
  (in-redis-set [client redis?]
                [key redis-key/c]) (sequence/c bytes?)]{

  Returns a sequence that can be used to efficiently iterate through
  the set at @racket[key].

  Any keyword arguments that are passed to this function will be
  forwarded to @racket[redis-set-scan].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Sorted Set Commands}

@defcmd[
  ((ZADD)
   (zset-add! [key redis-key/c]
              [member redis-string/c] ...+
              [score real?] ...+) exact-nonnegative-integer?)]{

  Adds each @racket[member] with its associated @racket[score] to the
  sorted set at @racket[key].
}

@defcmd[
  ((ZCARD ZCOUNT)
   (zset-count [key redis-key/c]
               [#:min min real? #f]
               [#:max max real? #f]) exact-nonnegative-integer?)]{

  Counts the number of elements within the sorted set at @racket[key].
}

@defcmd[
  ((ZLEXCOUNT)
   (zset-count/lex [key redis-key/c]
                   [#:min min redis-string/c #"-"]
                   [#:max max redis-string/c #"+"]) exact-nonnegative-integer?)]{

  Counts the number of elements within the sorted set at @racket[key]
  according to a lexicographic sort of the set members.
}

@defcmd[
  ((ZINCRBY)
   (zset-incr! [key redis-key/c]
               [member redis-string/c]
               [n real? 1]) real?)]{

  Increments @racket[member]'s score within the sorted set at
  @racket[key] by @racket[n].
}

@defcmd[
  ((ZINTERSTORE)
   (zset-intersect! [dest redis-key/c]
                    [key redis-key/c] ...+
                    [#:weights weights (non-empty-listof real?) #f]
                    [#:aggregate aggregate (or/c 'sum 'min 'max) 'sum]) exact-nonnegative-integer?)]{

  Stores the intersection of all the sorted sets at the given
  @racket[key]s into @racket[dest].
}

@defcmd[
  ((BZPOPMAX ZPOPMAX)
   (zset-pop/max! [key redis-key/c] ...
                  [#:count count exact-positive-integer? 1]
                  [#:block? block? boolean? #f]
                  [#:timeout timeout exact-nonnegative-integer? 0])
   (or/c #f
         (list/c bytes? bytes? real?)
         (listof (cons/c bytes? real?))))]{

  When @racket[block?] is @racket[#t], a @racket[count] cannot be
  provided and the result will either be @racket[#f] in case the
  @racket[timeout] was reached or a list containing the name of the
  key that was popped, the name of the member and the member's score.

  When @racket[block?] is @racket[#f], a single @racket[key] may be
  provided, but @racket[count] can be positive.  The result will be an
  association list containing each popped member and its associated
  score.
}

@defcmd[
  ((BZPOPMIN ZPOPMIN)
   (zset-pop/min! [key redis-key/c] ...
                  [#:count count exact-positive-integer? 1]
                  [#:block? block? boolean? #f]
                  [#:timeout timeout exact-nonnegative-integer? 0])
   (or/c #f
         (list/c bytes? bytes? real?)
         (listof (cons/c bytes? real?))))]{

  When @racket[block?] is @racket[#t], a @racket[count] cannot be
  provided and the result will either be @racket[#f] in case the
  @racket[timeout] was reached or a list containing the name of the
  key that was popped, the name of the member and the member's score.

  When @racket[block?] is @racket[#f], a single @racket[key] may be
  provided, but @racket[count] can be positive.  The result will be an
  association list containing each popped member and its associated
  score.
}

@defcmd[
  ((ZRANK ZREVRANK)
   (zset-rank [key redis-key/c]
              [member redis-string/c]
              [#:reverse? reverse? boolean? #f]) (or/c #f exact-nonnegative-integer?))]{

  Returns the rank of @racket[member] within the sorted set at
  @racket[key].  If @racket[member] is not in the set, then
  @racket[#f] is returned.

  If @racket[reverse?] is @racket[#t], then the reverse rank of
  @racket[member] is returned.
}

@defcmd[
  ((ZREM)
   (zset-remove! [key redis-key/c]
                 [member redis-string/c] ...+) exact-nonnegative-integer?)]{

  Removes each @racket[member] from the sorted set at @racket[key].
}

@defcmd[
  ((ZREMRANGEBYLEX)
   (zset-remove/lex! [key redis-key/c]
                     [#:min min redis-string/c "-"]
                     [#:max max redis-string/c "+"]) exact-nonnegative-integer?)]{

  Removes all of the lexicographically-sorted members between
  @racket[min] and @racket[max] from the sorted set at @racket[key]
  and returns the total number of elements that were removed.
}

@defcmd[
  ((ZREMRANGEBYRANK)
   (zset-remove/rank! [key redis-key/c]
                      [#:start start exact-integer? 0]
                      [#:stop stop exact-integer? -1]) exact-nonnegative-integer?)]{

  Removes all of the members whose indices are within the inclusive
  range between @racket[start] and @racket[stop] from the sorted set
  at @racket[key] and returns the total number of elements that were
  removed.
}

@defcmd[
  ((ZREMRANGEBYSCORE)
   (zset-remove/score! [key redis-key/c]
                       [#:start start real? -inf.0]
                       [#:stop stop real? +inf.0]) exact-nonnegative-integer?)]{

  Removes all of the members whose scores are within the inclusive
  range between @racket[start] and @racket[stop] from the sorted set
  at @racket[key] and returns the total number of elements that were
  removed.
}

@defcmd[
  ((ZSCAN)
   (zset-scan [key redis-key/c]
              [#:cursor cursor exact-nonnegative-integer? 0]
              [#:pattern pattern (or/c #f redis-string/c) #f]
              [#:limit limit (or/c #f exact-positive-integer?) #f]) (values exact-nonnegative-integer? (listof (cons/c redis-string/c real?))))]{

  Efficiently iterates through the members and their associated scores
  in the sorted set at @racket[key].

  The @racket[limit] parameter serves as a hint for the
  implementation, but the server may return more items than
  @racket[limit] per iteration.
}

@defcmd[
  ((ZSCORE)
   (zset-score [key redis-key/c]
               [member redis-string/c]) (or/c #f real?))]{

  Returns the score of @racket[member] from the sorted set at
  @racket[key] or @racket[#f] if @racket[member] isn't in the set.
}

@defcmd[
  ((ZUNIONSTORE)
   (zset-union! [dest redis-key/c]
                [key redis-key/c] ...+
                [#:weights weights (non-empty-listof real?) #f]
                [#:aggregate aggregate (or/c 'sum 'min 'max) 'sum]) exact-nonnegative-integer?)]{

  Stores the union of all the sorted sets at the given @racket[key]s
  into @racket[dest].
}

@defcmd[
  ((ZRANGE ZREVRANGE)
   (subzset [key redis-key/c]
            [#:reverse? reverse? boolean? #f]
            [#:start start exact-integer? 0]
            [#:stop stop exact-integer? -1]
            [#:include-scores? scores? boolean? #f]) (or/c (listof bytes?)
                                                           (listof (cons/c bytes? real?))))]{

  Retrieves the members between the inclusive indices @racket[start]
  and @racket[stop] from the sorted set at @racket[key].  If
  @racket[reverse?] is @racket[#t], then the elements are sorted in
  reverse before retrieval.

  When @racket[scores?] is @racket[#t], the result contains an alist
  mapping members to their scores.
}

@defcmd[
  ((ZRANGEBYLEX ZREVRANGEBYLEX)
   (subzset/lex [key redis-key/c]
                [#:reverse? reverse? boolean? #f]
                [#:min min redis-string/c #"-"]
                [#:max max redis-string/c #"+"]
                [#:limit limit exact-positive-integer? #f]
                [#:offset offset exact-nonnegative-integer? 0]) (listof bytes?))]{

  Retrieves the lexicographically-sorted set of members between
  @racket[min] and @racket[max] from the sorted set at @racket[key].
  If @racket[reverse?] is @racket[#t], then the elements are sorted in
  reverse before retrieval.
}

@defcmd[
  ((ZRANGEBYSCORE ZREVRANGEBYSCORE)
   (subzset/score [key redis-key/c]
                  [#:reverse? reverse? boolean? #f]
                  [#:include-scores? scores? boolean? #f]
                  [#:start start real? -inf.0]
                  [#:stop stop real? +inf.0]
                  [#:limit limit exact-positive-integer? #f]
                  [#:offset offset exact-nonnegative-integer? 0]) (or/c (listof bytes?)
                                                                        (listof (cons/c bytes? real?))))]{

  Retrieves the of members whose scores are between @racket[start] and
  @racket[stop] from the sorted set at @racket[key].  When
  @racket[reverse?] is @racket[#t], then the elements are sorted in
  reverse before retrieval.

  When @racket[scores?] is @racket[#t], the result contains an alist
  mapping members to their scores.
}

@defproc[
  (in-redis-zset [client redis?]
                 [key redis-key/c]) (sequence/c (cons/c bytes? real?))]{

  Returns a sequence that can be used to efficiently iterate through
  the sorted set at @racket[key].

  Any keyword arguments that are passed to this function will be
  forwarded to @racket[redis-zset-scan].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{Stream Commands}

@defstruct[redis-stream-entry ([id bytes?]
                               [fields (hash/c bytes? bytes?)])]{

  A struct representing individual entries within a stream.
}

@defstruct[redis-stream-entry/pending ([id bytes?]
                                       [consumer bytes?]
                                       [elapsed-time exact-nonnegative-integer?]
                                       [delivery-count exact-positive-integer?])]{

  A struct representing pending entries within a stream group.
}

@defstruct[redis-stream-info ([length exact-nonnegative-integer?]
                              [radix-tree-keys exact-nonnegative-integer?]
                              [radix-tree-nodes exact-nonnegative-integer?]
                              [groups exact-nonnegative-integer?]
                              [last-generated-id bytes?]
                              [first-entry redis-stream-entry?]
                              [last-entry redis-stream-entry?])]{

  A struct representing information about a stream.
}

@defstruct[redis-stream-group ([name bytes?]
                               [consumers exact-nonnegative-integer?]
                               [pending exact-nonnegative-integer?])]{

  A struct representing an individual stream group.
}

@defstruct[redis-stream-consumer ([name bytes?]
                                  [idle exact-nonnegative-integer?]
                                  [pending exact-nonnegative-integer?])]{

  A struct representing an individual stream consumer.
}

@defcmd[
  ((XACK)
   (stream-ack! [key redis-key/c]
                [group redis-string/c]
                [id redis-string/c] ...+) exact-nonnegative-integer?)]{

  Acknowledges all of the messages represented by the given
  @racket[ids] within the @racket[group] belonging to the stream at
  @racket[key] and returns the total number of acknowledged messages.
}

@defcmd[
  ((XADD)
   (stream-add! [key redis-key/c]
                [flds-and-vals redis-string/c] ...+
                [#:id id redis-string/c "*"]
                [#:max-length max-length exact-positive-integer?]
                [#:max-length/approximate max-length/approximate exact-positive-integer?]) bytes?)]{

  Adds an entry to the stream at @racket[key] with fields
  @racket[flds-and-vals].  @racket[flds-and-vals] must contain an even
  number of items (one field name and one value for each field).

  See the Redis documentation for the value of the @racket[id] parameter.

  Either @racket[max-length] or @racket[max-length/approximate] can be
  provided, but not both.
}

@defcmd[
  ((XGROUP_DELCONSUMER)
   (stream-consumer-remove! [key redis-key/c]
                            [group redis-string/c]
                            [consumer redis-string/c]) boolean?)]{

  Removes @racket[consumer] from the stream group named @racket[group]
  belonging to the stream at @racket[key].
}

@defcmd[
  ((XINFO_CONSUMERS)
   (stream-consumers [key redis-key/c]
                     [group redis-string/c]) (listof redis-stream-consumer?))]{

  Returns all of the consumers belonging to the @racket[group] of the
  stream at @racket[key].
}

@defcmd[
  ((XINFO_STREAM)
   (stream-get [key redis-key/c]) redis-stream-info?)]{

  Returns information about the stream at @racket[key].
}

@defcmd[
  ((XGROUP_CREATE)
   (stream-group-create! [key redis-key/c]
                         [group redis-string/c]
                         [starting-id redis-string/c]) boolean?)]{

  Creates a stream group called @racket[group] for the stream at
  @racket[key].
}

@defcmd[
  ((XREADGROUP)
   (stream-group-read! [#:streams streams (non-empty-listof (cons/c redis-key/c (or/c 'new-entries redis-string/c)))]
                       [#:group group redis-string/c]
                       [#:consumer consumer redis-string/c]
                       [#:limit limit (or/c #f exact-positive-integer?) #f]
                       [#:block? block? boolean? #f]
                       [#:timeout timeout exact-nonnegative-integer? 0]
                       [#:no-ack? no-ack? boolean? #f]) (or/c #f (listof (list/c bytes? (listof redis-stream-entry?)))))]{

  Reads entries from a stream group for every stream and id pair given
  via the @racket[streams] alist and returns a list of lists where the
  @racket[first] element of each sublist is the name of the stream and
  the @racket[second] is the list of entries retrieved for that
  stream.

  The special @racket['new-entries] id value maps to the special id
  @racket[">"], meaning that only entries that haven't yet been
  retrieved by this @racket[consumer] should be returned.
}

@defcmd[
  ((XGROUP_REMOVE)
   (stream-group-remove! [key redis-key/c]
                         [group redis-string/c]) boolean?)]{

  Removes the group named @racket[group] from the stream at
  @racket[key].
}

@defcmd[
  ((XGROUP_SETID)
   (stream-group-set-id! [key redis-key/c]
                         [group redis-string/c]
                         [id redis-string/c]) boolean?)]{

  Sets the starting @racket[id] for the stream group named
  @racket[group] belonging to the stream at @racket[key].
}

@defcmd[
  ((XINFO_GROUPS)
   (stream-groups [key redis-key/c]) (listof redis-stream-group?))]{

  Returns all of the groups belonging to the stream at @racket[key].
}

@defcmd[
  ((XLEN)
   (stream-length [key redis-key/c]) exact-nonnegative-integer?)]{

  Returns the length of the stream at @racket[key].
}

@defcmd[
  ((XREAD)
   (stream-read! [#:streams streams (non-empty-listof (cons/c redis-key/c (or/c 'new-entries redis-string/c)))]
                 [#:limit limit (or/c #f exact-positive-integer?) #f]
                 [#:block? block? boolean? #f]
                 [#:timeout timeout exact-nonnegative-integer? 0]) (or/c #f (listof (list/c bytes? (listof redis-stream-entry?)))))]{

  Reads entries from every stream and id pair given via the
  @racket[streams] alist and returns a list of lists where the
  @racket[first] element of each sublist is the name of the stream and
  the @racket[second] is the list of entries retrieved for that
  stream.

  The special @racket['new-entries] id value maps to the special id
  @racket["$"], meaning that only entries added after the read was
  initiated should be retrieved.
}

@defcmd[
  ((XDEL)
   (stream-remove! [key redis-key/c]
                   [id redis-string/c] ...+) exact-nonnegative-integer?)]{

  Removes the entries represented by each @racket[id] from the stream
  at @racket[key], returning the total number of removed entries.
}

@defcmd[
  ((XTRIM)
   (stream-trim! [key redis-key/c]
                 [#:max-length max-length exact-positive-integer?]
                 [#:max-length/approximate max-length/approximate exact-positive-integer?]) exact-nonnegative-integer?)]{

  Trims the stream at @racket[key] to either @racket[max-length] or
  @racket[max-length/approximate].  Usually, you will want to use the
  latter for performance.  Either keyword argument must be provided
  but not both.
}

@defcmd[
  ((XRANGE)
   (substream [key redis-key/c]
              [#:reverse? reverse? boolean? #f]
              [#:start start (or/c 'first-entry 'last-entry redis-string/c)]
              [#:stop stop (or/c 'first-entry 'last-entry redis-string/c)]
              [#:limit limit (or/c #f exact-positive-integer?)]) (listof redis-stream-entry?))]{

  Returns at most @racket[limit] entries between @racket[start] and
  @racket[stop] from the stream at @racket[key].  If @racket[limit] is
  @racket[#f], then all the entries are returned.

  @racket[start] and @racket[stop] accept a stream entry id or one of
  the special values @racket['first-entry] and @racket['last-entry],
  which map to the special ids @racket["-"] and @racket["+"],
  respectively, which mean the very first and the very last item in
  the stream.

  When @racket[reverse?] is @racket[#t] the entries are returned in
  reverse order and @racket[start] and @racket[stop] are swapped.
}

@defcmd[
  ((XPENDING)
   (substream/group [key redis-key/c]
                    [group redis-string/c]
                    [#:start start (or/c 'first-entry 'last-entry redis-string/c) 'first-entry]
                    [#:stop stop (or/c 'first-entry 'last-entry redis-string/c) 'last-entry]
                    [#:limit limit exact-positive-integer? 10]) (listof redis-stream-entry/pending?))]{

  Retrieves @racket[limit] pending entries for the @racket[group]
  belonging to the stream at @racket[key] between @racket[start] and
  @racket[stop].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section{String/bytes Commands}

All Redis strings are sequences of bytes, so whereas most of the
following functions accept both @racket[bytes?] and @racket[string?]
values, when a key is retrieved from the server, its value will always
be either @racket[#f] (if it doesn't exist) or @racket[bytes?].

@defcmd[
  ((APPEND)
   (bytes-append! [key redis-key/c]
                  [value redis-string/c]) exact-nonnegative-integer?)]{

  @exec{APPEND}s @racket[value] to the byte string at @racket[key] if
  it exists and returns the new length of @racket[key].
}

@defcmd[
  ((BITCOUNT)
   (bytes-bitcount [key redis-key/c]
                   [#:start start exact-integer? 0]
                   [#:stop stop exact-integer? -1]) exact-nonnegative-integer?)]{

  Counts the bits in the byte string at @racket[key] between
  @racket[start] and @racket[end] using @exec{BITCOUNT}.
}

@defcmd[
  ((BITOP)
   (bytes-bitwise-and! [dst redis-key/c]
                       [src redis-key/c] ...) exact-nonnegative-integer?)]{

  @exec{AND}s all the @racket[src] byte strings together and saves the
  result into @racket[dst], returning the length of the resulting byte
  string.
}

@defcmd[
  ((BITOP)
   (bytes-bitwise-not! [src redis-key/c]
                       [dst redis-key/c src]) exact-nonnegative-integer?)]{

  Flips all the bits in the byte string at @racket[src] and stores the
  result in @racket[dst], returning the length of the resulting byte
  string.
}

@defcmd[
  ((BITOP)
   (bytes-bitwise-or! [dst redis-key/c]
                      [src redis-key/c] ...) exact-nonnegative-integer?)]{

  @exec{OR}s all the @racket[src] byte strings together and saves the
  result into @racket[dst], returning the length of the resulting byte
  string.
}

@defcmd[
  ((BITOP)
   (bytes-bitwise-xor! [dst redis-key/c]
                       [src redis-key/c] ...) exact-nonnegative-integer?)]{

  @exec{XOR}s all the @racket[src] byte strings together and saves the
  result into @racket[dst], returning the length of the resulting byte
  string.
}

@defcmd[
  ((SETRANGE)
   (bytes-copy! [key redis-key/c]
                [offset exact-nonnegative-integer?]
                [value redis-string/c]) exact-nonnegative-integer?)]{

  Writes @racket[value] into the byte string at @racket[key] starting
  at @racket[offset], extending the string if necessary.  Returns the
  new length of the string.
}

@defcmd[
  ((DECR DECRBY)
   (bytes-decr! [key redis-key/c]
                [amt exact-integer? 1]) exact-integer?)]{

  Decrements the numeric value of the byte string at @racket[key] by
  @racket[amt].

  If the value at @racket[key] is not an integer, then the function
  will raise an @racket[exn:fail:redis] error.
}

@defcmd[
  ((GET MGET)
   (bytes-get [key redis-key/c] ...+) (or/c #f bytes?))]{

  Retrieves one or more @racket[key]s from the database.
}

@defcmd[
  ((INCR INCRBY INCRBYFLOAT)
   (bytes-incr! [key redis-key/c]
                [amt real?]) real?)]{

  Increments the value at @racket[key] by @racket[amt] and returns the
  result.
}

@defcmd[
  ((STRLEN)
   (bytes-length [key redis-key/c]) exact-nonnegative-integer?)]{

  Returns the number of bytes in the string at @racket[key].
}

@defcmd[
  ((GETBIT)
   (bytes-ref/bit [key redis-key/c]
                  [offset exact-nonnegative-integer?]) (or/c 0 1))]{

  Retrieves the value of the bit at @racket[offset] within the
  byte string at @racket[key].
}

@defcmd[
  ((SET)
   (bytes-set! [key redis-key/c]
               [value redis-string/c]
               [#:expires-in expires-in (or/c #f exact-nonnegative-integer?) #f]
               [#:unless-exists? unless-exists? boolean? #f]
               [#:when-exists? when-exists? boolean? #f]) boolean?)]{

  @exec{SET}s @racket[key] to @racket[value].  Byte string
  @racket[value]s are written to the server as-is, strings are
  converted to byte strings first.

  When @racket[expires-in] is @racket[#t], then the key will expire
  after @racket[expires-in] milliseconds.

  When @racket[unless-exists?] is @racket[#t], then the key will only
  be set if it doesn't already exist.

  When @racket[when-exists?] is @racket[#t], then the key will only be
  set if it already exists.
}

@defcmd[
  ((SETBIT)
   (bytes-set/bit! [key redis-key/c]
                   [offset exact-nonnegative-integer?]
                   [value (or/c 0 1)]) (or/c 0 1))]{

  Sets the bit at @racket[offset] within the byte string at
  @racket[key] to @racket[value] and returns the previous bit value at
  that position.
}

@defcmd[
  ((GETRANGE)
   (subbytes [key redis-key/c]
             [#:start start exact-integer? 0]
             [#:stop stop exact-integer? -1]) bytes?)]{

  Returns a substring between the indices @racket[start] and
  @racket[stop] of the byte string at @racket[key].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@section[#:tag "easy"]{Simplified API}
@defmodule[redis/easy]

The @racket[redis/easy] module re-provides all the bindings
documented in the previous sections except modified to implicitly
read a client from either @racket[current-redis-client] or
@racket[current-redis-pool].

For example, instead of

@racketblock[
(define pool (make-redis-pool))
(call-with-redis-client pool
  (lambda (c)
   (redis-bytes-get c "some-key")))
]

you can write

@racketblock[
(current-redis-pool (make-redis-pool))
(redis-bytes-get "some-key")
]

@deftogether[(
  @defparam[current-redis-client client redis? #:value #f]
  @defparam[current-redis-pool pool redis-pool? #:value #f]
)]{
  Parameters implicitly used by the bindings exported from
  @racket[redis/easy]. The @racket[current-redis-client] parameter takes
  precedence over the @racket[current-redis-pool] parameter if set.
}
