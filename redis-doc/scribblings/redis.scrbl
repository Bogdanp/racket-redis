#lang scribble/manual

@(require (for-label racket/base
                     racket/contract
                     racket/serialize
                     redis)
          "redis.rkt")

@title{@exec{redis}: bindings for Redis}
@author[(author+email "Bogdan Popa" "bogdan@defn.io")]

@section[#:tag "intro"]{Introduction}

This package provides up-to-date bindings to the Redis database.

@section[#:tag "reference"]{Reference}
@defmodule[redis]

The functions in this package are named differently from their Redis
counterparts to avoid confusion as much as possible.  The rule is that
when a function name is ambiguous with regards to the type of value it
operates on, then it must contain the type in its name.

For example, rather than exposing a function called @exec{redis-ref}
for looking up keys, we expose @racket[redis-bytes-ref] so that it is
clear to the user that they're about to receive one or more byte
strings.  On the other hand, @racket[redis-rename!] doesn't need to be
prefixed, because the operation can only refer to renaming a key.

If you're looking to run a particular command but are not sure what
the associated function's name is, simply search this documentation
for that command.  The documentation for each function names the
commands said function relies on.

@subsection[#:tag "client"]{The Client}

Each client represents a single TCP connection to the Redis server.

@defproc[(make-redis [#:client-name client-name string? "racket-redis"]
                     [#:host host string? "127.0.0.1"]
                     [#:port port (integer-in 0 65535) 6379]
                     [#:timeout timeout (and/c rational? positive?) 5]
                     [#:db db (integer-in 0 16) 0]) redis?]{

  Creates a redis client and immediately attempts to connect to the
  database at @racket[host] and @racket[port].  The @racket[timeout]
  parameter controls the maximum amount of time the client will wait
  for any individual response from the database.
}

@defproc[(redis? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a Redis client.
}

@defproc[(redis-connected? [client redis?]) boolean?]{
  Returns @racket[#t] when @racket[client] appears to be connected to
  the database.  Does not detect broken pipes.
}

@defproc[(redis-connect! [client redis?]) void?]{
  Initiales a connection to the database.  If one is already open,
  then the client is first disconnected before the new connection is
  made.
}

@defproc[(redis-disconnect! [client redis?]) void?]{
  Disconnects from the server immediately and without sending a
  @exec{QUIT} command.  Does nothing if the client is already
  disconnected.
}

@defparam[redis-null value any/c #:value 'null]{
  The parameter that holds the value that represents "null" values
  from Redis.
}

@defproc[(redis-null? [v any/c]) boolean?]{
  Returns @racket[#t] if @racket[v] is @racket[equal?] to
  @racket[(redis-null)].
}


@subsection[#:tag "scripts"]{Scripts}

@defthing[redis-script/c (->* (redis?)
                              (#:keys (listof string?)
                               #:args (listof string?))
                              redis-value/c)]{

  The contract for lua-backed Redis scripts.
}

@defproc[(make-redis-script [client redis?]
                            [lua-script string?]) redis-script/c]{

  Returns a function that will execute @racket[lua-script] via
  @exec{EVALSHA} every time it's called.
}


@subsection[#:tag "commands"]{Supported Commands}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{Connection Commands}

@defcmd[
  ((AUTH)
   (auth! [password string?]) string?)]{

  @exec{AUTH}s the current connection using @racket[password].  Raises
  an exception if authentication is not set up or if the password is
  invalid.
}

@defcmd[
  ((SELECT)
   (select-db! [db (integer-in 0 16)]) boolean?)]{

  Selects the current database.
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


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{Key Commands}

@defcmd[
  ((EXISTS)
   (count-keys [key string?] ...) exact-nonnegative-integer?)]{

  Returns how many of the given @racket[key]s exist.  Keys are counted
  as many times as they are provided.
}

@defcmd[
  ((PEXPIREAT)
   (expire-at! [key string?]
               [ms exact-nonnegative-integer?]) boolean?)]{

  Marks @racket[key] so that it will expire at the UNIX timestamp
  represented by @racket[ms] milliseconds.  Returns @racket[#f] if the
  key is not in the database.
}

@defcmd[
  ((PEXPIRE)
   (expire-in! [key string?]
               [ms exact-nonnegative-integer?]) boolean?)]{

  Marks @racket[key] so that it will expire in @racket[ms]
  milliseconds.  Returns @racket[#f] if the key is not in the
  database.
}

@defcmd[
  ((EXISTS)
   (has-key? [key string?]) boolean?)]{

  Returns @racket[#t] when @racket[key] is in the database.
}

@defcmd[
  ((PERSIST)
   (persist! [key string?]) boolean?)]{

  Removes @racket[key]'s expiration.
}

@defcmd[
  ((RANDOMKEY)
   (random-key) (or/c redis-null? string?))]{

  Returns a random key from the database or @racket[(redis-null)].
}

@defcmd[
  ((DEL)
   (remove! [key string?] ...+) exact-nonnegative-integer?)]{

  Removes each @racket[key] from the database and returns the number
  of keys that were removed.
}

@defcmd[
  ((RENAME)
   (rename! [src string?]
            [dest string?]
            [#:unless-exists? unless-exists? boolean? #f]) boolean?)]{

  Renames @racket[src] to @racket[dest].

  If @racket[unless-exists?] is @racket[#t], then the key is only
  renamed if a key named @racket[dest] does not already exist.
}

@defcmd[
  ((TOUCH)
   (touch! [key string?] ...+) exact-nonnegative-integer?)]{

  Updates the last modification time for each @racket[key] and returns
  the number of keys that were updated.
}

@defcmd[
  ((PTTL)
   (key-ttl [key string?]) (or/c 'missing 'persisted exact-nonnegative-integer?))]{

  Returns the number of milliseconds before @racket[key] expires.

  If @racket[key] is not present on the server, then @racket['missing]
  is returned.

  If @racket[key] exists but isn't marked for expiration, then
  @racket['persisted] is returned.
}

@defcmd[
  ((TYPE)
   (key-type [key string?]) (or/c 'none 'string 'list 'set 'zset 'hash 'stream))]{

  Returns @racket[key]'s type.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{List Commands}

@defcmd[
  ((RPUSH)
   (list-append! [key string?]
                 [value (or/c bytes? string?)]) (or/c false/c exact-nonnegative-integer?))]{

  Appends @racket[value] to the list at @racket[key], returning the
  new length of the list.
}

@defcmd[
  ((LINSERT)
   (list-insert! [key string?]
                 [value (or/c bytes? string?)]
                 [#:after pivot/after (or/c bytes? string?)]
                 [#:before pivot/before (or/c bytes? string?)]) (or/c false/c exact-nonnegative-integer?))]{

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
   (list-length [key string?]) exact-nonnegative-integer?)]{

  Returns the length of the list at @racket[key].
}

@defcmd[
  ((LPOP)
   (list-pop-left! [key string?]) redis-value/c)]{

  Removes and then returns the first value from the list at @racket[key].
}

@defcmd[
  ((RPOP)
   (list-pop-right! [key string?]) redis-value/c)]{

  Removes and then returns the last value from the list at @racket[key].
}

@defcmd[
  ((LPUSH)
   (list-prepend! [key string?]
                  [value (or/c bytes? string?)]) (or/c false/c exact-nonnegative-integer?))]{

  Prepends @racket[value] to the list at @racket[key], returning the
  new length of the list.
}

@defcmd[
  ((LRANGE)
   (list-range [key string?]
                    [start exact-integer? 0]
                    [stop exact-integer? -1]) redis-value/c)]{

  Returns the sublist between the inclusive indices @racket[start] and
  @racket[end] of the list at @racket[key].
}

@defcmd[
  ((LINDEX)
   (list-ref [key string?] [index exact-integer?]) redis-value/c)]{

  Returns the item at @racket[index] in @racket[key] or @racket[(redis-null)].
}

@defcmd[
  ((LSET)
   (list-set! [key string?] [index exact-integer?] [value (or/c bytes? string?)]) boolean?)]{

  Sets the value at @racket[index] in the list at @racket[key] to
  @racket[value].
}

@defcmd[
  ((LTRIM)
   (list-trim! [key string?]
                    [start exact-integer? 0]
                    [stop exact-integer? -1]) boolean?)]{

  Removes any elements from the list not included in the inclusive
  range between @racket[start] and @racket[end].
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{Script Commands}

@defcmd[
  ((EVAL)
   (script-eval! [lua-script string?]
                 [#:keys keys (listof string?) null]
                 [#:args args (listof string?) null]) redis-value/c)]{

  Evaluate the @racket[lua-script] on the fly within the database.
}

@defcmd[
  ((EVALSHA)
   (script-eval-sha! [script-sha1 string?]
                     [#:keys keys (listof string?) null]
                     [#:args args (listof string?) null]) redis-value/c)]{

  Evaluate the lua script represented by the given
  @racket[script-sha1] on the fly within the database.
}

@defcmd[
  ((SCRIPT_FLUSH)
   (scripts-flush!) boolean?)]{

  Removes all the registered lua scripts from the server.
}

@defcmd[
  ((SCRIPT_KILL)
   (script-kill!) boolean?)]{

  Stops the currently-running lua script.
}

@defcmd[
  ((SCRIPT_LOAD)
   (script-load! [script string?]) string?)]{

  Registers the given lua script with the server, returning its sha-1 hash.
}


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{Server Commands}

@defcmd[
  ((BGREWRITEAOF)
   (bg-rewrite-aof!) #t)]{

  Starts the AOF-rewrite process on the server.
}

@defcmd[
  ((BGSAVE)
   (bg-save!) #t)]{

  Starts the save process on the server.
}

@defcmd[
  ((CLIENT_ID)
   (client-id) exact-integer?)]{

  Returns the current client id.
}

@defcmd[
  ((CLIENT_GETNAME)
   (client-name) (or/c redis-null? string?))]{

  Returns the current client name.
}

@defcmd[
  ((CLIENT_SETNAME)
   (set-client-name! [name string?]) boolean?)]{

  Sets the current client name on the server.
}

@defcmd[
  ((DBSIZE)
   (key-count) exact-nonnegative-integer?)]{

  Returns the number of keys in the database.
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


@;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
@subsubsection{String/bytes Commands}

All Redis strings are sequences of bytes, so whereas most of the
following functions accept both @racket[bytes?] and @racket[string?]
values, when a key is retrieved from the server, its value will always
be either @racket[(redis-null)] or @racket[bytes?].

@defcmd[
  ((APPEND)
   (bytes-append! [key string?]
                  [value (or/c bytes? string?)]) exact-nonnegative-integer?)]{

  @exec{APPEND}s @racket[value] to the byte string at @racket[key] if
  it exists and returns the new length of @racket[key].
}

@defcmd[
  ((BITCOUNT)
   (bytes-bitcount [key string?]
                   [#:start start exact-integer? 0]
                   [#:end end exact-integer? -1]) exact-nonnegative-integer?)]{

  Counts the bits in the byte string at @racket[key] between
  @racket[start] and @racket[end] using @exec{BITCOUNT}.
}

@defcmd[
  ((DECR DECRBY)
   (bytes-decr! [key string?]
                [amt exact-integer? 1]) exact-integer?)]{

  Decrements the numeric value of the byte string at @racket[key] by
  @racket[amt].

  If the value at @racket[key] is not an integer, then the function
  will raise an @racket[exn:fail:redis] error.
}

@defcmd[
  ((GET MGET)
   (bytes-ref [key string?] ...+) (or/c redis-null? bytes?))]{

  Retrieves one or more @racket[key]s from the database.
}

@defcmd[
  ((INCR INCRBY INCRBYFLOAT)
   (bytes-incr! [key string?]
                 [amt (or/c exact-integer? rational?)]) (or/c string? exact-integer?))]{

  Increments the value at @racket[key] by @racket[amt].  If the
  resulting value is a float, then a string is returned rather than an
  integer.

  If the value at @racket[key] is not a number, then the function will
  raise an @racket[exn:fail:redis] error.
}

@defcmd[
  ((SET)
   (bytes-set! [key string?]
               [value (or/c bytes? string?)]
               [#:expires-in expires-in (or/c false/c exact-nonnegative-integer?) #f]
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
