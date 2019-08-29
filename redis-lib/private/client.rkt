#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/contract
         racket/list
         racket/match
         racket/string
         racket/tcp
         "error.rkt"
         "protocol.rkt")

;; client ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 make-redis
 redis?
 redis-connected?
 redis-connect!
 redis-disconnect!)

(struct redis (host port timeout in out mutex response-ch response-reader)
  #:mutable)

(define/contract (make-redis #:client-name [client-name "racket-redis"]
                             #:host [host "127.0.0.1"]
                             #:port [port 6379]
                             #:timeout [timeout 5]
                             #:db [db 0])
  (->* ()
       (#:host non-empty-string?
        #:port (integer-in 0 65535)
        #:timeout (and/c real? positive?)
        #:db (integer-in 0 16))
       redis?)

  (define mutex (make-semaphore 1))
  (define client (redis host port timeout #f #f mutex #f #f))
  (begin0 client
    (redis-connect! client)
    (redis-select! client db)
    (redis-set-client-name! client client-name)))

(define/contract (redis-connected? client)
  (-> redis? boolean?)
  (and (redis-in client)
       (redis-out client)
       (redis-response-reader client)
       (not (port-closed? (redis-in client)))
       (not (port-closed? (redis-out client)))
       (not (thread-dead? (redis-response-reader client)))))

(define/contract (redis-connect! client)
  (-> redis? void?)
  (when (redis-connected? client)
    (redis-disconnect! client))

  (call-with-redis client
    (lambda _
      (define-values (in out)
        (tcp-connect (redis-host client)
                     (redis-port client)))

      (set-redis-in! client in)
      (set-redis-out! client out)

      (define response-ch (make-channel))
      (define response-reader
        (thread (make-response-reader in response-ch)))

      (set-redis-response-ch! client response-ch)
      (set-redis-response-reader! client response-reader))))

(define/contract (redis-disconnect! client)
  (-> redis? void?)
  (call-with-redis client
    (lambda _
      (kill-thread (redis-response-reader client))
      (tcp-abandon-port (redis-in client))
      (tcp-abandon-port (redis-out client)))))

(define ((make-response-reader in ->out))
  (let loop ()
    (channel-put ->out (redis-read in))
    (loop)))

(define (send-request! client cmd [args null])
  (parameterize ([current-output-port (redis-out client)])
    (redis-write (cons cmd args))
    (flush-output)))

(define (take-response! client)
  (match (sync/timeout (redis-timeout client)
                       (redis-response-ch client))
    [#f
     (raise (exn:fail:redis:timeout
             "timed out while waiting for response from Redis"
             (current-continuation-marks)))]

    [(cons 'err message)
     (cond
       [(string-prefix? message "ERR")
        (raise (exn:fail:redis (substring message 4) (current-continuation-marks)))]

       [(string-prefix? message "WRONGTYPE")
        (raise (exn:fail:redis (substring message 10) (current-continuation-marks)))]

       [else
        (raise (exn:fail:redis message (current-continuation-marks)))])]

    [response response]))

(define (call-with-redis client proc)
  (call-with-semaphore (redis-mutex client)
    (lambda _
      (proc client))))

(define (redis-emit! client command . args)
  ;; TODO: reconnect here if disconnected.
  (call-with-redis client
    (lambda _
      (send-request! client command args)
      (take-response! client))))

(begin-for-syntax
  (define non-alpha-re #rx"[^a-z]+")

  (define (stx->command stx)
    (datum->syntax stx (string-upcase (regexp-replace* non-alpha-re (symbol->string (syntax->datum stx)) ""))))

  (define-syntax-class arg
    (pattern name:id
             #:with e #'name
             #:with contract #'any/c)

    (pattern [name:id contract:expr]
             #:with e #'name)

    (pattern [name:id contract:expr #:converter converter:expr]
             #:with e #'(converter name))))

(define-syntax (define/contract/provide stx)
  (syntax-parse stx
    ([_ (name:id arg ...) ctr:expr e ...+]
     #'(begin
         (define/contract (name arg ...) ctr e ...)
         (provide name)))))

(define-syntax (define-simple-command stx)
  (syntax-parse stx
    ([_ (name:id arg:arg ...)
        (~optional (~seq #:command-name command-name:expr) #:defaults ([command-name (stx->command #'name)]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'any/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(define/contract/provide (fn-name client arg.name ...)
           (-> redis? arg.contract ... res-contract)

           (define res-name
             (redis-emit! client command-name arg.e ...))

           e ...)))))

(define-syntax (define-variadic-command stx)
  (syntax-parse stx
    ([_ (name:id arg:arg ... . vararg:arg)
        (~optional (~seq #:command-name command-name:expr) #:defaults ([command-name (stx->command #'name)]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'any/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(begin
           (define/contract (fn-name client arg.name ... . vararg.name)
             (->* (redis? arg.contract ...)
                  #:rest (listof vararg.contract)
                  res-contract)

             (define res-name
               (apply redis-emit! client command-name arg.e ... vararg.e))

             e ...)

           (provide fn-name))))))

(define-syntax-rule (define-simple-command/ok e0 e ...)
  (define-simple-command e0 e ...
    #:result-contract boolean?
    #:result-name res
    (ok? res)))

(define (ok? v)
  (equal? v "OK"))


;; commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: BITFIELD
;; TODO: BITOP
;; TODO: BITPOS
;; TODO: BLPOP
;; TODO: BRPOP
;; TODO: BRPOPLPUSH
;; TODO: BZPOPMAX
;; TODO: BZPOPMIN
;; TODO: CLIENT KILL
;; TODO: CLIENT LIST
;; TODO: CLIENT PAUSE
;; TODO: CLIENT REPLY
;; TODO: CLIENT UNBLOCK
;; TODO: CLUSTER ADDSLOTS
;; TODO: CLUSTER COUNT-FAILURE-REPORTS
;; TODO: CLUSTER COUNTKEYSINSLOT
;; TODO: CLUSTER DELSLOTS
;; TODO: CLUSTER FAILOVER
;; TODO: CLUSTER FORGET
;; TODO: CLUSTER GETKEYSINSLOT
;; TODO: CLUSTER INFO
;; TODO: CLUSTER KEYSLOT
;; TODO: CLUSTER MEET
;; TODO: CLUSTER NODES
;; TODO: CLUSTER REPLICAS
;; TODO: CLUSTER REPLICATE
;; TODO: CLUSTER RESET
;; TODO: CLUSTER SAVECONFIG
;; TODO: CLUSTER SET-CONFIG-EPOCH
;; TODO: CLUSTER SETSLOT
;; TODO: CLUSTER SLAVES
;; TODO: CLUSTER SLOTS
;; TODO: COMMAND
;; TODO: COMMAND COUNT
;; TODO: COMMAND GETKEYS
;; TODO: COMMAND INFO
;; TODO: CONFIG GET
;; TODO: CONFIG RESETSTAT
;; TODO: CONFIG REWRITE
;; TODO: CONFIG SET
;; TODO: DEBUG OBJECT
;; TODO: DEBUG SEGFAULT
;; TODO: DECR
;; TODO: DECRBY
;; TODO: DISCARD
;; TODO: DUMP
;; TODO: ECHO
;; TODO: EVAL
;; TODO: EVALSHA
;; TODO: EXEC
;; TODO: EXPIRE
;; TODO: EXPIREAT
;; TODO: GEOADD
;; TODO: GEODIST
;; TODO: GEOHASH
;; TODO: GEOPOS
;; TODO: GEORADIUS
;; TODO: GEORADIUSBYMEMBER
;; TODO: GETBIT
;; TODO: GETRANGE
;; TODO: GETSET
;; TODO: HDEL
;; TODO: HEXISTS
;; TODO: HGET
;; TODO: HGETALL
;; TODO: HINCRBY
;; TODO: HINCRBYFLOAT
;; TODO: HKEYS
;; TODO: HLEN
;; TODO: HMGET
;; TODO: HMSET
;; TODO: HSCAN
;; TODO: HSET
;; TODO: HSETNX
;; TODO: HSTRLEN
;; TODO: HVALS
;; TODO: INCR
;; TODO: INCRBY
;; TODO: INCRBYFLOAT
;; TODO: INFO
;; TODO: KEYS
;; TODO: LASTSAVE
;; TODO: LINDEX
;; TODO: LINSERT
;; TODO: LLEN
;; TODO: LPOP
;; TODO: LPUSH
;; TODO: LPUSHX
;; TODO: LRANGE
;; TODO: LREM
;; TODO: LSET
;; TODO: LTRIM
;; TODO: MEMORY DOCTOR
;; TODO: MEMORY HELP
;; TODO: MEMORY MALLOC-STATS
;; TODO: MEMORY PURGE
;; TODO: MEMORY STATS
;; TODO: MEMORY USAGE
;; TODO: MGET
;; TODO: MIGRATE
;; TODO: MONITOR
;; TODO: MOVE
;; TODO: MSET
;; TODO: MSETNX
;; TODO: MULTI
;; TODO: OBJECT
;; TODO: PERSIST
;; TODO: PEXPIRE
;; TODO: PEXPIREAT
;; TODO: PFADD
;; TODO: PFCOUNT
;; TODO: PFMERGE
;; TODO: PING
;; TODO: PSETEX
;; TODO: PSUBSCRIBE
;; TODO: PTTL
;; TODO: PUBLISH
;; TODO: PUBSUB
;; TODO: PUNSUBSCRIBE
;; TODO: RANDOMKEY
;; TODO: READONLY
;; TODO: READWRITE
;; TODO: RENAME
;; TODO: RENAMENX
;; TODO: REPLICAOF
;; TODO: RESTORE
;; TODO: ROLE
;; TODO: RPOP
;; TODO: RPOPLPUSH
;; TODO: RPUSH
;; TODO: RPUSHX
;; TODO: SADD
;; TODO: SAVE
;; TODO: SCAN
;; TODO: SCARD
;; TODO: SCRIPT DEBUG
;; TODO: SCRIPT EXISTS
;; TODO: SCRIPT FLUSH
;; TODO: SCRIPT KILL
;; TODO: SCRIPT LOAD
;; TODO: SDIFF
;; TODO: SDIFFSTORE
;; TODO: SETBIT
;; TODO: SETEX
;; TODO: SETNX
;; TODO: SETRANGE
;; TODO: SHUTDOWN
;; TODO: SINTER
;; TODO: SINTERSTORE
;; TODO: SISMEMBER
;; TODO: SLAVEOF
;; TODO: SLOWLOG
;; TODO: SMEMBERS
;; TODO: SMOVE
;; TODO: SORT
;; TODO: SPOP
;; TODO: SRANDMEMBER
;; TODO: SREM
;; TODO: SSCAN
;; TODO: STRLEN
;; TODO: SUBSCRIBE
;; TODO: SUNION
;; TODO: SUNIONSTORE
;; TODO: SWAPDB
;; TODO: SYNC
;; TODO: TIME
;; TODO: TOUCH
;; TODO: TTL
;; TODO: TYPE
;; TODO: UNLINK
;; TODO: UNSUBSCRIBE
;; TODO: UNWATCH
;; TODO: WAIT
;; TODO: WATCH
;; TODO: XACK
;; TODO: XADD
;; TODO: XCLAIM
;; TODO: XDEL
;; TODO: XGROUP
;; TODO: XINFO
;; TODO: XLEN
;; TODO: XPENDING
;; TODO: XRANGE
;; TODO: XREAD
;; TODO: XREADGROUP
;; TODO: XREVRANGE
;; TODO: XTRIM
;; TODO: ZADD
;; TODO: ZCARD
;; TODO: ZCOUNT
;; TODO: ZINCRBY
;; TODO: ZINTERSTORE
;; TODO: ZLEXCOUNT
;; TODO: ZPOPMAX
;; TODO: ZPOPMIN
;; TODO: ZRANGE
;; TODO: ZRANGEBYLEX
;; TODO: ZRANGEBYSCORE
;; TODO: ZRANK
;; TODO: ZREM
;; TODO: ZREMRANGEBYLEX
;; TODO: ZREMRANGEBYRANK
;; TODO: ZREMRANGEBYSCORE
;; TODO: ZREVRANGE
;; TODO: ZREVRANGEBYLEX
;; TODO: ZREVRANGEBYSCORE
;; TODO: ZREVRANK
;; TODO: ZSCAN
;; TODO: ZSCORE
;; TODO: ZUNIONSTORE

;; APPEND key value
(define-simple-command (append! [key string?] [value string?])
  #:result-contract exact-nonnegative-integer?)

;; AUTH password
(define-simple-command (auth! [password string?])
  #:result-contract string?)

;; BGREWRITEAOF
(define-simple-command/ok (bg-rewrite-aof!))

;; BGSAVE
(define-simple-command/ok (bg-save!))

;; BITCOUNT key [start end]
(define/contract/provide (redis-bitcount client key
                                         #:start [start 0]
                                         #:end [end -1])
  (->* (redis? string?)
       (#:start exact-integer?
        #:end exact-integer?)
       exact-nonnegative-integer?)
  (redis-emit! client "BITCOUNT" key (number->string start) (number->string end)))

;; CLIENT ID
(define/contract/provide (redis-client-id client)
  (-> redis? exact-integer?)
  (redis-emit! client "CLIENT" "ID"))

;; CLIENT GETNAME
(define/contract/provide (redis-client-name client)
  (-> redis? string?)
  (bytes->string/utf-8 (redis-emit! client "CLIENT" "GETNAME")))

;; CLIENT SETNAME connection-name
(define/contract/provide (redis-set-client-name! client name)
  (-> redis? string? boolean?)
  (ok? (redis-emit! client "CLIENT" "SETNAME" name)))

;; DBSIZE
(define-simple-command (count)
  #:command-name "DBSIZE"
  #:result-contract exact-integer?)

;; DEL key [key ...]
(define-variadic-command (remove! [key0 string?] . [keys string?])
  #:command-name "DEL"
  #:result-contract exact-nonnegative-integer?)

;; EXISTS key [key ...]
(define-simple-command (has-key? [key string?])
  #:command-name "EXISTS"
  #:result-contract boolean?
  #:result-name res
  (equal? res (list 1)))

(define-variadic-command (count-keys . [key string?])
  #:command-name "EXISTS"
  #:result-contract exact-nonnegative-integer?)

;; ECHO message
(define-simple-command (echo [message string?])
  #:result-contract string?
  #:result-name res
  (bytes->string/utf-8 res))

;; FLUSHALL
(define-simple-command/ok (flush-all!))

;; FLUSHDB
(define-simple-command/ok (flush-db!))

;; GET key
(define-simple-command (ref [key string?])
  #:command-name "GET"
  #:result-contract maybe-redis-value/c)

;; PING
(define-simple-command (ping)
  #:result-contract string?)

;; QUIT
(define/contract/provide (redis-quit! client)
  (-> redis? void?)
  (send-request! client "QUIT")
  (redis-disconnect! client))

;; SELECT db
(define-simple-command/ok (select! [db (integer-in 0 15) #:converter number->string]))

;; SET key value [EX seconds | PX milliseconds] [NX|XX]
(define/contract/provide (redis-set! client key value
                                     #:expires-in [expires-in #f]
                                     #:unless-exists? [unless-exists? #f]
                                     #:when-exists? [when-exists? #f])
  (->* (redis? string? string?)
       (#:expires-in (or/c false/c exact-positive-integer?)
        #:unless-exists? boolean?
        #:when-exists? boolean?)
       boolean?)
  (ok? (apply redis-emit!
              client
              "SET" key value
              (flatten (list (if expires-in
                                 (list "PX" expires-in)
                                 (list))
                             (if unless-exists?
                                 (list "NX")
                                 (if when-exists?
                                     (list "XX")
                                     (list))))))))


(module+ test
  (require rackunit)

  (define client
    (make-redis #:timeout 0.1))

  (define-syntax-rule (test message e0 e ...)
    (test-case message
      (redis-flush-all! client)
      e0 e ...))

  (check-true (redis-select! client 0))
  (check-true (redis-flush-all! client))

  (check-equal? (redis-echo client "hello") "hello")
  (check-equal? (redis-ping client) "PONG")

  (test "AUTH"
    (check-exn
     (lambda (e)
       (and (exn:fail:redis? e)
            (check-equal? (exn-message e) "Client sent AUTH, but no password is set")))
     (lambda _
       (redis-auth! client "hunter2"))))

  (test "APPEND"
    (check-equal? (redis-append! client "a" "hello") 5)
    (check-equal? (redis-append! client "a" "world!") 11))

  (test "BITCOUNT"
    (check-equal? (redis-bitcount client "a") 0)
    (check-true (redis-set! client "a" "hello"))
    (check-equal? (redis-bitcount client "a") 21))

  (test "CLIENT *"
    (check-not-false (redis-client-id client))
    (check-equal? (redis-client-name client) "racket-redis")
    (check-true (redis-set-client-name! client "custom-name"))
    (check-equal? (redis-client-name client) "custom-name"))

  (test "DBSIZE"
    (check-equal? (redis-count client) 0)
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-count client) 1))

  (test "DEL"
    (check-equal? (redis-remove! client "a") 0)
    (check-equal? (redis-remove! client "a" "b") 0)
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-remove! client "a" "b") 1)
    (check-true (redis-set! client "a" "1"))
    (check-true (redis-set! client "b" "2"))
    (check-equal? (redis-remove! client "a" "b") 2))

  (test "GET and SET"
    (check-false (redis-has-key? client "a"))
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-ref client "a") #"1")
    (check-false (redis-set! client "a" "2" #:unless-exists? #t))
    (check-equal? (redis-ref client "a") #"1")
    (check-false (redis-set! client "b" "2" #:when-exists? #t))
    (check-false (redis-has-key? client "b")))

  (check-equal? (redis-quit! client) (void)))
