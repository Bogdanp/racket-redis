#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/contract
         racket/dict
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
 redis-disconnect!

 redis-value/c
 redis-null?
 redis-null)

(struct redis (host port timeout in out response-ch response-reader)
  #:mutable)

(define/contract (make-redis #:client-name [client-name "racket-redis"]
                             #:host [host "127.0.0.1"]
                             #:port [port 6379]
                             #:timeout [timeout 5]
                             #:db [db 0]
                             #:password [password #f])
  (->* ()
       (#:client-name non-empty-string?
        #:host non-empty-string?
        #:port (integer-in 0 65536)
        #:timeout exact-nonnegative-integer?
        #:db (integer-in 0 16)
        #:password (or/c false/c non-empty-string?))
       redis?)

  (define client (redis host port timeout #f #f #f #f))
  (begin0 client
    (redis-connect! client)
    (when password (redis-auth! client password))
    (redis-select-db! client db)
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

  (define-values (in out)
    (tcp-connect (redis-host client)
                 (redis-port client)))

  (set-redis-in! client in)
  (set-redis-out! client out)

  (define response-ch (make-channel))
  (define response-reader
    (thread (make-response-reader in response-ch)))

  (set-redis-response-ch! client response-ch)
  (set-redis-response-reader! client response-reader))

(define/contract (redis-disconnect! client)
  (-> redis? void?)
  (kill-thread (redis-response-reader client))
  (tcp-abandon-port (redis-in client))
  (tcp-abandon-port (redis-out client)))

(define ((make-response-reader in ->out))
  (let loop ()
    (channel-put ->out (redis-read in))
    (loop)))

(define (send-request! client cmd [args null])
  (parameterize ([current-output-port (redis-out client)])
    (redis-write (cons cmd args))
    (flush-output)))

(define (take-response! client timeout)
  (match (sync/timeout timeout (redis-response-ch client))
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

(define (redis-emit! client command
                     #:timeout [timeout (redis-timeout client)]
                     . args)
  ;; TODO: reconnect here if disconnected.
  (send-request! client command args)
  (take-response! client timeout))

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
    ([_ (name:id . args) ctr:expr e ...+]
     #'(begin
         (define/contract (name . args) ctr e ...)
         (provide name)))

    ([_ name:id ctr:expr e ...+]
     #'(begin
         (define/contract name ctr e ...)
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
       #'(define/contract/provide (fn-name client arg.name ... . vararg.name)
           (->* (redis? arg.contract ...)
                #:rest (listof vararg.contract)
                res-contract)

           (define res-name
             (apply redis-emit! client command-name arg.e ... vararg.e))

           e ...)))))

(define-syntax-rule (define-simple-command/ok e0 e ...)
  (define-simple-command e0 e ...
    #:result-contract boolean?
    #:result-name res
    (ok? res)))

(define (ok? v)
  (equal? v "OK"))

(define-syntax-rule (define-simple-command/1 e0 e ...)
  (define-simple-command e0 e ...
    #:result-contract boolean?
    #:result-name res
    (= res 1)))


;; commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define redis-string?
  (or/c bytes? string?))

;; APPEND key value
(define-simple-command (bytes-append! [key string?] [value redis-string?])
  #:command-name "APPEND"
  #:result-contract exact-nonnegative-integer?)

;; AUTH password
(define-simple-command (auth! [password string?])
  #:result-contract string?)

;; BGREWRITEAOF
(define-simple-command/ok (bg-rewrite-aof!))

;; BGSAVE
(define-simple-command/ok (bg-save!))

;; BITCOUNT key [start stop]
(define/contract/provide (redis-bytes-bitcount client key
                                                #:start [start 0]
                                                #:stop [stop -1])
  (->* (redis? string?)
       (#:start exact-integer?
        #:stop exact-integer?)
       exact-nonnegative-integer?)
  (redis-emit! client "BITCOUNT" key (number->string start) (number->string stop)))

;; BITOP AND destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-and! client dest src0 . srcs)
  (-> redis? string? string? string? ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "AND" dest src0 srcs))

;; BITOP NOT destkey key
(define/contract/provide (redis-bytes-bitwise-not! client src [dest src])
  (->* (redis? string?) (string?) exact-nonnegative-integer?)
  (redis-emit! client "BITOP" "NOT" dest src))

;; BITOP OR destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-or! client dest src0 . srcs)
  (-> redis? string? string? string? ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "OR" dest src0 srcs))

;; BITOP XOR destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-xor! client dest src0 . srcs)
  (-> redis? string? string? string? ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "XOR" dest src0 srcs))

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
(define-simple-command (key-count)
  #:command-name "DBSIZE"
  #:result-contract exact-integer?)

;; DECR key
;; DECRBY key decrement
(define/contract/provide (redis-bytes-decr! client key [n 1])
  (->* (redis? string?)
       (exact-integer?)
       exact-integer?)
  (apply redis-emit! client (cond
                              [(= n 1) (list "DECR" key)]
                              [else    (list "DECRBY" key (number->string n))])))

;; DEL key [key ...]
(define-variadic-command (remove! [key string?] . [keys string?])
  #:command-name "DEL"
  #:result-contract exact-nonnegative-integer?)

;; ECHO message
(define-simple-command (echo [message string?])
  #:result-contract string?
  #:result-name res
  (bytes->string/utf-8 res))

;; EVAL script numkeys [key ...] [arg ...]
(define/contract/provide (redis-script-eval! client script
                                             #:keys [keys null]
                                             #:args [args null])
  (->* (redis? string?)
       (#:keys (listof string?)
        #:args (listof string?))
       redis-value/c)
  (apply redis-emit! client "EVAL" script (number->string (length keys)) (append keys args)))

;; EVALSHA sha1 numkeys [key ...] [arg ...]
(define/contract/provide (redis-script-eval-sha! client script-sha1
                                                 #:keys [keys null]
                                                 #:args [args null])
  (->* (redis? string?)
       (#:keys (listof string?)
        #:args (listof string?))
       redis-value/c)
  (apply redis-emit! client "EVALSHA" script-sha1 (number->string (length keys)) (append keys args)))

;; EXISTS key [key ...]
(define-simple-command/1 (has-key? [key string?])
  #:command-name "EXISTS")

(define-variadic-command (count-keys . [key string?])
  #:command-name "EXISTS"
  #:result-contract exact-nonnegative-integer?)

;; FLUSHALL
(define-simple-command/ok (flush-all!))

;; FLUSHDB
(define-simple-command/ok (flush-db!))

;; GET key
;; MGET key [key ...]
(define/contract/provide (redis-bytes-get client key . keys)
  (-> redis? string? string? ... redis-value/c)
  (if (null? keys)
      (redis-emit! client "GET" key)
      (apply redis-emit! client "MGET" key keys)))

;; HDEL key field [field ...]
(define-variadic-command (hash-remove! [key string?] [fld redis-string?] . [flds redis-string?])
  #:command-name "HDEL"
  #:result-contract exact-nonnegative-integer?)

;; HEXISTS key field
(define-simple-command/1 (hash-has-key? [key string?] [fld redis-string?])
  #:command-name "HEXISTS")

;; HGET key field
;; HGETALL key
;; HMGET key field [field ...]
(define/contract/provide redis-hash-get
  (case->
   (-> redis? string? redis-string? redis-value/c)
   (-> redis? string? hash?)
   (-> redis? string? #:rest (listof redis-string?) hash?))
  (case-lambda
    [(client key f)
     (redis-emit! client "HGET" key f)]

    [(client key)
     (apply hash (redis-emit! client "HGETALL" key))]

    [(client key f . fs)
     (apply hash (for/fold ([items null])
                           ([name (in-list (cons f fs))]
                            [value (in-list (apply redis-emit! client "HMGET" key f fs))])
                   (cons (if (string? name)
                             (string->bytes/utf-8 name)
                             name)
                         (cons value items))))]))

;; HKEYS key
(define-simple-command (hash-keys [key string?])
  #:command-name "HKEYS"
  #:result-contract (listof bytes?))

;; HLEN key
(define-simple-command (hash-length [key string?])
  #:command-name "HLEN"
  #:result-contract exact-nonnegative-integer?)

;; HSET key field value
;; HMSET key field value [field value ...]
(define/contract/provide redis-hash-set!
  (case->
   (-> redis? string? redis-string? redis-string? boolean?)
   (-> redis? string? redis-string? redis-string? #:rest redis-string? boolean?)
   (-> redis? string? dict? boolean?))
  (case-lambda
    [(client key f v)
     (= 1 (redis-emit! client "HSET" key f v))]

    [(client key f v . fvs)
     (unless (even? (length fvs))
       (raise-argument-error 'redis-hash-set! "an even number of fields and values" fvs))

     (ok? (apply redis-emit! client "HMSET" key f v fvs))]

    [(client key d)
     (ok? (apply redis-emit! client "HMSET" key (for/fold ([fields null])
                                                          ([(k v) (in-dict d)])
                                                  (cons k (cons v fields)))))]))

;; HVALS key
(define-simple-command (hash-values [key string?])
  #:command-name "HVALS"
  #:result-contract (listof bytes?))

;; INCR key
;; INCRBY key increment
;; INCRBYFLOAT key increment
(define/contract/provide (redis-bytes-incr! client key [n 1])
  (->* (redis? string?)
       ((or/c exact-integer? rational?))
       (or/c string? exact-integer?))
  (define res
    (apply redis-emit! client (cond
                                [(= n 1)            (list "INCR" key)]
                                [(exact-integer? n) (list "INCRBY" key (number->string n))]
                                [else               (list "INCRBYFLOAT" key (number->string n))])))

  (if (bytes? res)
      (bytes->string/utf-8 res)
      res))

;; KEYS pattern
(define-simple-command (keys [pattern redis-string?])
  #:result-contract (listof string?)
  #:result-name res
  (map bytes->string/utf-8 res))

;; LINDEX key index
(define-simple-command (list-ref [key string?] [index exact-integer? #:converter number->string])
  #:command-name "LINDEX"
  #:result-contract redis-value/c)

;; LINSERT key AFTER pivot value
;; LINSERT key BEFORE pivot value
(define/contract/provide (redis-list-insert! client key value
                                             #:after [pivot/after #f]
                                             #:before [pivot/before #f])
  (->i ([client redis?]
        [key string?]
        [value redis-string?])
       (#:after [pivot/after redis-string?]
        #:before [pivot/before redis-string?])

       #:pre/name (pivot/after pivot/before)
       "either after or before, but not both"
       (or (and pivot/after (unsupplied-arg? pivot/before))
           (and pivot/before (unsupplied-arg? pivot/after)))

       [result (or/c false/c exact-nonnegative-integer?)])
  (define args
    (if pivot/after
        (list "AFTER"  pivot/after  value)
        (list "BEFORE" pivot/before value)))

  (define res (apply redis-emit! client "LINSERT" key args))
  (and (not (= res -1)) res))

;; LLEN key
(define-simple-command (list-length [key string?])
  #:command-name "LLEN"
  #:result-contract exact-nonnegative-integer?)

;; LPOP key
;; BLPOP key [key ...] timeout
(define/contract/provide (redis-list-pop-left! client key
                                               #:block? [block? #f]
                                               #:timeout [timeout 0]
                                               . keys)
  (->i ([client redis?]
        [key string?])
       (#:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof string?)]

       #:pre/name (keys block?)
       "a list of keys may only be provided if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (block? timeout)
       "a timeout may only be provided if block? is #t"
       (or (unsupplied-arg? timeout) (equal? block? #t))

       [result redis-value/c])
  (if block?
      (apply redis-emit! client "BLPOP" (append (cons key keys) (list (number->string timeout)))
             ;; This command's timeout must take precedence over the
             ;; client's, otherwise we risk timing out before the server
             ;; does and that can leave the connection in a bad state.
             #:timeout (if (> timeout 0) (add1 timeout) #f))
      (redis-emit! client "LPOP" key)))

;; LPUSH key value [value ...]
(define-variadic-command (list-prepend! [key string?] . [value redis-string?])
  #:command-name "LPUSH"
  #:result-contract exact-nonnegative-integer?)

;; LRANGE key value start stop
(define/contract/provide (redis-sublist client key
                                        #:start [start 0]
                                        #:stop [stop -1])
  (->* (redis? string?)
       (#:start exact-integer?
        #:stop exact-integer?)
       redis-value/c)
  (redis-emit! client "LRANGE" key (number->string start) (number->string stop)))

(define/contract/provide (redis-list-get client key)
  (-> redis? string? redis-value/c)
  (redis-sublist client key))

;; LREM key count value
(define-simple-command (list-remove! [key string?]
                                     [count exact-integer?]
                                     [value redis-string?])
  #:command-name "LREM"
  #:result-contract exact-nonnegative-integer?)

;; LSET key index value
(define-simple-command/ok (list-set! [key string?] [index exact-integer?] [value redis-string?])
  #:command-name "LSET")

;; LTRIM key value start stop
(define/contract/provide (redis-list-trim! client key
                                           #:start [start 0]
                                           #:stop [stop -1])
  (->* (redis? string?)
       (#:start exact-integer?
        #:stop exact-integer?)
       boolean?)
  (ok? (redis-emit! client "LTRIM" key (number->string start) (number->string stop))))

;; MOVE key db
(define-simple-command/1 (move-key! [key string?] [db (integer-in 0 16) #:converter number->string])
  #:command-name "MOVE")

;; PERSIST key
(define-simple-command/1 (persist! [key string?]))

;; PEXPIRE key milliseconds
(define-simple-command/1 (expire-in! [key string?] [ms exact-nonnegative-integer? #:converter number->string])
  #:command-name "PEXPIRE")

;; PEXPIREAT key milliseconds-timestamp
(define-simple-command/1 (expire-at! [key string?] [ms exact-nonnegative-integer? #:converter number->string])
  #:command-name "PEXPIREAT")

;; PFADD key elt [elt ...]
(define-variadic-command (hll-add! [key string?] [elt redis-string?] . [elts redis-string?])
  #:command-name "PFADD"
  #:result-contract boolean?
  #:result-name res
  (= res 1))

;; PFCOUNT key [key ...]
(define-variadic-command (hll-count [key string?] . [keys string?])
  #:command-name "PFCOUNT"
  #:result-contract exact-nonnegative-integer?)

;; PFMERGE dest key [key ...]
(define-variadic-command (hll-merge! [dest string?] [key string?] . [keys string?])
  #:command-name "PFMERGE"
  #:result-contract boolean?
  #:result-name res
  (ok? res))

;; PTTL key
(define-simple-command (key-ttl [key string?])
  #:command-name "PTTL"
  #:result-contract (or/c 'missing 'persisted exact-nonnegative-integer?)
  #:result-name res
  (case res
    [(-2) 'missing]
    [(-1) 'persisted]
    [else res]))

;; PING
(define-simple-command (ping)
  #:result-contract string?)

;; QUIT
(define/contract/provide (redis-quit! client)
  (-> redis? void?)
  (send-request! client "QUIT")
  (redis-disconnect! client))

;; RANDOMKEY
(define-simple-command (random-key))

;; RENAME{,NX} key newkey
(define/contract/provide (redis-rename! client src dest
                                        #:unless-exists? [unless-exists? #f])
  (->* (redis? string? string?)
       (#:unless-exists? boolean?)
       boolean?)
  (ok? (redis-emit! client
                    (if unless-exists?
                        "RENAMENX"
                        "RENAME")
                    src
                    dest)))

;; BRPOP key [key ...] timeout
;; BRPOPLPUSH src dest timeout
;; RPOP key
;; RPOPLPUSH src dest
(define/contract/provide (redis-list-pop-right! client key
                                                #:dest [dest #f]
                                                #:block? [block? #f]
                                                #:timeout [timeout 0]
                                                . keys)
  (->i ([client redis?]
        [key string?])
       (#:dest [dest string?]
        #:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof string?)]

       #:pre/name (keys block?)
       "a list of keys may only be provided if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (keys dest)
       "dest and multiple keys are incompatible"
       (or (unsupplied-arg? dest) (null? keys))

       #:pre/name (block? timeout)
       "a timeout may only be provided if block? is #t"
       (or (unsupplied-arg? timeout) (equal? block? #t))

       [result redis-value/c])
  ;; This command's timeout must take precedence over the client's,
  ;; otherwise we risk timing out before the server does and that can
  ;; leave the connection in a bad state.
  (define timeout/read (if (> timeout 0) (add1 timeout) #f))
  (define timeout:str (number->string timeout))

  (cond
    [(and dest block?)
     (redis-emit! #:timeout timeout/read
                  client "BRPOPLPUSH" key dest timeout:str)]

    [dest
     (redis-emit! client "RPOPLPUSH" key dest)]

    [block?
     (apply redis-emit!
            #:timeout timeout/read
            client "BRPOP" (append (cons key keys) (list timeout:str)))]

    [else
     (redis-emit! client "RPOP" key)]))

;; RPUSH key value [value ...]
(define-variadic-command (list-append! [key string?] . [value redis-string?])
  #:command-name "RPUSH"
  #:result-contract exact-nonnegative-integer?)

;; SCRIPT EXISTS sha1 [sha1 ...]
(define/contract/provide (redis-scripts-exist? client . shas)
  (-> redis? string? ... (listof boolean?))
  (for/list ([one-or-zero (in-list (apply redis-emit! client "SCRIPT" "EXISTS" shas))])
    (= 1 one-or-zero)))

;; SCRIPT FLUSH
(define/contract/provide (redis-scripts-flush! client)
  (-> redis? boolean?)
  (ok? (redis-emit! client "SCRIPT" "FLUSH")))

;; SCRIPT KILL
(define/contract/provide (redis-script-kill! client)
  (-> redis? boolean?)
  (ok? (redis-emit! client "SCRIPT" "KILL")))

;; SCRIPT LOAD
(define/contract/provide (redis-script-load! client script)
  (-> redis? string? string?)
  (bytes->string/utf-8 (redis-emit! client "SCRIPT" "LOAD" script)))

;; SELECT db
(define-simple-command/ok (select-db! [db (integer-in 0 16) #:converter number->string])
  #:command-name "SELECT")

;; SET key value [EX seconds | PX milliseconds] [NX|XX]
(define/contract/provide (redis-bytes-set! client key value
                                           #:expires-in [expires-in #f]
                                           #:unless-exists? [unless-exists? #f]
                                           #:when-exists? [when-exists? #f])
  (->* (redis? string? redis-string?)
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

;; SWAPDB a b
(define-simple-command/ok (swap-dbs! [a (integer-in 0 16) #:converter number->string]
                                     [b (integer-in 0 16) #:converter number->string])
  #:command-name "SWAPDB")

;; TIME
(define-simple-command (time)
  #:result-contract real?
  #:result-name res
  (define-values (seconds micros)
    (apply values (map (compose1 string->number bytes->string/utf-8) res)))
  (real->double-flonum (+ (* seconds 1000) (/ micros 1000))))

;; TOUCH key [key ...]
(define-variadic-command (touch! [key string?] . [keys string?])
  #:result-contract exact-nonnegative-integer?)

;; TYPE key
(define-simple-command (key-type [key string?])
  #:command-name "TYPE"
  #:result-contract (or/c 'none 'string 'list 'set 'zset 'hash 'stream)
  #:result-name res
  (string->symbol res))


(module+ test
  (require rackunit)

  (define client
    (make-redis #:host (or (getenv "REDIS_HOST") "127.0.0.1")))

  (define-syntax-rule (test message e0 e ...)
    (test-case message
      (redis-flush-all! client)
      e0 e ...))

  (check-true (redis-select-db! client 0))
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
    (check-equal? (redis-bytes-append! client "a" "hello") 5)
    (check-equal? (redis-bytes-append! client "a" "world!") 11))

  (test "BITCOUNT"
    (check-equal? (redis-bytes-bitcount client "a") 0)
    (check-true (redis-bytes-set! client "a" "hello"))
    (check-equal? (redis-bytes-bitcount client "a") 21))

  (test "BITOP"
    (redis-bytes-set! client "a" "hello")
    (check-equal? (redis-bytes-bitwise-not! client "a") 5)
    (check-equal? (redis-bytes-get client "a") #"\227\232\223\223\220")
    (redis-bytes-set! client "a" #"\xFF")
    (redis-bytes-set! client "b" #"\x00")
    (redis-bytes-bitwise-and! client "c" "a" "b")
    (check-equal? (redis-bytes-get client "c") #"\x00"))

  (test "CLIENT *"
    (check-not-false (redis-client-id client))
    (check-equal? (redis-client-name client) "racket-redis")
    (check-true (redis-set-client-name! client "custom-name"))
    (check-equal? (redis-client-name client) "custom-name"))

  (test "DBSIZE"
    (check-equal? (redis-key-count client) 0)
    (check-true (redis-bytes-set! client "a" "1"))
    (check-equal? (redis-key-count client) 1))

  (test "DECR and DECRBY"
    (check-equal? (redis-bytes-decr! client "a") -1)
    (check-equal? (redis-bytes-decr! client "a") -2)
    (check-equal? (redis-bytes-decr! client "a" 3) -5)
    (check-equal? (redis-key-type client "a") 'string)

    (check-true (redis-bytes-set! client "a" "1.5"))
    (check-exn
     (lambda (e)
       (and (exn:fail:redis? e)
            (check-equal? (exn-message e) "value is not an integer or out of range")))
     (lambda _
       (redis-bytes-decr! client "a"))))

  (test "DEL"
    (check-equal? (redis-remove! client "a") 0)
    (check-equal? (redis-remove! client "a" "b") 0)
    (check-true (redis-bytes-set! client "a" "1"))
    (check-equal? (redis-remove! client "a" "b") 1)
    (check-true (redis-bytes-set! client "a" "1"))
    (check-true (redis-bytes-set! client "b" "2"))
    (check-equal? (redis-remove! client "a" "b") 2))

  (test "EVAL"
    (check-equal? (redis-script-eval! client "return 1") 1)
    (check-equal? (redis-script-eval! client "return {KEYS[1], ARGV[1], ARGV[2]}"
                                      #:keys '("a")
                                      #:args '("b" "c"))
                  '(#"a" #"b" #"c")))

  (test "HEXISTS, HSET, HMSET, HGETALL, HDEL, HLEN, HKEYS, HVALS"
    (check-false (redis-hash-has-key? client "notahash" "a"))
    (check-true (redis-hash-set! client "simple-hash" "a" "1"))
    (check-true (redis-hash-has-key? client "simple-hash" "a"))
    (check-equal? (redis-hash-get client "simple-hash") (hash #"a" #"1"))
    (check-equal? (redis-hash-get client "simple-hash" "a") #"1")
    (check-equal? (redis-hash-remove! client "simple-hash" "a") 1)
    (check-equal? (redis-hash-get client "simple-hash") (hash))

    (check-true (redis-hash-set! client "alist-hash" '(("a" . "1")
                                                       ("b" . "2")
                                                       ("c" . "3"))))
    (check-equal? (redis-hash-get client "alist-hash") (hash #"a" #"1"
                                                             #"b" #"2"
                                                             #"c" #"3"))
    (check-equal? (redis-hash-get client "alist-hash" "a") #"1")
    (check-equal? (redis-hash-get client "alist-hash" "a" "b") (hash #"a" #"1"
                                                                     #"b" #"2"))
    (check-equal? (redis-hash-get client "alist-hash" "a" "d" "b") (hash #"a" #"1"
                                                                         #"b" #"2"
                                                                         #"d" (redis-null)))

    (check-equal? (redis-hash-length client "notahash") 0)
    (check-equal? (redis-hash-length client "alist-hash") 3)

    (check-equal? (redis-hash-keys client "notahash") null)
    (check-equal? (sort (redis-hash-keys client "alist-hash") bytes<?)
                  (sort'(#"a" #"b" #"c") bytes<?))

    (check-equal? (redis-hash-values client "notahash") null)
    (check-equal? (sort (redis-hash-values client "alist-hash") bytes<?)
                  (sort '(#"1" #"2" #"3") bytes<?)))

  (test "{M,}GET and SET"
    (check-false (redis-has-key? client "a"))
    (check-true (redis-bytes-set! client "a" "1"))
    (check-equal? (redis-bytes-get client "a") #"1")
    (check-false (redis-bytes-set! client "a" "2" #:unless-exists? #t))
    (check-equal? (redis-bytes-get client "a") #"1")
    (check-false (redis-bytes-set! client "b" "2" #:when-exists? #t))
    (check-false (redis-has-key? client "b"))
    (check-true (redis-bytes-set! client "b" "2" #:unless-exists? #t))
    (check-true (redis-has-key? client "b"))
    (check-equal? (redis-bytes-get client "a" "b") '(#"1" #"2")))

  (test "INCR, INCRBY and INCRBYFLOAT"
    (check-equal? (redis-bytes-incr! client "a") 1)
    (check-equal? (redis-bytes-incr! client "a") 2)
    (check-equal? (redis-bytes-incr! client "a" 3) 5)
    (check-equal? (redis-bytes-incr! client "a" 1.5) "6.5")
    (check-equal? (redis-key-type client "a") 'string))

  (test "LINDEX, LLEN, LPUSH, LPOP, BLPOP"
    (check-equal? (redis-list-length client "a") 0)
    (check-equal? (redis-list-prepend! client "a" "1") 1)
    (check-equal? (redis-list-prepend! client "a" "2" "3") 3)
    (check-equal? (redis-list-length client "a") 3)
    (check-equal? (redis-list-ref client "a" 1) #"2")
    (check-equal? (redis-list-pop-left! client "a") #"3")
    (check-equal? (redis-list-pop-left! client "a") #"2")
    (check-equal? (redis-list-pop-left! client "a") #"1")
    (check-equal? (redis-list-pop-left! client "a") (redis-null))

    (check-exn
     exn:fail:contract?
     (lambda _
       (redis-list-pop-left! client "a" "b")))

    (check-exn
     exn:fail:contract?
     (lambda _
       (redis-list-pop-left! client "a" #:timeout 10)))

    (redis-list-append! client "a" "1")
    (check-equal? (redis-list-pop-left! client "a" #:block? #t) '(#"a" #"1"))

    (redis-list-append! client "b" "2")
    (check-equal? (redis-list-pop-left! client "a" "b" #:block? #t) '(#"b" #"2")))

  (test "LINSERT"
    (check-equal? (redis-list-prepend! client "a" "1") 1)
    (check-equal? (redis-list-prepend! client "a" "2") 2)
    (check-equal? (redis-list-insert! client "a" "3" #:before "1") 3)
    (check-false (redis-list-insert! client "a" "3" #:before "8"))
    (check-equal? (redis-sublist client "a") '(#"2" #"3" #"1"))
    (check-equal? (redis-list-insert! client "a" "4" #:after "3") 4)
    (check-equal? (redis-sublist client "a") '(#"2" #"3" #"4" #"1")))

  (test "LTRIM"
    (check-equal? (redis-list-prepend! client "a" "2") 1)
    (check-equal? (redis-list-prepend! client "a" "2") 2)
    (check-true (redis-list-trim! client "a" #:start 1))
    (check-equal? (redis-sublist client "a") '(#"2")))

  (test "PERSIST, PEXPIRE and PTTL"
    (check-false (redis-expire-in! client "a" 200))
    (check-equal? (redis-key-ttl client "a") 'missing)
    (check-true (redis-bytes-set! client "a" "1"))
    (check-equal? (redis-key-ttl client "a") 'persisted)
    (check-true (redis-expire-in! client "a" 20))
    (check-true (> (redis-key-ttl client "a") 5))
    (check-true (redis-persist! client "a"))
    (check-equal? (redis-key-ttl client "a") 'persisted))

  (test "PFADD, PFCOUNT and PFMERGE"
    (check-true (redis-hll-add! client "a" "1"))
    (check-true (redis-hll-add! client "a" "2"))
    (check-false (redis-hll-add! client "a" "1"))
    (check-equal? (redis-hll-count client "a") 2)
    (check-equal? (redis-hll-count client "a" "b") 2)
    (check-true (redis-hll-add! client "b" "1"))
    (check-equal? (redis-hll-count client "a" "b") 2)
    (check-true (redis-hll-merge! client "c" "a" "b"))
    (check-equal? (redis-hll-count client "c") 2))

  (test "RENAME"
    (check-true (redis-bytes-set! client "a" "1"))
    (check-true (redis-bytes-set! client "b" "2"))
    (check-true (redis-rename! client "a" "c"))
    (check-false (redis-has-key? client "a"))
    (check-false (redis-rename! client "c" "b" #:unless-exists? #t))
    (check-true (redis-has-key? client "c")))

  (test "RPUSH, RPOP, BRPOP, BRPOPLPUSH"
    (check-equal? (redis-list-append! client "a" "1") 1)
    (check-equal? (redis-list-append! client "a" "2") 2)
    (check-equal? (redis-list-pop-right! client "a") #"2")
    (check-equal? (redis-list-pop-right! client "a") #"1")
    (check-equal? (redis-list-pop-right! client "a") (redis-null))

    (check-exn
     exn:fail:contract?
     (lambda _
       (redis-list-pop-right! client "a" "b")))

    (check-exn
     exn:fail:contract?
     (lambda _
       (redis-list-pop-right! client "a" #:timeout 10)))

    (check-exn
     exn:fail:contract?
     (lambda _
       (redis-list-pop-right! client "a" "b" #:dest "b" #:block? #t)))

    (redis-list-append! client "a" "1")
    (check-equal? (redis-list-pop-right! client "a" #:block? #t) '(#"a" #"1"))

    (redis-list-append! client "b" "2")
    (check-equal? (redis-list-pop-right! client "a" "b" #:block? #t) '(#"b" #"2"))

    (redis-list-append! client "b" "2")
    (check-equal? (redis-list-pop-right! client "b" #:dest "a") #"2")
    (check-equal? (redis-list-pop-right! client "a") #"2")

    (redis-list-append! client "b" "2")
    (check-equal? (redis-list-pop-right! client "b" #:dest "a" #:block? #t) #"2")
    (check-equal? (redis-list-pop-right! client "a") #"2"))

  (test "TOUCH"
    (check-equal? (redis-touch! client "a") 0)
    (check-equal? (redis-touch! client "a" "b") 0)
    (check-true (redis-bytes-set! client "a" "1"))
    (check-true (redis-bytes-set! client "b" "2"))
    (check-equal? (redis-touch! client "a" "b" "c") 2))

  (check-equal? (redis-quit! client) (void)))
