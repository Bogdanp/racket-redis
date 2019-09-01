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

;; contracts ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 redis-string/c
 redis-key/c
 redis-value/c)

(define redis-string/c
  (or/c bytes? string?))

(define redis-key/c
  redis-string/c)

(define redis-value/c
  (make-flat-contract
   #:name 'redis-value/c
   #:first-order (lambda (v)
                   ((or/c false/c bytes? exact-integer? (listof redis-value/c)) v))))


;; client ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 make-redis
 redis?
 redis-connected?
 redis-connect!
 redis-disconnect!)

(struct redis (host port timeout custodian in out response-ch response-reader)
  #:mutable)

(define/contract (make-redis #:client-name [client-name "racket-redis"]
                             #:host [host "127.0.0.1"]
                             #:port [port 6379]
                             #:timeout [timeout 5000]
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

  (define client (redis host port timeout #f #f #f #f #f))
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

  (define custodian (make-custodian))
  (set-redis-custodian! client custodian)

  (parameterize ([current-custodian custodian])
    (define-values (in out)
      (tcp-connect (redis-host client)
                   (redis-port client)))

    (set-redis-in! client in)
    (set-redis-out! client out)

    (define response-ch (make-channel))
    (define response-reader
      (thread (make-response-reader in response-ch)))

    (set-redis-response-ch! client response-ch)
    (set-redis-response-reader! client response-reader)))

(define/contract (redis-disconnect! client)
  (-> redis? void?)
  (custodian-shutdown-all (redis-custodian client)))

(define ((make-response-reader in ->out))
  (let loop ()
    (channel-put ->out (redis-read in))
    (loop)))

(define (send-request! client cmd [args null])
  (parameterize ([current-output-port (redis-out client)])
    (redis-write (cons cmd args))
    (flush-output)))

(define (take-response! client timeout)
  (sync
   (choice-evt
    (handle-evt
     (redis-response-ch client)
     (match-lambda
       [(cons 'err message)
        (cond
          [(string-prefix? message "ERR")
           (raise (exn:fail:redis (substring message 4) (current-continuation-marks)))]

          [(string-prefix? message "WRONGTYPE")
           (raise (exn:fail:redis (substring message 10) (current-continuation-marks)))]

          [else
           (raise (exn:fail:redis message (current-continuation-marks)))])]

       [response response]))

    (if timeout
        (handle-evt
         (alarm-evt (+ (current-inexact-milliseconds) timeout))
         (lambda _
           (raise (exn:fail:redis:timeout
                   "timed out while waiting for response from Redis"
                   (current-continuation-marks)))))
        never-evt))))

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
        (~optional (~seq #:command (command:expr ...+)) #:defaults ([(command 1) (list (stx->command #'name))]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'redis-value/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(define/contract/provide (fn-name client arg.name ...)
           (-> redis? arg.contract ... res-contract)

           (define res-name
             (redis-emit! client command ... arg.e ...))

           e ...)))))

(define-syntax (define-variadic-command stx)
  (syntax-parse stx
    ([_ (name:id arg:arg ... . vararg:arg)
        (~optional (~seq #:command (command:expr ...+)) #:defaults ([(command 1) (list (stx->command #'name))]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'redis-value/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(define/contract/provide (fn-name client arg.name ... . vararg.name)
           (->* (redis? arg.contract ...)
                #:rest (listof vararg.contract)
                res-contract)

           (define res-name
             (apply redis-emit! client command ... arg.e ... vararg.e))

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

;; APPEND key value
(define-simple-command (bytes-append! [key redis-key/c]
                                      [value redis-string/c])
  #:command ("APPEND")
  #:result-contract exact-nonnegative-integer?)

;; AUTH password
(define-simple-command/ok (auth! [password redis-string/c]))

;; BGREWRITEAOF
;; REWRITEAOF
(define-simple-command/ok (rewrite-aof/async!)
  #:command ("BGREWRITEAOF"))

(define-simple-command/ok (rewrite-aof!)
  #:command ("REWRITEAOF"))

;; BGSAVE
;; SAVE
(define-simple-command/ok (save/async!)
  #:command ("BGSAVE"))

(define-simple-command/ok (save!)
  #:command ("SAVE"))

;; BITCOUNT key [start stop]
(define/contract/provide (redis-bytes-bitcount client key
                                               #:start [start 0]
                                               #:stop [stop -1])
  (->* (redis? redis-key/c)
       (#:start exact-integer?
        #:stop exact-integer?)
       exact-nonnegative-integer?)
  (redis-emit! client "BITCOUNT" key (number->string start) (number->string stop)))

;; BITOP AND destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-and! client dest src0 . srcs)
  (-> redis? redis-key/c redis-key/c redis-key/c ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "AND" dest src0 srcs))

;; BITOP NOT destkey key
(define/contract/provide (redis-bytes-bitwise-not! client src [dest src])
  (->* (redis? redis-key/c) (redis-key/c) exact-nonnegative-integer?)
  (redis-emit! client "BITOP" "NOT" dest src))

;; BITOP OR destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-or! client dest src0 . srcs)
  (-> redis? redis-key/c redis-key/c redis-key/c ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "OR" dest src0 srcs))

;; BITOP XOR destkey key [key ...]
(define/contract/provide (redis-bytes-bitwise-xor! client dest src0 . srcs)
  (-> redis? redis-key/c redis-key/c redis-key/c ... exact-nonnegative-integer?)
  (apply redis-emit! client "BITOP" "XOR" dest src0 srcs))

;; CLIENT ID
(define/contract/provide (redis-client-id client)
  (-> redis? exact-integer?)
  (redis-emit! client "CLIENT" "ID"))

;; CLIENT GETNAME
(define/contract/provide (redis-client-name client)
  (-> redis? string?)
  (bytes->string/utf-8 (redis-emit! client "CLIENT" "GETNAME")))

;; CLIENT PAUSE timeout
(define-simple-command/ok (clients-pause! [timeout exact-nonnegative-integer? #:converter number->string])
  #:command ("CLIENT PAUSE"))

;; CLIENT SETNAME connection-name
(define/contract/provide (redis-set-client-name! client name)
  (-> redis? redis-string/c boolean?)
  (ok? (redis-emit! client "CLIENT" "SETNAME" name)))

;; COMMAND
(define-simple-command (commands)
  #:command ("COMMAND"))

;; COMMAND COUNT
(define-simple-command (command-count)
  #:command ("COMMAND" "COUNT")
  #:result-contract exact-nonnegative-integer?)

;; CONFIG GET parameter
(define-simple-command (config-ref [parameter redis-string/c])
  #:command ("CONFIG" "GET"))

;; CONFIG RESETSTAT
(define-simple-command/ok (config-reset-stats!)
  #:command ("CONFIG" "RESETSTAT"))

;; CONFIG REWRITE
(define-simple-command/ok (config-rewrite!)
  #:command ("CONFIG" "REWRITE"))

;; CONFIG SET parameter value
(define-simple-command/ok (config-set! [parameter redis-string/c] [value redis-string/c])
  #:command ("CONFIG" "SET"))

;; DBSIZE
(define-simple-command (key-count)
  #:command ("DBSIZE")
  #:result-contract exact-integer?)

;; DECR key
;; DECRBY key decrement
(define/contract/provide (redis-bytes-decr! client key [n 1])
  (->* (redis? redis-key/c)
       (exact-integer?)
       exact-integer?)
  (apply redis-emit! client (cond
                              [(= n 1) (list "DECR" key)]
                              [else    (list "DECRBY" key (number->string n))])))

;; DEL key [key ...]
;; UNLINK key [key ...]
(define/contract/provide (redis-remove! client key
                                        #:async? [async? #f]
                                        . keys)
  (->* (redis? redis-key/c)
       (#:async? boolean?)
       #:rest (listof redis-key/c)
       exact-nonnegative-integer?)
  (apply redis-emit! client (if async? "UNLINK" "DEL") key keys))

;; ECHO message
(define-simple-command (echo [message string?])
  #:result-contract string?
  #:result-name res
  (bytes->string/utf-8 res))

;; EVAL script numkeys [key ...] [arg ...]
(define/contract/provide (redis-script-eval! client script
                                             #:keys [keys null]
                                             #:args [args null])
  (->* (redis? redis-string/c)
       (#:keys (listof redis-key/c)
        #:args (listof redis-string/c))
       redis-value/c)
  (apply redis-emit! client "EVAL" script (number->string (length keys)) (append keys args)))

;; EVALSHA sha1 numkeys [key ...] [arg ...]
(define/contract/provide (redis-script-eval-sha! client script-sha1
                                                 #:keys [keys null]
                                                 #:args [args null])
  (->* (redis? redis-string/c)
       (#:keys (listof redis-key/c)
        #:args (listof redis-string/c))
       redis-value/c)
  (apply redis-emit! client "EVALSHA" script-sha1 (number->string (length keys)) (append keys args)))

;; EXISTS key [key ...]
(define-simple-command/1 (has-key? [key redis-key/c])
  #:command ("EXISTS"))

(define-variadic-command (count-keys . [key redis-key/c])
  #:command ("EXISTS")
  #:result-contract exact-nonnegative-integer?)

;; FLUSHALL
(define-simple-command/ok (flush-all!))

;; FLUSHDB
(define-simple-command/ok (flush-db!))

;; GET key
;; MGET key [key ...]
(define/contract/provide (redis-bytes-get client key . keys)
  (-> redis? redis-key/c redis-key/c ... redis-value/c)
  (if (null? keys)
      (redis-emit! client "GET" key)
      (apply redis-emit! client "MGET" key keys)))

;; HDEL key field [field ...]
(define-variadic-command (hash-remove! [key redis-key/c] [fld redis-string/c] . [flds redis-string/c])
  #:command ("HDEL")
  #:result-contract exact-nonnegative-integer?)

;; HEXISTS key field
(define-simple-command/1 (hash-has-key? [key redis-key/c] [fld redis-string/c])
  #:command ("HEXISTS"))

;; HGET key field
(define-simple-command (hash-ref [key redis-key/c] [fld redis-string/c])
  #:command ("HGET"))

;; HGETALL key
;; HMGET key field [field ...]
(define/contract/provide redis-hash-get
  (case->
   (-> redis? redis-key/c hash?)
   (-> redis? redis-key/c #:rest (listof redis-string/c) hash?))
  (case-lambda
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
(define-simple-command (hash-keys [key redis-key/c])
  #:command ("HKEYS")
  #:result-contract (listof bytes?))

;; HLEN key
(define-simple-command (hash-length [key redis-key/c])
  #:command ("HLEN")
  #:result-contract exact-nonnegative-integer?)

;; HSET key field value
;; HMSET key field value [field value ...]
(define/contract/provide redis-hash-set!
  (case->
   (-> redis? redis-key/c redis-string/c redis-string/c boolean?)
   (-> redis? redis-key/c redis-string/c redis-string/c #:rest redis-string/c boolean?)
   (-> redis? redis-key/c dict? boolean?))
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
(define-simple-command (hash-values [key redis-key/c])
  #:command ("HVALS")
  #:result-contract (listof bytes?))

;; INCR key
;; INCRBY key increment
;; INCRBYFLOAT key increment
(define/contract/provide (redis-bytes-incr! client key [n 1])
  (->* (redis? redis-key/c)
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

;; INFO
(define-simple-command (info [section redis-string/c]))

;; KEYS pattern
(define-simple-command (keys [pattern redis-string/c])
  #:result-contract (listof bytes?))

;; LASTSAVE
(define-simple-command (last-save-time)
  #:command ("LASTSAVE")
  #:result-contract exact-nonnegative-integer?)

;; LINDEX key index
(define-simple-command (list-ref [key redis-key/c]
                                 [index exact-integer? #:converter number->string])
  #:command ("LINDEX"))

;; LINSERT key AFTER pivot value
;; LINSERT key BEFORE pivot value
(define/contract/provide (redis-list-insert! client key value
                                             #:after [pivot/after #f]
                                             #:before [pivot/before #f])
  (->i ([client redis?]
        [key redis-key/c]
        [value redis-string/c])
       (#:after [pivot/after redis-string/c]
        #:before [pivot/before redis-string/c])

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
(define-simple-command (list-length [key redis-key/c])
  #:command ("LLEN")
  #:result-contract exact-nonnegative-integer?)

;; LPOP key
;; BLPOP key [key ...] timeout
(define/contract/provide (redis-list-pop-left! client key
                                               #:block? [block? #f]
                                               #:timeout [timeout 0]
                                               . keys)
  (->i ([client redis?]
        [key redis-key/c])
       (#:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof redis-key/c)]

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
(define-variadic-command (list-prepend! [key redis-key/c] . [value redis-string/c])
  #:command ("LPUSH")
  #:result-contract exact-nonnegative-integer?)

;; LRANGE key value start stop
(define/contract/provide (redis-sublist client key
                                        #:start [start 0]
                                        #:stop [stop -1])
  (->* (redis? redis-key/c)
       (#:start exact-integer?
        #:stop exact-integer?)
       redis-value/c)
  (redis-emit! client "LRANGE" key (number->string start) (number->string stop)))

(define/contract/provide (redis-list-get client key)
  (-> redis? redis-key/c redis-value/c)
  (redis-sublist client key))

;; LREM key count value
(define-simple-command (list-remove! [key redis-key/c]
                                     [count exact-integer?]
                                     [value redis-string/c])
  #:command ("LREM")
  #:result-contract exact-nonnegative-integer?)

;; LSET key index value
(define-simple-command/ok (list-set! [key redis-key/c] [index exact-integer?] [value redis-string/c])
  #:command ("LSET"))

;; LTRIM key value start stop
(define/contract/provide (redis-list-trim! client key
                                           #:start [start 0]
                                           #:stop [stop -1])
  (->* (redis? redis-key/c)
       (#:start exact-integer?
        #:stop exact-integer?)
       boolean?)
  (ok? (redis-emit! client "LTRIM" key (number->string start) (number->string stop))))

;; MOVE key db
(define-simple-command/1 (move-key! [key redis-key/c] [db (integer-in 0 16) #:converter number->string])
  #:command ("MOVE"))

;; PERSIST key
(define-simple-command/1 (persist! [key redis-key/c]))

;; PEXPIRE key milliseconds
(define-simple-command/1 (expire-in! [key redis-key/c] [ms exact-nonnegative-integer? #:converter number->string])
  #:command ("PEXPIRE"))

;; PEXPIREAT key milliseconds-timestamp
(define-simple-command/1 (expire-at! [key redis-key/c] [ms exact-nonnegative-integer? #:converter number->string])
  #:command ("PEXPIREAT"))

;; PFADD key elt [elt ...]
(define-variadic-command (hll-add! [key redis-key/c] [elt redis-string/c] . [elts redis-string/c])
  #:command ("PFADD")
  #:result-contract boolean?
  #:result-name res
  (= res 1))

;; PFCOUNT key [key ...]
(define-variadic-command (hll-count [key redis-key/c] . [keys redis-key/c])
  #:command ("PFCOUNT")
  #:result-contract exact-nonnegative-integer?)

;; PFMERGE dest key [key ...]
(define-variadic-command (hll-merge! [dest redis-key/c] [key redis-key/c] . [keys redis-key/c])
  #:command ("PFMERGE")
  #:result-contract boolean?
  #:result-name res
  (ok? res))

;; PTTL key
(define-simple-command (key-ttl [key redis-key/c])
  #:command ("PTTL")
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
(define-simple-command (random-key)
  #:result-contract (or/c false/c bytes?))

;; RENAME{,NX} key newkey
(define/contract/provide (redis-rename! client src dest
                                        #:unless-exists? [unless-exists? #f])
  (->* (redis? redis-key/c redis-key/c)
       (#:unless-exists? boolean?)
       boolean?)
  (ok? (redis-emit! client
                    (if unless-exists?
                        "RENAMENX"
                        "RENAME")
                    src
                    dest)))

;; REPLICAOF NO ONE
;; REPLICAOF host port
(define-simple-command (make-replica-of! [host redis-string/c] [port (integer-in 0 65536) #:converter number->string])
  #:command ("REPLICAOF"))

(define-simple-command (stop-replication!)
  #:command ("REPLICAOF" "NO" "ONE"))

;; ROLE
(define-simple-command (role))

;; RPOP key
;; RPOPLPUSH src dest
;; BRPOP key [key ...] timeout
;; BRPOPLPUSH src dest timeout
(define/contract/provide (redis-list-pop-right! client key
                                                #:dest [dest #f]
                                                #:block? [block? #f]
                                                #:timeout [timeout 0]
                                                . keys)
  (->i ([client redis?]
        [key redis-key/c])
       (#:dest [dest redis-key/c]
        #:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof redis-key/c)]

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
(define-variadic-command (list-append! [key redis-key/c] . [value redis-string/c])
  #:command ("RPUSH")
  #:result-contract exact-nonnegative-integer?)

;; SCRIPT EXISTS sha1 [sha1 ...]
(define/contract/provide (redis-scripts-exist? client . shas)
  (-> redis? redis-string/c ... (listof boolean?))
  (for/list ([one-or-zero (in-list (apply redis-emit! client "SCRIPT" "EXISTS" shas))])
    (= 1 one-or-zero)))

(define/contract/provide (redis-script-exists? client sha)
  (-> redis? redis-string/c boolean?)
  (car (redis-scripts-exist? client sha)))

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
  (-> redis? redis-string/c string?)
  (bytes->string/utf-8 (redis-emit! client "SCRIPT" "LOAD" script)))

;; SELECT db
(define-simple-command/ok (select-db! [db (integer-in 0 16) #:converter number->string])
  #:command ("SELECT"))

;; SET key value [EX seconds | PX milliseconds] [NX|XX]
(define/contract/provide (redis-bytes-set! client key value
                                           #:expires-in [expires-in #f]
                                           #:unless-exists? [unless-exists? #f]
                                           #:when-exists? [when-exists? #f])
  (->* (redis? redis-key/c redis-string/c)
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

;; SLOWLOG GET count
(define-simple-command (slowlog-get [count exact-positive-integer? #:converter number->string])
  #:command ("SLOWLOG" "GET"))

;; SLOWLOG LEN
(define-simple-command (slowlog-length)
  #:command ("SLOWLOG" "LEN")
  #:result-contract exact-nonnegative-integer?)

;; SLOWLOG RESET
(define-simple-command/ok (slowlog-reset!)
  #:command ("SLOWLOG" "RESET"))

;; SWAPDB a b
(define-simple-command/ok (swap-dbs! [a (integer-in 0 16) #:converter number->string]
                                     [b (integer-in 0 16) #:converter number->string])
  #:command ("SWAPDB"))

;; TIME
(define-simple-command (time)
  #:result-contract real?
  #:result-name res
  (define-values (seconds micros)
    (apply values (map (compose1 string->number bytes->string/utf-8) res)))
  (real->double-flonum (+ (* seconds 1000) (/ micros 1000))))

;; TOUCH key [key ...]
(define-variadic-command (touch! [key redis-key/c] . [keys redis-key/c])
  #:result-contract exact-nonnegative-integer?)

;; TYPE key
(define-simple-command (key-type [key redis-key/c])
  #:command ("TYPE")
  #:result-contract (or/c 'none 'string 'list 'set 'zset 'hash 'stream)
  #:result-name res
  (string->symbol res))


;; stream commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (contract-out
  [struct redis-stream-entry ([id bytes?]
                              [fields (hash/c bytes? bytes?)])]
  [struct redis-stream-entry/pending ([id bytes?]
                                      [consumer bytes?]
                                      [elapsed-time exact-nonnegative-integer?]
                                      [delivery-count exact-positive-integer?])]
  [struct redis-stream-info ([length exact-nonnegative-integer?]
                             [radix-tree-keys exact-nonnegative-integer?]
                             [radix-tree-nodes exact-nonnegative-integer?]
                             [groups exact-nonnegative-integer?]
                             [last-generated-id bytes?]
                             [first-entry redis-stream-entry?]
                             [last-entry redis-stream-entry?])]
  [struct redis-stream-group ([name bytes?]
                              [consumers exact-nonnegative-integer?]
                              [pending exact-nonnegative-integer?])]
  [struct redis-stream-consumer ([name bytes?]
                                 [idle exact-nonnegative-integer?]
                                 [pending exact-positive-integer?])]))

(struct redis-stream-entry (id fields) #:transparent)
(struct redis-stream-entry/pending (id consumer elapsed-time delivery-count) #:transparent)
(struct redis-stream-info (length radix-tree-keys radix-tree-nodes groups last-generated-id first-entry last-entry) #:transparent)
(struct redis-stream-group (name consumers pending) #:transparent)
(struct redis-stream-consumer (name idle pending) #:transparent)

;; XACK key group id [id ...]
(define-variadic-command (stream-ack! [key redis-key/c] [group redis-string/c] [id redis-string/c] . [ids redis-string/c])
  #:command ("XACK")
  #:result-contract exact-nonnegative-integer?)

;; XADD key MAXLEN ~ n id field value [field value ...]
;; XADD key MAXLEN n id field value [field value ...]
;; XADD key id field value [field value ...]
(define/contract/provide (redis-stream-add! client key fld val
                                            #:id [id "*"]
                                            #:max-length [max-length #f]
                                            #:max-length/approximate [max-length/approximate #f]
                                            . flds-and-vals)
  (->i ([client redis?]
        [key redis-key/c]
        [fld redis-string/c]
        [val redis-string/c])
       (#:id [id non-empty-string?]
        #:max-length [max-length exact-positive-integer?]
        #:max-length/approximate [max-length/approximate exact-positive-integer?])
       #:rest [flds-and-vals (listof redis-string/c)]

       #:pre/name (max-length max-length/approximate)
       "either max-length or max-length/approximate can be provided but not both"
       (or (and max-length (unsupplied-arg? max-length/approximate))
           (and max-length/approximate (unsupplied-arg? max-length)))

       #:pre/name (flds-and-vals)
       "flds-and-vals must contain an even number of field and value pairs"
       (even? (length flds-and-vals))

       [result bytes?])

  (cond
    [max-length/approximate
     (apply redis-emit! client "XADD" key "MAXLEN" "~" (number->string max-length/approximate) id fld val flds-and-vals)]

    [max-length
     (apply redis-emit! client "XADD" key "MAXLEN" (number->string max-length) id fld val flds-and-vals)]

    [else
     (apply redis-emit! client "XADD" key id fld val flds-and-vals)]))

;; XDEL key id [id ...]
(define-variadic-command (stream-remove! [key redis-key/c] [id redis-string/c] . [ids redis-string/c])
  #:command ("XDEL")
  #:result-contract exact-nonnegative-integer?)

;; XGROUP CREATE key group id
(define/contract/provide (redis-stream-group-create! client key group [starting-id "0-0"])
  (->* (redis? redis-key/c redis-string/c)
       (redis-string/c)
       boolean?)
  (ok? (redis-emit! client "XGROUP" "CREATE" key group starting-id)))

;; XGROUP DESTROY key group
(define/contract/provide (redis-stream-group-remove! client key group)
  (-> redis? redis-key/c redis-string/c boolean?)
  (ok? (redis-emit! client "XGROUP" "DESTROY" key group)))

;; XGROUP DELCONSUMER key group consumer
(define/contract/provide (redis-stream-consumer-remove! client key group consumer)
  (-> redis? redis-key/c redis-string/c redis-string/c boolean?)
  (ok? (redis-emit! client "XGROUP" "DELCONSUMER" key group consumer)))

;; XGROUP SETID key group id
(define/contract/provide (redis-stream-group-set-id! client key group id)
  (-> redis? redis-key/c redis-string/c redis-string/c boolean?)
  (ok? (redis-emit! client "XGROUP" "SETID" key group id)))

;; XINFO CONSUMERS key group
(define/contract/provide (redis-stream-consumers client key group)
  (-> redis? redis-key/c redis-string/c (listof redis-stream-consumer?))
  (for*/list ([fields (redis-emit! client "XINFO" "CONSUMERS" key)]
              [info (in-value (apply hash fields))])
    (redis-stream-consumer (hash-ref info #"name")
                           (hash-ref info #"idle")
                           (hash-ref info #"pending"))))

;; XINFO GROUPS key
(define/contract/provide (redis-stream-groups client key)
  (-> redis? redis-key/c (listof redis-stream-group?))
  (for*/list ([fields (redis-emit! client "XINFO" "GROUPS" key)]
              [info (in-value (apply hash fields))])
    (redis-stream-group (hash-ref info #"name")
                        (hash-ref info #"consumers")
                        (hash-ref info #"pending"))))

;; XINFO STREAM key
(define/contract/provide (redis-stream-get client key)
  (-> redis? redis-key/c redis-stream-info?)
  (define info
    (apply hash (redis-emit! client "XINFO" "STREAM" key)))

  (redis-stream-info (hash-ref info #"length")
                     (hash-ref info #"radix-tree-keys")
                     (hash-ref info #"radix-tree-nodes")
                     (hash-ref info #"groups")
                     (hash-ref info #"last-generated-id")
                     (pair->entry (hash-ref info #"first-entry"))
                     (pair->entry (hash-ref info #"last-entry"))))

(define (pair->entry p)
  (redis-stream-entry (car p) (apply hash (cadr p))))

;; XLEN key
(define-simple-command (stream-length [key redis-key/c])
  #:command ("XLEN")
  #:result-contract exact-nonnegative-integer?)

;; XPENDING key group [start stop count] [consumer]
(define stream-range-position/c
  (or/c 'first-entry 'last-entry redis-string/c))

(define (stream-range-position->string p)
  (match p
    ['first-entry #"-"]
    ['last-entry  #"+"]
    [p            p]))

(define/contract/provide (redis-stream-group-range client key group [consumer #f]
                                                   #:start [start 'first-entry]
                                                   #:stop [stop 'last-entry]
                                                   #:limit [limit 10])
  (->* (redis? redis-key/c redis-string/c)
       (redis-string/c
        #:start stream-range-position/c
        #:stop stream-range-position/c
        #:limit exact-positive-integer?)
       (listof redis-stream-entry/pending?))
  (map list->entry/pending
       (apply redis-emit! client "XPENDING" key group
              (stream-range-position->string start)
              (stream-range-position->string stop)
              (number->string limit)
              (if consumer
                  (list consumer)
                  (list)))))

(define (list->entry/pending l)
  (apply redis-stream-entry/pending l))

;; XRANGE key start stop [COUNT count]
;; XREVRANGE key stop start [COUNT count]
(define/contract/provide (redis-stream-range client key
                                             #:reverse? [reverse? #f]
                                             #:start [start 'first-entry]
                                             #:stop [stop 'last-entry]
                                             #:limit [limit #f])
  (->* (redis? redis-key/c)
       (#:reverse? boolean?
        #:start stream-range-position/c
        #:stop stream-range-position/c
        #:limit (or/c false/c exact-positive-integer?))
       (listof redis-stream-entry?))

  (map pair->entry
       (apply redis-emit! client
              (if reverse? "XREVRANGE" "XRANGE")
              key
              (stream-range-position->string (if reverse? stop start))
              (stream-range-position->string (if reverse? start stop))
              (if limit
                  (list (number->string limit))
                  (list)))))

;; XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
(define stream-group-position/c
  (or/c 'new-entries redis-string/c))

(define (stream-group-position->string p)
  (match p
    ['new-entries #">"]
    [p            p]))

(define/contract/provide (redis-stream-group-read! client
                                                   #:streams streams
                                                   #:group group
                                                   #:consumer consumer
                                                   #:limit [limit #f]
                                                   #:block? [block? #f]
                                                   #:timeout [timeout 0]
                                                   #:no-ack? [no-ack? #f])
  (->* (redis?
        #:streams (non-empty-listof (cons/c redis-key/c stream-group-position/c))
        #:group redis-string/c
        #:consumer redis-string/c)
       (#:limit (or/c false/c exact-positive-integer?)
        #:block? boolean?
        #:timeout exact-nonnegative-integer?
        #:no-ack? boolean?)
       (or/c false/c (listof (list/c bytes? (listof redis-stream-entry?)))))

  (define timeout/read (if (> timeout 0) (add1 timeout) #f))

  (define-values (keys ids)
    (for/lists (keys ids)
               ([pair (in-list streams)])
      (values (car pair) (stream-group-position->string (cdr pair)))))

  (let* ([args (cons "STREAMS" (append keys ids))]
         [args (if no-ack? (cons "NOACK" args) args)]
         [args (if block? (cons "BLOCK" (cons (number->string timeout) args)) args)]
         [args (if limit (cons "COUNT" (cons (number->string limit) args)) args)])
    (for/list ([pair (in-list (apply redis-emit! client "XREADGROUP" "GROUP" group consumer args
                                     #:timeout timeout/read))])
      (list (car pair) (map pair->entry (cadr pair))))))

;; XTRIM key MAXLEN [~] count
(define/contract/provide (redis-stream-trim! client key
                                             #:max-length [max-length #f]
                                             #:max-length/approximate [max-length/approximate #f])
  (->i ([client redis?]
        [key redis-key/c])
       (#:max-length [max-length exact-positive-integer?]
        #:max-length/approximate [max-length/approximate exact-positive-integer?])

       #:pre/name (max-length max-length/approximate)
       "either max-length or max-length/approximate can be provided but not both"
       (or (and max-length (unsupplied-arg? max-length/approximate))
           (and max-length/approximate (unsupplied-arg? max-length)))

       [result exact-nonnegative-integer?])

  (cond
    [max-length/approximate
     (apply redis-emit! client "XTRIM" key "MAXLEN" "~" (number->string max-length/approximate))]

    [max-length
     (apply redis-emit! client "XTRIM" key "MAXLEN" (number->string max-length))]))
