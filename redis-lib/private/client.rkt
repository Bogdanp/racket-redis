#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/async-channel
         racket/contract
         racket/dict
         racket/function
         racket/list
         racket/match
         racket/set
         racket/string
         racket/tcp
         "error.rkt"
         "protocol.rkt")

;; contracts ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 redis-string/c
 redis-key/c
 redis-key-type/c
 redis-value/c)

(define redis-string/c
  (or/c bytes? string?))

(define redis-key/c
  redis-string/c)

(define redis-key-type/c
  (or/c 'string 'list 'set 'zset 'hash 'stream))

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

(define-logger redis)

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
    (log-redis-warning "client already connected; disconnecting...")
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
      (thread
       (lambda _
         (with-handlers ([exn:fail?
                          (lambda (e)
                            (redis-disconnect! client)
                            (raise e))])
           (let loop ()
             (define response (redis-read in))
             (log-redis-debug "received ~v" response)
             (channel-put response-ch response)
             (loop))))))

    (set-redis-response-ch! client response-ch)
    (set-redis-response-reader! client response-reader)))

(define/contract (redis-disconnect! client)
  (-> redis? void?)
  (custodian-shutdown-all (redis-custodian client)))

(define (send-request! client cmd [args null])
  (parameterize ([current-output-port (redis-out client)])
    (define request (cons cmd args))
    (log-redis-debug "sending ~v" request)
    (redis-write request)
    (flush-output)))

(define (take-response! client [timeout #f])
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

          [(string-prefix? message "NOSCRIPT")
           (raise (exn:fail:redis:script:missing (substring message 9) (current-continuation-marks)))]

          [else
           (raise (exn:fail:redis message (current-continuation-marks)))])]

       [response response]))

    (if timeout
        (handle-evt
         (alarm-evt (+ (current-inexact-milliseconds) timeout))
         (lambda _
           (redis-disconnect! client)
           (raise (exn:fail:redis:timeout
                   "timed out while waiting for response from Redis"
                   (current-continuation-marks)))))
        never-evt))))

(define (redis-emit! client command
                     #:timeout [timeout (redis-timeout client)]
                     . args)
  (unless (redis-connected? client)
    (log-redis-warning "client is not connected; reconnecting...")
    (redis-connect! client))

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


;; bytestring commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; APPEND key value
(define-simple-command (bytes-append! [key redis-key/c]
                                      [value redis-string/c])
  #:command ("APPEND")
  #:result-contract exact-nonnegative-integer?)

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

;; GET key
;; MGET key [key ...]
(define/contract/provide (redis-bytes-get client key . keys)
  (-> redis? redis-key/c redis-key/c ... redis-value/c)
  (if (null? keys)
      (redis-emit! client "GET" key)
      (apply redis-emit! client "MGET" key keys)))

;; GETBIT key offset
(define-simple-command (bytes-ref/bit [key redis-key/c]
                                      [offset exact-nonnegative-integer? #:converter number->string])
  #:command ("GETBIT")
  #:result-contract (or/c 0 1))

;; GETRANGE key start end
(define/contract/provide (redis-subbytes client key
                                         #:start [start 0]
                                         #:stop [stop -1])
  (->* (redis? redis-key/c)
       (#:start exact-integer?
        #:stop exact-integer?)
       bytes?)
  (redis-emit! client "GETRANGE" key (number->string start) (number->string stop)))

;; INCR key
;; INCRBY key increment
;; INCRBYFLOAT key increment
(define/contract/provide (redis-bytes-incr! client key [n 1])
  (->* (redis? redis-key/c) (real?) real?)
  (define res
    (apply redis-emit! client (cond
                                [(= n 1)            (list "INCR"        key)]
                                [(exact-integer? n) (list "INCRBY"      key (number->string n))]
                                [else               (list "INCRBYFLOAT" key (number->string n))])))

  (if (bytes? res)
      (bytes->number res)
      res))

;; SET key value [EX seconds | PX milliseconds] [NX|XX]
(define/contract/provide (redis-bytes-set! client key value
                                           #:expires-in [expires-in #f]
                                           #:unless-exists? [unless-exists? #f]
                                           #:when-exists? [when-exists? #f])
  (->i ([client redis?]
        [key redis-key/c]
        [value redis-string/c])
       (#:expires-in [expires-in (or/c false/c exact-positive-integer?)]
        #:unless-exists? [unless-exists? boolean?]
        #:when-exists? [when-exists? boolean?])

       #:pre/name (unless-exists? when-exists?)
       "either unless-exists? or when-exists? can be supplied but not both"
       (exclusive-args unless-exists? when-exists?)

       [result boolean?])
  (ok? (apply redis-emit! client "SET" key value (optionals
                                                  [expires-in "PX" expires-in]
                                                  [unless-exists? "NX"]
                                                  [when-exists? "XX"]))))

;; SETBIT key offset value
(define-simple-command (bytes-set/bit! [key redis-key/c]
                                       [offset exact-nonnegative-integer? #:converter number->string]
                                       [value (or/c 0 1) #:converter number->string])
  #:command ("SETBIT")
  #:result-contract (or/c 0 1))

;; SETRANGE key offset value
(define-simple-command (bytes-copy! [key redis-key/c]
                                    [offset exact-nonnegative-integer? #:converter number->string]
                                    [value redis-string/c])
  #:command ("SETRANGE")
  #:result-contract exact-nonnegative-integer?)

;; STRLEN key
(define-simple-command (bytes-length [key redis-key/c])
  #:command ("STRLEN")
  #:result-contract exact-nonnegative-integer?)


;; cluster commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; REPLICAOF NO ONE
;; REPLICAOF host port
(define-simple-command (make-replica-of! [host redis-string/c] [port (integer-in 0 65536) #:converter number->string])
  #:command ("REPLICAOF"))

(define-simple-command (stop-replication!)
  #:command ("REPLICAOF" "NO" "ONE"))


;; connection commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; AUTH password
(define-simple-command/ok (auth! [password redis-string/c]))

;; ECHO message
(define-simple-command (echo [message string?])
  #:result-contract string?
  #:result-name res
  (bytes->string/utf-8 res))

;; PING
(define-simple-command (ping)
  #:result-contract string?)

;; QUIT
(define/contract/provide (redis-quit! client)
  (-> redis? void?)
  (send-request! client "QUIT")
  (redis-disconnect! client))

;; SELECT db
(define-simple-command/ok (select-db! [db (integer-in 0 16) #:converter number->string])
  #:command ("SELECT"))

;; SWAPDB a b
(define-simple-command/ok (swap-dbs! [a (integer-in 0 16) #:converter number->string]
                                     [b (integer-in 0 16) #:converter number->string])
  #:command ("SWAPDB"))


;; geo commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 redis-latitude/c
 redis-longitude/c
 redis-geo/c
 redis-geo-unit/c)

(define redis-latitude/c
  (real-in -90 90))

(define redis-longitude/c
  (real-in -180 180))

(define redis-geo-pos/c
  (list/c redis-longitude/c redis-latitude/c))

(define redis-geo/c
  (list/c redis-longitude/c redis-latitude/c redis-string/c))

(define redis-geo-unit/c
  (or/c 'm 'km 'mi 'ft))

(define (flatten-geos geos)
  (for/fold ([items null])
            ([geo (in-list geos)])
    (cons (number->string (first geo))
          (cons (number->string (second geo))
                (cons (third geo) items)))))

;; GEOADD key long lat member [long lat member ...]
(define/contract/provide (redis-geo-add! client key geo . geos)
  (-> redis? redis-key/c redis-geo/c redis-geo/c ... exact-nonnegative-integer?)
  (apply redis-emit! client "GEOADD" key (flatten-geos (cons geo geos))))

;; GEODIST key member1 member2 [unit]
(define/contract/provide (redis-geo-dist client key member1 member2
                                         #:unit [unit #f])
  (->* (redis? redis-key/c redis-string/c redis-string/c)
       (#:unit (or/c false/c redis-geo-unit/c))
       (or/c false/c real?))

  (define res
    (apply redis-emit! client "GEODIST" key member1 member2 (if unit
                                                                (list (symbol->string unit))
                                                                (list))))

  (and res (bytes->number res)))

;; GEOHASH key member [member ...]
(define-variadic-command (geo-hash [key redis-key/c] [mem redis-string/c] . [mems redis-string/c])
  #:command ("GEOHASH")
  #:result-contract (listof (or/c false/c bytes?)))

;; GEOPOS key member [member ...]
(define/contract/provide (redis-geo-pos client key mem . mems)
  (-> redis? redis-key/c redis-string/c redis-string/c ... (listof (or/c false/c (list/c redis-longitude/c redis-latitude/c))))
  (for/list ([pair (in-list (apply redis-emit! client "GEOPOS" key mem mems))])
    (and pair (list (bytes->number (car pair))
                    (bytes->number (cadr pair))))))


;; hash commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;; HINCRBY key field amount
;; HINCRBYFLOAT key field amount
(define/contract/provide (redis-hash-incr! client key fld [n 1])
  (->* (redis? redis-key/c redis-string/c) (real?) real?)
  (define res
    (apply redis-emit! client (cond
                                [(exact-integer? n) (list "HINCRBY"      key fld (number->string n))]
                                [else               (list "HINCRBYFLOAT" key fld (number->string n))])))

  (if (bytes? res)
      (bytes->number res)
      res))

;; HKEYS key
(define-simple-command (hash-keys [key redis-key/c])
  #:command ("HKEYS")
  #:result-contract (listof bytes?))

;; HLEN key
(define-simple-command (hash-length [key redis-key/c])
  #:command ("HLEN")
  #:result-contract exact-nonnegative-integer?)

;; HSCAN key cursor [MATCH pattern] [COUNT count]
(define/contract/provide (redis-hash-scan client key
                                          #:cursor [cursor 0]
                                          #:pattern [pattern #f]
                                          #:limit [limit #f])
  (->* (redis? redis-key/c)
       (#:cursor exact-nonnegative-integer?
        #:pattern (or/c false/c redis-string/c)
        #:limit (or/c false/c exact-positive-integer?))
       (values exact-nonnegative-integer? (listof (cons/c bytes? bytes?))))


  (define res
    (apply redis-emit! client "HSCAN" key (number->string cursor) (optionals
                                                                   [pattern "MATCH" pattern]
                                                                   [limit "COUNT" (number->string limit)])))

  (values (bytes->number (car res))
          (for/list ([(key value) (in-twos (cadr res))])
            (cons key value))))

(define (make-scanner-sequence scanner)
  (make-keyword-procedure
   (lambda (kws kw-args . args)
     (make-do-sequence
      (lambda _
        (define cursor 0)
        (define buffer null)

        (define (fresh!)
          (define-values (cursor* buffer*)
            (keyword-apply scanner kws kw-args args
                           #:cursor cursor))

          (set! buffer buffer*)
          (set! cursor (if (zero? cursor*) #f cursor*)))

        (define (take!)
          (when (null? buffer)
            (fresh!))

          (begin0 (car buffer)
            (set! buffer (cdr buffer))))

        (values
         (lambda _
           (cond
             [(and (null? buffer)
                   (not cursor)) 'done]
             [else (take!)]))
         (lambda _ #f)
         #f
         #f
         (lambda (v) (not (eq? v 'done)))
         #f))))))

(define in-redis-hash (make-scanner-sequence redis-hash-scan))
(provide in-redis-hash)

;; HSET key field value
;; HMSET key field value [field value ...]
(define/contract/provide redis-hash-set!
  (case->
   (-> redis? redis-key/c redis-string/c redis-string/c boolean?)
   (-> redis? redis-key/c redis-string/c redis-string/c #:rest (listof redis-string/c) boolean?)
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

;; HSTRLEN key field
(define-simple-command (hash-string-length [key redis-key/c] [fld redis-string/c])
  #:command ("HSTRLEN")
  #:result-contract exact-nonnegative-integer?)

;; HVALS key
(define-simple-command (hash-values [key redis-key/c])
  #:command ("HVALS")
  #:result-contract (listof bytes?))


;; hyperloglog commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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


;; key commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; EXISTS key [key ...]
(define-simple-command/1 (has-key? [key redis-key/c])
  #:command ("EXISTS"))

(define-variadic-command (count-keys . [key redis-key/c])
  #:command ("EXISTS")
  #:result-contract exact-nonnegative-integer?)

;; KEYS pattern
(define-simple-command (keys [pattern redis-string/c])
  #:result-contract (listof bytes?))

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

;; PTTL key
(define-simple-command (key-ttl [key redis-key/c])
  #:command ("PTTL")
  #:result-contract (or/c 'missing 'persisted exact-nonnegative-integer?)
  #:result-name res
  (case res
    [(-2) 'missing]
    [(-1) 'persisted]
    [else res]))

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

;; SCAN key cursor [MATCH pattern] [COUNT count] [TYPE type]
(define/contract/provide (redis-scan client
                                     #:cursor [cursor 0]
                                     #:pattern [pattern #f]
                                     #:limit [limit #f]
                                     #:type [type #f])
  (->* (redis?)
       (#:cursor exact-nonnegative-integer?
        #:pattern (or/c false/c redis-string/c)
        #:limit (or/c false/c exact-positive-integer?)
        #:type (or/c false/c redis-key-type/c))
       (values exact-nonnegative-integer? (listof redis-key/c)))

  (define res
    (apply redis-emit! client "SCAN" (number->string cursor) (optionals
                                                              [pattern "MATCH" pattern]
                                                              [limit "COUNT" (number->string limit)]
                                                              [type "TYPE" (symbol->string type)])))

  (values (bytes->number (car res)) (cadr res)))

(define in-redis (make-scanner-sequence redis-scan))
(provide in-redis)

;; TOUCH key [key ...]
(define-variadic-command (touch! [key redis-key/c] . [keys redis-key/c])
  #:result-contract exact-nonnegative-integer?)

;; TYPE key
(define-simple-command (key-type [key redis-key/c])
  #:command ("TYPE")
  #:result-contract (or/c 'none redis-key-type/c)
  #:result-name res
  (string->symbol res))

;; WAIT numreplicas timeout
(define-simple-command (wait! [replicas exact-positive-integer? #:converter number->string]
                              [timeout exact-nonnegative-integer? #:converter number->string])
  #:command ("WAIT")
  #:result-contract exact-nonnegative-integer?)


;; list commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
       (exclusive-args pivot/after pivot/before)

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
       "a list of keys may only be supplied if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (block? timeout)
       "a timeout may only be supplied if block? is #t"
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
       "a list of keys may only be supplied if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (keys dest)
       "dest and multiple keys are incompatible"
       (or (unsupplied-arg? dest) (null? keys))

       #:pre/name (block? timeout)
       "a timeout may only be supplied if block? is #t"
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


;; pubsub commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (contract-out
  [redis-pubsub? (-> any/c boolean?)]))

(struct redis-pubsub (client custodian listeners channel overseer)
  #:property prop:evt (struct-field-index channel))

(define/contract/provide (make-redis-pubsub client)
  (-> redis? redis-pubsub?)
  (define custodian (make-custodian (redis-custodian client)))
  (parameterize ([current-custodian custodian])
    (define listeners (mutable-set))
    (define (broadcast message)
      (for ([listener (in-set listeners)])
        (async-channel-put listener message)))

    (define channel (make-async-channel))
    (define overseer
      (thread
       (lambda _
         (let loop ()
           (match (take-response! client)
             [(list #"message" channel-name message)
              (async-channel-put channel (list channel-name message))]

             [(list #"pmessage" pattern channel-name message)
              (async-channel-put channel (list pattern channel-name message))]

             [(list type channel-name n)
              (broadcast (list (string->symbol (bytes->string/utf-8 type)) channel-name n))])

           (loop)))))

    (redis-pubsub client custodian listeners channel overseer)))

(define/contract/provide (redis-pubsub-kill! pubsub)
  (-> redis-pubsub? void?)
  (call-with-pubsub-events pubsub
    (lambda (events)
      (send-request! (redis-pubsub-client pubsub) "UNSUBSCRIBE")
      (send-request! (redis-pubsub-client pubsub) "PUNSUBSCRIBE")
      (wait-until-unsubscribed/all events)
      (wait-until-punsubscribed/all events)))

  (custodian-shutdown-all (redis-pubsub-custodian pubsub)))

(define/contract/provide (call-with-redis-pubsub client proc)
  (-> redis? (-> redis-pubsub? any) any)
  (define pubsub #f)
  (dynamic-wind
    (lambda _
      (set! pubsub (make-redis-pubsub client)))
    (lambda _
      (proc pubsub))
    (lambda _
      (redis-pubsub-kill! pubsub))))

(define (call-with-pubsub-events pubsub proc)
  (define listener #f)
  (dynamic-wind
    (lambda _
      (set! listener (make-async-channel))
      (set-add! (redis-pubsub-listeners pubsub) listener))
    (lambda _
      (proc listener))
    (lambda _
      (set-remove! (redis-pubsub-listeners pubsub) listener))))

(define ((make-pubsub-waiter proc) events remaining)
  (let loop ([remaining (for/list ([item (in-list remaining)])
                          (if (string? item)
                              (string->bytes/utf-8 item)
                              item))])
    (unless (null? remaining)
      (loop (proc (sync events) remaining)))))

(define-syntax (define-pubsub-waiter stx)
  (syntax-parse stx
    [(_ type:id)
     (with-syntax ([fn-name (format-id #'type "wait-until-~ad" #'type)])
       #'(define fn-name
           (make-pubsub-waiter (lambda (event remaining)
                                 (match event
                                   [(list 'type item _)
                                    (remove item remaining)]

                                   [_ remaining])))))]))

(define-pubsub-waiter subscribe)
(define-pubsub-waiter psubscribe)
(define-pubsub-waiter unsubscribe)
(define-pubsub-waiter punsubscribe)

(define (wait-until-unsubscribed/all events)
  (let loop ()
    (match (sync events)
      [(list 'unsubscribe #f _) (void)]
      [(list 'unsubscribe _  0) (void)]
      [_ (loop)])))

(define (wait-until-punsubscribed/all events)
  (let loop ()
    (match (sync events)
      [(list 'punsubscribe #f _) (void)]
      [(list 'punsubscribe _  0) (void)]
      [_ (loop)])))

;; PSUBSCRIBE pattern [pattern ...]
;; SUBSCRIBE channel [channel ...]
(define/contract/provide (redis-pubsub-subscribe! pubsub
                                                  #:patterns? [patterns? #f]
                                                  channel-or-pattern . channel-or-patterns)
  (->* (redis-pubsub? redis-string/c)
       (#:patterns? boolean?)
       #:rest (listof redis-string/c)
       void?)
  (call-with-pubsub-events pubsub
    (lambda (events)
      (let ([channel-or-patterns (cons channel-or-pattern channel-or-patterns)])
        (send-request! (redis-pubsub-client pubsub)
                       (if patterns?
                           "PSUBSCRIBE"
                           "SUBSCRIBE")
                       channel-or-patterns)
        (if patterns?
            (wait-until-psubscribed events channel-or-patterns)
            (wait-until-subscribed events channel-or-patterns))))))

;; PUNSUBSCRIBE [pattern ...]
;; UNSUBSCRIBE [channel ...]
(define/contract/provide (redis-pubsub-unsubscribe! pubsub
                                                    #:patterns? [patterns? #f]
                                                    . channel-or-patterns)
  (->* (redis-pubsub?)
       (#:patterns? boolean?)
       #:rest (listof redis-string/c)
       void?)
  (call-with-pubsub-events pubsub
    (lambda (events)
      (send-request! (redis-pubsub-client pubsub)
                     (if patterns?
                         "PUNSUBSCRIBE"
                         "UNSUBSCRIBE")
                     channel-or-patterns)
      (if patterns?
          (wait-until-punsubscribed events channel-or-patterns)
          (wait-until-unsubscribed events channel-or-patterns)))))

;; PUBLISH channel message
(define-simple-command (pubsub-publish! [channel redis-string/c] [message redis-string/c])
  #:command ("PUBLISH")
  #:result-contract exact-nonnegative-integer?)


;; script commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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


;; server commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
  #:command ("CLIENT" "PAUSE"))

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

;; DUMP key
(define-simple-command (dump [key redis-key/c])
  #:result-contract (or/c false/c bytes?))

;; FLUSHALL [ASYNC]
(define-simple-command/ok (flush-all!))

;; FLUSHDB [ASYNC]
(define-simple-command/ok (flush-db!))

;; INFO
(define-simple-command (info [section redis-string/c]))

;; LASTSAVE
(define-simple-command (last-save-time)
  #:command ("LASTSAVE")
  #:result-contract exact-nonnegative-integer?)

;; REWRITEAOF
;; BGREWRITEAOF
(define-simple-command/ok (rewrite-aof!)
  #:command ("REWRITEAOF"))

(define-simple-command/ok (rewrite-aof/async!)
  #:command ("BGREWRITEAOF"))

;; ROLE
(define-simple-command (role))

;; SAVE
;; BGSAVE
(define-simple-command/ok (save!)
  #:command ("SAVE"))

(define-simple-command/ok (save/async!)
  #:command ("BGSAVE"))

;; SHUTDOWN
(define/contract/provide (redis-shutdown! client [save? #t])
  (->* (redis?) (boolean?) void?)
  (send-request! client "SHUTDOWN" (list (if save? "SAVE" "NOSAVE"))))

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

;; TIME
(define-simple-command (time)
  #:result-contract real?
  #:result-name res
  (define-values (seconds micros)
    (apply values (map (compose1 string->number bytes->string/utf-8) res)))
  (real->double-flonum (+ (* seconds 1000) (/ micros 1000))))


;; set commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; SADD key val [val ...]
(define-variadic-command (set-add! [key redis-key/c] [val redis-string/c] . [vals redis-string/c])
  #:command ("SADD")
  #:result-contract exact-nonnegative-integer?)

;; SCARD key
(define-simple-command (set-count [key redis-key/c])
  #:command ("SCARD")
  #:result-contract exact-nonnegative-integer?)

;; SDIFF key [key ...]
(define-variadic-command (set-difference [key redis-key/c] . [keys redis-key/c])
  #:command ("SDIFF")
  #:result-contract (listof bytes?))

;; SDIFFSTORE target key [key ...]
(define-variadic-command (set-difference! [target redis-key/c] [key redis-key/c] . [keys redis-key/c])
  #:command ("SDIFFSTORE")
  #:result-contract exact-nonnegative-integer?)

;; SINTER key [key ...]
(define-variadic-command (set-intersect [key redis-key/c] . [keys redis-key/c])
  #:command ("SINTER")
  #:result-contract (listof bytes?))

;; SINTERSTORE target key [key ...]
(define-variadic-command (set-intersect! [target redis-key/c] [key redis-key/c] . [keys redis-key/c])
  #:command ("SINTERSTORE")
  #:result-contract exact-nonnegative-integer?)

;; SISMEMBER key val
(define-simple-command/1 (set-member? [key redis-key/c] [val redis-string/c])
  #:command ("SISMEMBER"))

;; SMEMBERS key
(define-simple-command (set-members [key redis-key/c])
  #:command ("SMEMBERS")
  #:result-contract (listof bytes?))

;; SMOVE source destination member
(define-simple-command/1 (set-move! [source redis-key/c] [destination redis-key/c] [val redis-string/c])
  #:command ("SMOVE"))

;; SPOP key [count]
(define/contract/provide (redis-set-pop! client key #:count [cnt 1])
  (->* (redis? redis-key/c)
       (#:count exact-positive-integer?)
       (listof bytes?))
  (if (> cnt 1)
      (redis-emit! client "SPOP" key (number->string cnt))
      (redis-emit! client "SPOP" key)))

;; SRANDMEMBER key [count]
(define/contract/provide redis-set-random-ref
  (case->
   (-> redis? redis-key/c (or/c false/c bytes?))
   (-> redis? redis-key/c exact-integer? (listof bytes?)))
  (case-lambda
    [(client key)
     (redis-emit! client "SRANDMEMBER" key)]

    [(client key cnt)
     (redis-emit! client "SRANDMEMBER" key (number->string cnt))]))

;; SREM key val [val ...]
(define-variadic-command (set-remove! [key redis-key/c] [val redis-string/c] . [vals redis-string/c])
  #:command ("SREM")
  #:result-contract exact-nonnegative-integer?)

;; SSCAN key cursor [MATCH pattern] [COUNT count]
(define/contract/provide (redis-set-scan client key
                                         #:cursor [cursor 0]
                                         #:pattern [pattern #f]
                                         #:limit [limit #f])
  (->* (redis? redis-key/c)
       (#:cursor exact-nonnegative-integer?
        #:pattern (or/c false/c redis-string/c)
        #:limit (or/c false/c exact-positive-integer?))
       (values exact-nonnegative-integer? (listof redis-string/c)))

  (define res
    (apply redis-emit! client "SSCAN" key (number->string cursor) (optionals
                                                                   [pattern "MATCH" pattern]
                                                                   [limit "COUNT" (number->string limit)])))

  (values (bytes->number (car res)) (cadr res)))

(define in-redis-set (make-scanner-sequence redis-set-scan))
(provide in-redis-set)

;; SUNION key [key ...]
(define-variadic-command (set-union [key redis-key/c] . [keys redis-key/c])
  #:command ("SUNION")
  #:result-contract (listof bytes?))

;; SUNIONSTORE target key [key ...]
(define-variadic-command (set-union! [target redis-key/c] [key redis-key/c] . [keys redis-key/c])
  #:command ("SUNIONSTORE")
  #:result-contract exact-nonnegative-integer?)


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
       "either max-length or max-length/approximate can be supplied but not both"
       (exclusive-args max-length max-length/approximate)

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

;; XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME milliseconds] [RETRYCOUNT count] [FORCE] [JUSTID]
(define/contract/provide (redis-stream-group-claim! client key
                                                    #:group group
                                                    #:consumer consumer
                                                    #:min-idle-time min-idle-time
                                                    #:new-idle-value [new-idle-value #f]
                                                    #:new-time-value [new-time-value #f]
                                                    #:new-retry-count [new-retry-count #f]
                                                    #:force? [force? #f]
                                                    . ids)
  (->* (redis?
        redis-key/c
        #:group redis-string/c
        #:consumer redis-string/c
        #:min-idle-time exact-nonnegative-integer?)
       (#:new-idle-value exact-nonnegative-integer?
        #:new-time-value exact-nonnegative-integer?
        #:new-retry-count exact-nonnegative-integer?
        #:force? boolean?)
       #:rest (non-empty-listof redis-string/c)
       (non-empty-listof redis-stream-entry?))

  (define args
    (optionals
     [new-idle-value "IDLE" (number->string new-idle-value)]
     [new-time-value "TIME" (number->string new-time-value)]
     [new-retry-count "RETRYCOUNT" (number->string new-retry-count)]
     [force? "FORCE"]))

  (define pairs
    (apply redis-emit! client "XCLAIM" key group consumer min-idle-time (append ids args)))

  (and pairs (map pair->entry pairs)))

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

(define stream-range-position->string
  (match-lambda
    ['first-entry #"-"]
    ['last-entry  #"+"]
    [posn         posn]))

(define/contract/provide (redis-substream/group client key group [consumer #f]
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
(define/contract/provide (redis-substream client key
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

;; XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
(define stream-position/c
  (or/c 'new-entries redis-string/c))

(define stream-position->string
  (match-lambda
    ['new-entries #"$"]
    [posn         posn]))

(define/contract/provide (redis-stream-read! client
                                             #:streams streams
                                             #:limit [limit #f]
                                             #:block? [block? #f]
                                             #:timeout [timeout 0])
  (->* (redis?
        #:streams (non-empty-listof (cons/c redis-key/c stream-position/c)))
       (#:limit (or/c false/c exact-positive-integer?)
        #:block? boolean?
        #:timeout exact-nonnegative-integer?)
       (or/c false/c (listof (list/c bytes? (listof redis-stream-entry?)))))

  (define timeout/read (if (> timeout 0) (add1 timeout) #f))

  (define-values (keys ids)
    (for/lists (keys ids)
               ([pair (in-list streams)])
      (values (car pair) (stream-position->string (cdr pair)))))

  (let* ([args (cons "STREAMS" (append keys ids))]
         [args (if block? (cons "BLOCK" (cons (number->string timeout) args)) args)]
         [args (if limit (cons "COUNT" (cons (number->string limit) args)) args)]
         [pairs (apply redis-emit! client "XREAD" args #:timeout timeout/read)])
    (and pairs (for/list ([pair (in-list pairs)])
                 (list (car pair) (map pair->entry (cadr pair)))))))

;; XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
(define stream-group-position/c
  (or/c 'new-entries redis-string/c))

(define stream-group-position->string
  (match-lambda
    ['new-entries #">"]
    [posn         posn]))

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
         [args (if limit (cons "COUNT" (cons (number->string limit) args)) args)]
         [pairs (apply redis-emit! client "XREADGROUP" "GROUP" group consumer args #:timeout timeout/read)])
    (and pairs (for/list ([pair (in-list pairs)])
                 (list (car pair) (map pair->entry (cadr pair)))))))

;; XTRIM key MAXLEN [~] count
(define/contract/provide (redis-stream-trim! client key
                                             #:max-length [max-length #f]
                                             #:max-length/approximate [max-length/approximate #f])
  (->i ([client redis?]
        [key redis-key/c])
       (#:max-length [max-length exact-positive-integer?]
        #:max-length/approximate [max-length/approximate exact-positive-integer?])

       #:pre/name (max-length max-length/approximate)
       "either max-length or max-length/approximate can be supplied but not both"
       (exclusive-args max-length max-length/approximate)

       [result exact-nonnegative-integer?])

  (cond
    [max-length/approximate
     (apply redis-emit! client "XTRIM" key "MAXLEN" "~" (number->string max-length/approximate))]

    [max-length
     (apply redis-emit! client "XTRIM" key "MAXLEN" (number->string max-length))]))


;; sorted set commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (in-twos seq)
  (make-do-sequence
   (lambda _
     (define canary (lambda () 'canary))
     (define buffer seq)
     (define (take!)
       (begin0 (car buffer)
         (set! buffer (cdr buffer))))

     (values
      (lambda _
        (if (< (length buffer) 2)
            (values canary canary)
            (values (take!) (take!))))
      (lambda _ #f)
      #f
      #f
      (lambda (v1 v2) (not (eq? v1 canary)))
      #f))))

;; ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
;; TODO: NX, XX, CH and INCR
(define/contract/provide (redis-zset-add! client key . member-and-scores)
  (->i ([client redis?]
        [key redis-key/c])
       #:rest [member-and-scores (non-empty-listof any/c)]
       #:pre/name (member-and-scores)
       "must contain an even number of member names and score real?s"
       (even? (length member-and-scores))
       [result exact-nonnegative-integer?])

  (apply redis-emit! client "ZADD" key (for/fold ([res null])
                                                 ([(mem score) (in-twos member-and-scores)])
                                         (cons (number->string score) (cons mem res)))))

;; ZCARD key
;; ZCOUNT key min max
;; TODO: min/exclusive?, max/exclusive?
(define/contract/provide (redis-zset-count client key
                                           #:min [min #f]
                                           #:max [max #f])
  (->i ([client redis?]
        [key redis-key/c])
       (#:min [min real?]
        #:max [max real?])
       #:pre/name (min max)
       "both min and max need to be supplied if either one is"
       (if (unsupplied-arg? min)
           (unsupplied-arg? max)
           (not (unsupplied-arg? max)))
       [result exact-nonnegative-integer?])

  (define (~n n)
    (case n
      [(-inf.0) "-inf"]
      [(+inf.0) "+inf"]
      [else (number->string n)]))

  (cond
    [(and min max)
     (redis-emit! client "ZCOUNT" key (~n min) (~n max))]

    [else
     (redis-emit! client "ZCARD" key)]))

;; ZINCRBY key increment member
(define/contract/provide (redis-zset-incr! client key mem [n 1])
  (->* (redis? redis-key/c redis-string/c) (real?) real?)
  (bytes->number (redis-emit! client "ZINCRBY" key (number->string n) mem)))

;; ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
(define/contract/provide (redis-zset-intersect! client dest
                                                #:weights [weights #f]
                                                #:aggregate [aggregate #f]
                                                . keys)
  (->i ([client redis?]
        [dest redis-key/c])
       (#:weights [weights (non-empty-listof real?)]
        #:aggregate [aggregate (or/c 'sum 'min 'max)])
       #:rest [keys (non-empty-listof redis-key/c)]

       #:pre/name (keys weights)
       "a weight for each supplied key"
       (or (unsupplied-arg? weights)
           (= (length weights)
              (length keys)))

       [result exact-nonnegative-integer?])

  (apply redis-emit!
         client "ZINTERSTORE" dest
         (number->string (length keys))
         (append keys (optionals
                       [weights "WEIGHTS" (map number->string weights)]
                       [aggregate "AGGREGATE" (string-upcase (symbol->string aggregate))]))))

;; ZLEXCOUNT key min max
(define/contract/provide (redis-zset-count/lex client key
                                               #:min [min #"-"]
                                               #:max [max #"+"])
  (->* (redis? redis-key/c)
       (#:min redis-string/c
        #:max redis-string/c)
       exact-nonnegative-integer?)

  (redis-emit! client "ZLEXCOUNT" key min max))

;; BZPOPMAX key [key ...] timeout
;; ZPOPMAX key [count]
(define/contract/provide (redis-zset-pop/max! client key
                                              #:count [cnt #f]
                                              #:block? [block? #f]
                                              #:timeout [timeout 0]
                                              . keys)
  (->i ([client redis?]
        [key redis-key/c])
       (#:count [cnt exact-positive-integer?]
        #:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof redis-key/c)]

       #:pre/name (cnt block?)
       "a count may only be supplied if block? is #f"
       (or (unsupplied-arg? cnt) (unsupplied-arg? block?) (not block?))

       #:pre/name (keys block?)
       "a list of keys may only be supplied if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (block? timeout)
       "a timeout may only be supplied if block? is #t"
       (or (unsupplied-arg? timeout) (equal? block? #t))

       [result (or/c false/c
                     (list/c bytes? bytes? real?)
                     (listof (cons/c bytes? real?)))])

  (cond
    [block?
     (define res
       (apply redis-emit! client "BZPOPMAX" key (append keys (list (number->string timeout)))))

     (match res
       [#f #f]
       [(list key mem score)
        (list key mem (bytes->number score))])]

    [else
     (define res
       (apply redis-emit! client "ZPOPMAX" key (optionals
                                                [cnt (number->string cnt)])))

     (for/list ([(mem score) (in-twos res)])
       (cons mem (bytes->number score)))]))

;; BZPOPMIN key [key ...] timeout
;; ZPOPMIN key [count]
(define/contract/provide (redis-zset-pop/min! client key
                                              #:count [cnt #f]
                                              #:block? [block? #f]
                                              #:timeout [timeout 0]
                                              . keys)
  (->i ([client redis?]
        [key redis-key/c])
       (#:count [cnt exact-positive-integer?]
        #:block? [block? boolean?]
        #:timeout [timeout exact-nonnegative-integer?])
       #:rest [keys (listof redis-key/c)]

       #:pre/name (cnt block?)
       "a count may only be supplied if block? is #f"
       (or (unsupplied-arg? cnt) (unsupplied-arg? block?) (not block?))

       #:pre/name (keys block?)
       "a list of keys may only be supplied if block? is #t"
       (or (null? keys) (equal? block? #t))

       #:pre/name (block? timeout)
       "a timeout may only be supplied if block? is #t"
       (or (unsupplied-arg? timeout) (equal? block? #t))

       [result (or/c false/c
                     (list/c bytes? bytes? real?)
                     (listof (cons/c bytes? real?)))])

  (cond
    [block?
     (define res
       (apply redis-emit! client "BZPOPMIN" key (append keys (list (number->string timeout)))))

     (match res
       [#f #f]
       [(list key mem score)
        (list key mem (bytes->number score))])]

    [else
     (define res
       (apply redis-emit! client "ZPOPMIN" key (optionals
                                                [cnt (number->string cnt)])))

     (for/list ([(mem score) (in-twos res)])
       (cons mem (bytes->number score)))]))

;; ZRANGE key start stop [WITHSCORES]
;; ZREVRANGE key start stop [WITHSCORES]
(define/contract/provide (redis-subzset client key
                                        #:reverse? [reverse? #f]
                                        #:start [start 0]
                                        #:stop [stop -1]
                                        #:include-scores? [scores? #f])
  (->* (redis? redis-key/c)
       (#:reverse? boolean?
        #:start exact-integer?
        #:stop exact-integer?
        #:include-scores? boolean?)
       (or/c (listof bytes?)
             (listof (cons/c bytes? real?))))

  (define command (if reverse? "ZREVRANGE" "ZRANGE"))
  (define args (optionals [scores? "WITHSCORES"]))
  (define res (apply redis-emit! client command key (number->string start) (number->string stop) args))

  (if scores?
      (for/list ([(mem score) (in-twos res)])
        (cons mem (bytes->number score)))
      res))

;; ZRANGEBYLEX key min max [LIMIT offset count]
;; ZREVRANGEBYLEX key min max [LIMIT offset count]
(define/contract/provide (redis-subzset/lex client key
                                            #:reverse? [reverse? #f]
                                            #:min [min (if reverse? #"+" #"-")]
                                            #:max [max (if reverse? #"-" #"+")]
                                            #:limit [limit #f]
                                            #:offset [offset 0])
  (->i ([client redis?]
        [key redis-key/c])
       (#:reverse? [reverse? boolean?]
        #:min [min redis-string/c]
        #:max [max redis-string/c]
        #:limit [limit exact-positive-integer?]
        #:offset [offset exact-nonnegative-integer?])

       #:pre/name (limit offset)
       "an offset may only be supplied if limit is"
       (if (unsupplied-arg? limit)
           (unsupplied-arg? offset)
           offset)

       [result (listof bytes?)])

  (define command (if reverse? "ZREVRANGEBYLEX" "ZRANGEBYLEX"))
  (define args (optionals [limit "LIMIT" (number->string offset) (number->string limit)]))
  (apply redis-emit! client command key min max args))

;; ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
;; ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
(define/contract/provide (redis-subzset/score client key
                                            #:reverse? [reverse? #f]
                                            #:include-scores? [scores? #f]
                                            #:start [start (if reverse? +inf.0 -inf.0)]
                                            #:stop [stop (if reverse? -inf.0 +inf.0)]
                                            #:limit [limit #f]
                                            #:offset [offset 0])
  (->i ([client redis?]
        [key redis-key/c])
       (#:reverse? [reverse? boolean?]
        #:include-scores? [scores? boolean?]
        #:start [start real?]
        #:stop [stop real?]
        #:limit [limit exact-positive-integer?]
        #:offset [offset exact-nonnegative-integer?])

       #:pre/name (limit offset)
       "an offset may only be supplied if limit is"
       (if (unsupplied-arg? limit)
           (unsupplied-arg? offset)
           offset)

       [result (or/c (listof bytes?)
                     (listof (cons/c bytes? real?)))])

  (define (~n n)
    (case n
      [(-inf.0) "-inf"]
      [(+inf.0) "+inf"]
      [else (number->string n)]))

  (define command
    (if reverse? "ZREVRANGEBYSCORE" "ZRANGEBYSCORE"))

  (define args
    (optionals
     [scores? "WITHSCORES"]
     [limit "LIMIT" (number->string offset) (number->string limit)]))

  (define res
    (apply redis-emit! client command key (~n start) (~n stop) args))

  (if scores?
      (for/list ([(mem score) (in-twos res)])
        (cons mem (bytes->number score)))
      res))

;; ZRANK key member
;; ZREVRANK key member
(define/contract/provide (redis-zset-rank client key mem #:reverse? [reverse? #f])
  (->* (redis? redis-key/c redis-string/c)
       (#:reverse? boolean?)
       (or/c false/c exact-nonnegative-integer?))
  (if reverse?
      (redis-emit! client "ZREVRANK" key mem)
      (redis-emit! client "ZRANK"    key mem)))

;; ZREM key member [member ...]
(define-variadic-command (zset-remove! [key redis-key/c] [mem redis-string/c] . [mems redis-string/c])
  #:command ("ZREM")
  #:result-contract exact-nonnegative-integer?)

;; ZREMRANGEBYLEX key min max
(define/contract/provide (redis-zset-remove/lex! client key
                                                 #:min [min "-"]
                                                 #:max [max "+"])
  (->* (redis? redis-key/c)
       (#:min redis-string/c
        #:max redis-string/c)
       exact-nonnegative-integer?)
  (redis-emit! client "ZREMRANGEBYLEX" key min max))

;; ZREMRANGEBYRANK key min max
(define/contract/provide (redis-zset-remove/rank! client key
                                                  #:start [start 0]
                                                  #:stop [stop -1])
  (->* (redis? redis-key/c)
       (#:start exact-integer?
        #:stop exact-integer?)
       exact-nonnegative-integer?)
  (redis-emit! client "ZREMRANGEBYRANK" key (number->string start) (number->string stop)))

;; ZREMRANGEBYSCORE key min max
(define/contract/provide (redis-zset-remove/score! client key
                                                   #:start [start -inf.0]
                                                   #:stop [stop +inf.0])
  (->* (redis? redis-key/c)
       (#:start real?
        #:stop real?)
       exact-nonnegative-integer?)

  (define (~n n)
    (case n
      [(-inf.0) "-inf"]
      [(+inf.0) "+inf"]
      [else (number->string n)]))

  (redis-emit! client "ZREMRANGEBYSCORE" key (~n start) (~n stop)))

;; ZSCAN key cursor [MATCH pattern] [COUNT count]
(define/contract/provide (redis-zset-scan client key
                                          #:cursor [cursor 0]
                                          #:pattern [pattern #f]
                                          #:limit [limit #f])
  (->* (redis? redis-key/c)
       (#:cursor exact-nonnegative-integer?
        #:pattern (or/c false/c redis-string/c)
        #:limit (or/c false/c exact-positive-integer?))
       (values exact-nonnegative-integer? (listof (cons/c redis-string/c real?))))

  (define res
    (apply redis-emit! client "ZSCAN" key (number->string cursor) (optionals
                                                                   [pattern "MATCH" pattern]
                                                                   [limit "COUNT" (number->string limit)])))

  (values (bytes->number (car res))
          (for/list ([(mem score) (in-twos (cadr res))])
            (cons mem (bytes->number score)))))

(define in-redis-zset (make-scanner-sequence redis-zset-scan))
(provide in-redis-zset)

;; ZSCORE key member
(define-simple-command (zset-score [key redis-key/c] [mem redis-string/c])
  #:command ("ZSCORE")
  #:result-contract (or/c false/c real?)
  #:result-name res
  (and res (bytes->number res)))

;; ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
(define/contract/provide (redis-zset-union! client dest
                                            #:weights [weights #f]
                                            #:aggregate [aggregate #f]
                                            . keys)
  (->i ([client redis?]
        [dest redis-key/c])
       (#:weights [weights (non-empty-listof real?)]
        #:aggregate [aggregate (or/c 'sum 'min 'max)])
       #:rest [keys (non-empty-listof redis-key/c)]

       #:pre/name (keys weights)
       "a weight for each supplied key"
       (or (unsupplied-arg? weights)
           (= (length weights)
              (length keys)))

       [result exact-nonnegative-integer?])

  (apply redis-emit!
         client "ZUNIONSTORE" dest
         (number->string (length keys))
         (append keys (optionals
                       [weights "WEIGHTS" (map number->string weights)]
                       [aggregate "AGGREGATE" (string-upcase (symbol->string aggregate))]))))


;; common helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define bytes->number
  (compose1 string->number bytes->string/utf-8))

(define supplied-arg?
  (compose1 not unsupplied-arg?))

(define (exclusive-args a b)
  (cond
    [(supplied-arg? a) (unsupplied-arg? b)]
    [(supplied-arg? b) (unsupplied-arg? a)]
    [else #t]))

(define-syntax-rule (optional* p arg ...)
  (if p (list arg ...) null))

(define-syntax-rule (optionals (p arg ...) ...)
  (flatten (list (optional* p arg ...) ...)))
