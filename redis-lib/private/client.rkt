#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/contract
         racket/function
         racket/list
         racket/match
         racket/port
         racket/serialize
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

 redis-null?
 redis-null)

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
        #:timeout (and/c rational? positive?)
        #:db (integer-in 0 16))
       redis?)

  (define mutex (make-semaphore 1))
  (define client (redis host port timeout #f #f mutex #f #f))
  (begin0 client
    (redis-connect! client)
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
    ([_ (name:id arg ... . vararg) ctr:expr e ...+]
     #'(begin
         (define/contract (name arg ... . vararg) ctr e ...)
         (provide name)))

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

;; BITCOUNT key [start end]
(define/contract/provide (redis-string-bitcount client key
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
(define-variadic-command (remove! [key0 string?] . [keys string?])
  #:command-name "DEL"
  #:result-contract exact-nonnegative-integer?)

;; ECHO message
(define-simple-command (echo [message string?])
  #:result-contract string?
  #:result-name res
  (bytes->string/utf-8 res))

;; EVAL script numkeys [key ...] [arg ...]
(define/contract/provide (redis-eval! client script
                                      #:keys [keys null]
                                      #:args [args null])
  (->* (redis? string?)
       (#:keys (listof string?)
        #:args (listof string?))
       maybe-redis-value/c)
  (apply redis-emit! client "EVAL" script (number->string (length keys)) (append keys args)))

;; EVALSHA sha1 numkeys [key ...] [arg ...]
(define/contract/provide (redis-eval-sha! client script-sha1
                                          #:keys [keys null]
                                          #:args [args null])
  (->* (redis? string?)
       (#:keys (listof string?)
        #:args (listof string?))
       maybe-redis-value/c)
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
(define/contract/provide (redis-bytes-ref client key . keys)
  (-> redis? string? string? ... maybe-redis-value/c)
  (if (null? keys)
      (redis-emit! client "GET" key)
      (apply redis-emit! client "MGET" key keys)))

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

;; LINDEX key index
(define-simple-command (list-ref [key string?] [index exact-integer? #:converter number->string])
  #:command-name "LINDEX"
  #:result-contract maybe-redis-value/c)

;; LINSERT key BEFORE pivot value
(define/contract/provide (redis-list-insert-before! client key pivot value)
  (-> redis? string? string? string? (or/c false/c exact-nonnegative-integer?))
  (define res (redis-emit! client "LINSERT" key "BEFORE" pivot value))
  (and (not (= res -1)) res))

;; LINSERT key AFTER pivot value
(define/contract/provide (redis-list-insert-after! client key pivot value)
  (-> redis? string? string? string? (or/c false/c exact-nonnegative-integer?))
  (define res (redis-emit! client "LINSERT" key "AFTER" pivot value))
  (and (not (= res -1)) res))

;; LLEN key
(define-simple-command (list-length [key string?])
  #:command-name "LLEN"
  #:result-contract exact-nonnegative-integer?)

;; LPOP key
(define-simple-command (list-pop-left! [key string?])
  #:command-name "LPOP"
  #:result-contract maybe-redis-value/c)

;; LPUSH key value [value ...]
(define-variadic-command (list-prepend! [key string?] . [value redis-string?])
  #:command-name "LPUSH"
  #:result-contract exact-nonnegative-integer?)

;; LRANGE key value start stop
(define/contract/provide (redis-list-range client key [start 0] [stop -1])
  (->* (redis? string?)
       (exact-integer? exact-integer?)
       maybe-redis-value/c)
  (redis-emit! client "LRANGE" key (number->string start) (number->string stop)))

;; LREM key count value
(define-simple-command (list-remove! [key string?] [count exact-integer?] [value redis-string?])
  #:command-name "LREM"
  #:result-contract exact-nonnegative-integer?)

;; LSET key index value
(define-simple-command/ok (list-set! [key string?] [index exact-integer?] [value redis-string?])
  #:command-name "LSET")

;; LTRIM key value start stop
(define/contract/provide (redis-list-trim! client key [start 0] [stop -1])
  (->* (redis? string?)
       (exact-integer? exact-integer?)
       boolean?)
  (ok? (redis-emit! client "LTRIM" key (number->string start) (number->string stop))))

;; PERSIST key
(define-simple-command/1 (persist! [key string?]))

;; PEXPIRE key milliseconds
(define-simple-command/1 (expire-in! [key string?] [ms exact-nonnegative-integer? #:converter number->string])
  #:command-name "PEXPIRE")

;; PEXPIREAT key milliseconds-timestamp
(define-simple-command/1 (expire-at! [key string?] [ms exact-nonnegative-integer? #:converter number->string])
  #:command-name "PEXPIREAT")

;; PTTL key
(define-simple-command (ttl [key string?])
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

;; RPOP key
(define-simple-command (list-pop-right! [key string?])
  #:command-name "RPOP"
  #:result-contract maybe-redis-value/c)

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
(define/contract/provide (redis-flush-all-scripts! client)
  (-> redis? boolean?)
  (ok? (redis-emit! client "SCRIPT" "FLUSH")))

;; SCRIPT KILL
(define/contract/provide (redis-kill-current-script! client)
  (-> redis? boolean?)
  (ok? (redis-emit! client "SCRIPT" "KILL")))

;; SCRIPT LOAD
(define/contract/provide (redis-load-script! client script)
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

(define redis-set!
  (make-keyword-procedure
   (lambda (kws kw-args client key value . args)
     (define serialized-value
       (cond
         [(redis-string? value) value]
         [else (with-output-to-string
                 (lambda _
                   (write (serialize value))))]))

     (keyword-apply redis-bytes-set! kws kw-args client key serialized-value args))))

(provide redis-set!)

;; TOUCH key [key ...]
(define-variadic-command (touch! [key0 string?] . [key string?])
  #:result-contract exact-nonnegative-integer?)

;; TYPE key
(define-simple-command (type [key string?])
  #:result-contract (or/c 'none 'string 'list 'set 'zset 'hash 'stream)
  #:result-name res
  (string->symbol res))


(module+ test
  (require rackunit)

  (define client
    (make-redis #:timeout 0.1))

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
    (check-equal? (redis-string-bitcount client "a") 0)
    (check-true (redis-set! client "a" "hello"))
    (check-equal? (redis-string-bitcount client "a") 21))

  (test "CLIENT *"
    (check-not-false (redis-client-id client))
    (check-equal? (redis-client-name client) "racket-redis")
    (check-true (redis-set-client-name! client "custom-name"))
    (check-equal? (redis-client-name client) "custom-name"))

  (test "DBSIZE"
    (check-equal? (redis-count client) 0)
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-count client) 1))

  (test "DECR and DECRBY"
    (check-equal? (redis-bytes-decr! client "a") -1)
    (check-equal? (redis-bytes-decr! client "a") -2)
    (check-equal? (redis-bytes-decr! client "a" 3) -5)
    (check-equal? (redis-type client "a") 'string)

    (check-true (redis-set! client "a" "1.5"))
    (check-exn
     (lambda (e)
       (and (exn:fail:redis? e)
            (check-equal? (exn-message e) "value is not an integer or out of range")))
     (lambda _
       (redis-bytes-decr! client "a"))))

  (test "DEL"
    (check-equal? (redis-remove! client "a") 0)
    (check-equal? (redis-remove! client "a" "b") 0)
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-remove! client "a" "b") 1)
    (check-true (redis-set! client "a" "1"))
    (check-true (redis-set! client "b" "2"))
    (check-equal? (redis-remove! client "a" "b") 2))

  (test "EVAL"
    (check-equal? (redis-eval! client "return 1") 1)
    (check-equal? (redis-eval! client "return {KEYS[1], ARGV[1], ARGV[2]}"
                               #:keys '("a")
                               #:args '("b" "c"))
                  '(#"a" #"b" #"c")))

  (test "{M,}GET and SET"
    (check-false (redis-has-key? client "a"))
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-bytes-ref client "a") #"1")
    (check-false (redis-set! client "a" "2" #:unless-exists? #t))
    (check-equal? (redis-bytes-ref client "a") #"1")
    (check-false (redis-set! client "b" "2" #:when-exists? #t))
    (check-false (redis-has-key? client "b"))
    (check-true (redis-set! client "b" "2" #:unless-exists? #t))
    (check-true (redis-has-key? client "b"))
    (check-equal? (redis-bytes-ref client "a" "b") '(#"1" #"2")))

  (test "INCR, INCRBY and INCRBYFLOAT"
    (check-equal? (redis-bytes-incr! client "a") 1)
    (check-equal? (redis-bytes-incr! client "a") 2)
    (check-equal? (redis-bytes-incr! client "a" 3) 5)
    (check-equal? (redis-bytes-incr! client "a" 1.5) "6.5")
    (check-equal? (redis-type client "a") 'string))

  (test "LINDEX, LLEN, LPUSH, LPOP"
    (check-equal? (redis-list-length client "a") 0)
    (check-equal? (redis-list-prepend! client "a" "1") 1)
    (check-equal? (redis-list-prepend! client "a" "2" "3") 3)
    (check-equal? (redis-list-length client "a") 3)
    (check-equal? (redis-list-ref client "a" 1) #"2")
    (check-equal? (redis-list-pop-left! client "a") #"3")
    (check-equal? (redis-list-pop-left! client "a") #"2")
    (check-equal? (redis-list-pop-left! client "a") #"1")
    (check-equal? (redis-list-pop-left! client "a") (redis-null)))

  (test "LINSERT"
    (check-equal? (redis-list-prepend! client "a" "1") 1)
    (check-equal? (redis-list-prepend! client "a" "2") 2)
    (check-equal? (redis-list-insert-before! client "a" "1" "3") 3)
    (check-false (redis-list-insert-before! client "a" "8" "3"))
    (check-equal? (redis-list-range client "a") '(#"2" #"3" #"1"))
    (check-equal? (redis-list-insert-after! client "a" "3" "4") 4)
    (check-equal? (redis-list-range client "a") '(#"2" #"3" #"4" #"1")))

  (test "LTRIM"
    (check-equal? (redis-list-prepend! client "a" "2") 1)
    (check-equal? (redis-list-prepend! client "a" "2") 2)
    (check-true (redis-list-trim! client "a" 1))
    (check-equal? (redis-list-range client "a") '(#"2")))

  (test "PERSIST, PEXPIRE and PTTL"
    (check-false (redis-expire-in! client "a" 200))
    (check-equal? (redis-ttl client "a") 'missing)
    (check-true (redis-set! client "a" "1"))
    (check-equal? (redis-ttl client "a") 'persisted)
    (check-true (redis-expire-in! client "a" 20))
    (check-true (> (redis-ttl client "a") 5))
    (check-true (redis-persist! client "a"))
    (check-equal? (redis-ttl client "a") 'persisted))

  (test "RENAME"
    (check-true (redis-set! client "a" "1"))
    (check-true (redis-set! client "b" "2"))
    (check-true (redis-rename! client "a" "c"))
    (check-false (redis-has-key? client "a"))
    (check-false (redis-rename! client "c" "b" #:unless-exists? #t))
    (check-true (redis-has-key? client "c")))

  (test "RPUSH, RPOP"
    (check-equal? (redis-list-append! client "a" "1") 1)
    (check-equal? (redis-list-append! client "a" "2") 2)
    (check-equal? (redis-list-pop-right! client "a") #"2")
    (check-equal? (redis-list-pop-right! client "a") #"1")
    (check-equal? (redis-list-pop-right! client "a") (redis-null)))

  (test "TOUCH"
    (check-equal? (redis-touch! client "a") 0)
    (check-equal? (redis-touch! client "a" "b") 0)
    (check-true (redis-set! client "a" "1"))
    (check-true (redis-set! client "b" "2"))
    (check-equal? (redis-touch! client "a" "b" "c") 2))

  (check-equal? (redis-quit! client) (void)))
