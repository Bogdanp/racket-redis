#lang racket/base

(require (for-syntax racket/base
                     racket/provide-transform
                     syntax/stx)
         racket/contract
         "private/client.rkt"
         "private/error.rkt"
         "private/pool.rkt"
         "private/script.rkt")

(define/contract current-redis-client
  (parameter/c (or/c false/c redis?))
  (make-parameter #f))

(define/contract current-redis-pool
  (parameter/c (or/c false/c redis-pool?))
  (make-parameter #f))

(define (compose-with-implicit-pool f)
  (make-keyword-procedure
   (lambda (kws kw-args . args)
     (cond
       [(current-redis-client)
        => (lambda (client)
             (keyword-apply f kws kw-args client args))]

       [else
        (define pool (current-redis-pool))
        (unless pool
          (raise-user-error (object-name f) "no current redis pool or client"))

        (call-with-redis-client pool
          (lambda (client)
            (parameterize ([current-redis-client client])
              (keyword-apply f kws kw-args client args))))]))))

(define-syntax easy-version-out
  (make-provide-pre-transformer
   (lambda (stx modes)
     (syntax-case stx ()
       [(_ id ...)
        (with-syntax ([(wrapped-id ...) (stx-map
                                         syntax-local-lift-expression
                                         #'((compose-with-implicit-pool id) ...))])
          (pre-expand-export #'(rename-out [wrapped-id id] ...) modes))]))))

(provide
 (all-from-out "private/error.rkt")
 (all-from-out "private/pool.rkt")

 make-redis
 redis?
 redis-connected?
 redis-connect!
 redis-disconnect!

 current-redis-client
 current-redis-pool

 ;; connection commands
 (easy-version-out
  redis-auth!
  redis-echo
  redis-ping
  redis-quit!
  redis-select-db!
  redis-swap-dbs!)

 ;; geo commands
 redis-latitude/c
 redis-longitude/c
 redis-geo/c
 redis-geo-unit/c

 (easy-version-out
  redis-geo-add!
  redis-geo-dist
  redis-geo-hash
  redis-geo-pos)

 ;; hash commands
 (easy-version-out
  redis-hash-get
  redis-hash-has-key?
  redis-hash-incr!
  redis-hash-keys
  redis-hash-length
  redis-hash-ref
  redis-hash-remove!
  redis-hash-scan
  redis-hash-set!
  redis-hash-string-length
  redis-hash-values
  in-redis-hash)

 ;; hyperloglog commands
 (easy-version-out
  redis-hll-add!
  redis-hll-count
  redis-hll-merge!)

 ;; key commands
 (easy-version-out
  redis-count-keys
  redis-expire-at!
  redis-expire-in!
  redis-has-key?
  redis-key-ttl
  redis-key-type
  redis-keys
  redis-move-key!
  redis-persist!
  redis-random-key
  redis-remove!
  redis-rename!
  redis-scan
  redis-touch!
  in-redis)

 ;; list commands
 (easy-version-out
  redis-list-append!
  redis-list-get
  redis-list-insert!
  redis-list-length
  redis-list-pop-left!
  redis-list-pop-right!
  redis-list-prepend!
  redis-list-ref
  redis-list-remove!
  redis-list-set!
  redis-list-trim!
  redis-sublist)

 ;; pubsub commands
 redis-pubsub?
 redis-pubsub-kill!
 redis-pubsub-publish!
 redis-pubsub-subscribe!
 redis-pubsub-unsubscribe!

 (easy-version-out
  make-redis-pubsub
  call-with-redis-pubsub)

 ;; script commands
 (easy-version-out
  make-redis-script
  redis-script-eval!
  redis-script-eval-sha!
  redis-script-exists?
  redis-script-kill!
  redis-script-load!
  redis-scripts-flush!)

 ;; server commands
 (easy-version-out
  redis-client-id
  redis-client-name
  redis-flush-all!
  redis-flush-db!
  redis-key-count
  redis-rewrite-aof!
  redis-rewrite-aof/async!
  redis-save!
  redis-save/async!
  redis-set-client-name!
  redis-time)

 ;; set commands
 (easy-version-out
  redis-set-add!
  redis-set-count
  redis-set-difference
  redis-set-difference!
  redis-set-intersect
  redis-set-intersect!
  redis-set-member?
  redis-set-members
  redis-set-move!
  redis-set-pop!
  redis-set-random-ref
  redis-set-remove!
  redis-set-scan
  redis-set-union
  redis-set-union!
  in-redis-set)

 ;; sorted set commands
 (easy-version-out
  redis-zset-add!
  redis-zset-count
  redis-zset-count/lex
  redis-zset-incr!
  redis-zset-intersect!
  redis-zset-pop/max!
  redis-zset-pop/min!
  redis-zset-rank
  redis-zset-remove!
  redis-zset-remove/lex!
  redis-zset-remove/rank!
  redis-zset-remove/score!
  redis-zset-scan
  redis-zset-score
  redis-zset-union!
  redis-subzset
  redis-subzset/lex
  redis-subzset/score
  in-redis-zset)

 ;; stream commands
 (struct-out redis-stream-entry)
 (struct-out redis-stream-entry/pending)
 (struct-out redis-stream-info)
 (struct-out redis-stream-group)
 (struct-out redis-stream-consumer)

 (easy-version-out
  redis-stream-ack!
  redis-stream-add!
  redis-stream-consumer-remove!
  redis-stream-consumers
  redis-stream-get
  redis-stream-group-create!
  redis-stream-group-read!
  redis-stream-group-remove!
  redis-stream-group-set-id!
  redis-stream-groups
  redis-stream-length
  redis-stream-read!
  redis-stream-remove!
  redis-stream-trim!
  redis-substream
  redis-substream/group)

 ;; bytestring commands
 (easy-version-out
  redis-bytes-append!
  redis-bytes-bitcount
  redis-bytes-bitwise-and!
  redis-bytes-bitwise-not!
  redis-bytes-bitwise-or!
  redis-bytes-bitwise-xor!
  redis-bytes-copy!
  redis-bytes-decr!
  redis-bytes-get
  redis-bytes-incr!
  redis-bytes-length
  redis-bytes-ref/bit
  redis-bytes-set!
  redis-bytes-set/bit!
  redis-subbytes))
