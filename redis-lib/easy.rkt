#lang racket/base

(require (for-syntax racket/base
                     racket/provide-transform
                     syntax/stx)
         racket/contract
         "private/client.rkt"
         "private/pool.rkt"
         "private/script.rkt")

(define/contract current-redis-pool
  (parameter/c (or/c false/c redis-pool?))
  (make-parameter #f))

(define (compose-with-implicit-pool f)
  (make-keyword-procedure
   (lambda (kws kw-args . args)
     (define pool (current-redis-pool))
     (unless pool
       (raise-user-error (object-name f) "no current redis pool"))

     (call-with-redis-client pool
       (lambda (client)
         (keyword-apply f kw-args kws client args))))))

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
 (all-from-out "private/pool.rkt")

 make-redis
 redis?
 redis-connected?
 redis-connect!
 redis-disconnect!
 current-redis-pool

 (easy-version-out
  ;; connection commands
  redis-auth!
  redis-echo
  redis-ping
  redis-quit!
  redis-select-db!
  redis-swap-dbs!

  ;; geo commands
  redis-geo-add!
  redis-geo-dist
  redis-geo-hash
  redis-geo-pos

  ;; hash commands
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

  ;; hyperloglog commands
  redis-hll-add!
  redis-hll-count
  redis-hll-merge!

  ;; key commands
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
  redis-touch!

  ;; list commands
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
  redis-sublist

  ;; pubsub commands
  make-redis-pubsub
  call-with-redis-pubsub

  ;; script commands
  make-redis-script
  redis-script-eval!
  redis-script-eval-sha!
  redis-script-exists?
  redis-script-kill!
  redis-script-load!
  redis-scripts-flush!

  ;; server commands
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
  redis-time

  ;; set commands
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
  redis-set-remove!
  redis-set-union
  redis-set-union!

  ;; stream commands
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
  redis-substream/group

  ;; bytes commands
  redis-bytes-append!
  redis-bytes-bitcount
  redis-bytes-bitwise-and!
  redis-bytes-bitwise-not!
  redis-bytes-bitwise-or!
  redis-bytes-bitwise-xor!
  redis-bytes-decr!
  redis-bytes-get
  redis-bytes-incr!
  redis-bytes-set!)

 ;; pubsub functions
 redis-pubsub?
 redis-pubsub-kill!
 redis-pubsub-publish!
 redis-pubsub-subscribe!
 redis-pubsub-unsubscribe!

 ;; stream structs
 (struct-out redis-stream-entry)
 (struct-out redis-stream-entry/pending)
 (struct-out redis-stream-info)
 (struct-out redis-stream-group)
 (struct-out redis-stream-consumer))
