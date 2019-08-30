#lang racket/base

(require racket/contract
         "client.rkt"
         "protocol.rkt")

(provide
 make-redis-script
 redis-script/c)

(define redis-script/c
  (->* (redis?)
       (#:keys (listof string?)
        #:args (listof string?))
       maybe-redis-value/c))

(define/contract (make-redis-script client lua-script)
  (-> redis? string? redis-script/c)
  (define script-sha1 (redis-script-load! client lua-script))
  (lambda (client
           #:keys [keys null]
           #:args [args null])
    (unless (andmap values (redis-scripts-exist? client script-sha1))
      (redis-script-load! client lua-script))

    (redis-script-eval-sha! client script-sha1
                            #:keys keys
                            #:args args)))

(module+ test
  (require rackunit)

  (define c (make-redis))
  (define s1 (make-redis-script c "return 1"))
  (define s2 (make-redis-script c "return {ARGV[1], ARGV[2], 3}"))
  (check-equal? (s1 c) 1)
  (check-equal? (s2 c #:args '("a" "b")) '(#"a" #"b" 3)))
