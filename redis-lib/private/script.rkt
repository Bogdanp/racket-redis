#lang racket/base

(require racket/contract/base
         "client.rkt"
         "error.rkt")

(provide
 redis-script/c
 (contract-out
  [make-redis-script
   (-> redis? redis-string/c redis-script/c)]))

(define redis-script/c
  (->* [redis?]
       [#:keys (listof redis-key/c)
        #:args (listof redis-string/c)]
       redis-value/c))

(define (make-redis-script client lua-script)
  (define script-sha1
    (redis-script-load! client lua-script))
  (lambda (script-client
           #:keys [keys null]
           #:args [args null])
    (let loop ([attempts 0])
      (with-handlers ([(lambda (e)
                         (and (exn:fail:redis:script:missing? e)
                              (zero? attempts)))
                       (lambda (_)
                         (redis-script-load! script-client lua-script)
                         (loop (add1 attempts)))])
        (redis-script-eval-sha!
         script-client script-sha1
         #:keys keys
         #:args args)))))
