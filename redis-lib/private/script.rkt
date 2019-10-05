#lang racket/base

(require racket/contract
         "client.rkt"
         "error.rkt"
         "protocol.rkt")

(provide
 make-redis-script
 redis-script/c)

(define redis-script/c
  (->* (redis?)
       (#:keys (listof redis-key/c)
        #:args (listof redis-string/c))
       redis-value/c))

(define/contract (make-redis-script client lua-script)
  (-> redis? redis-string/c redis-script/c)
  (define script-sha1 (redis-script-load! client lua-script))
  (lambda (client
           #:keys [keys null]
           #:args [args null])
    (let loop ([attempts 0])
      (with-handlers ([(lambda (e)
                         (and (exn:fail:redis:script:missing? e)
                              (zero? attempts)))
                       (lambda _
                         (redis-script-load! client lua-script)
                         (loop (add1 attempts)))])
        (redis-script-eval-sha! client script-sha1
                                #:keys keys
                                #:args args)))))
