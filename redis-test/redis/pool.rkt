#lang racket/base

(require rackunit
         redis
         "common.rkt")

(define pool
  (make-redis-pool #:host test-host
                   #:pool-size 2))

(define pool-tests
  (test-suite
   "pool"

   (test-suite
    "take! and release!"

    (let ([c1-fn (redis-pool-take! pool)]
          [c2-fn (redis-pool-take! pool)])
      (check-false (redis-pool-take! pool 0))
      (redis-pool-release! pool c1-fn)
      (define c1-fn* (redis-pool-take! pool))
      (check-not-false c1-fn*)
      (check-equal? c1-fn c1-fn*)
      (redis-pool-release! pool c1-fn)
      (redis-pool-release! pool c2-fn)))

   (test-suite
    "call-with-redis-client"

    (call-with-redis-client pool
      (lambda (c1)
        (check-equal? (redis-ping c1) "PONG")
        (call-with-redis-client pool
          (lambda (c2)
            (check-equal? (redis-ping c2) "PONG")

            (check-exn
             exn:fail:redis:pool:timeout?
             (lambda _
               (call-with-redis-client pool
                 #:timeout 0
                 (lambda (c3)
                   (fail "must not run")))))))

        (call-with-redis-client pool
          (lambda (c2)
            (check-equal? (redis-ping c2) "PONG"))))))))

(module+ test
  (require rackunit/text-ui)
  (run-tests pool-tests))
