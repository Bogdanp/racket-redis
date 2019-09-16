#lang racket/base

(require rackunit
         redis/easy
         "common.rkt")

(provide
 easy-tests)

(define easy-tests
  (test-suite
   "easy"

   (test-commands "easy"
     (check-false (redis-bytes-get "a"))
     (check-true (redis-bytes-set! "a" "1"))
     (check-equal? (redis-bytes-get "a") #"1"))))

(module+ test
  (require rackunit/text-ui)
  (parameterize ([current-redis-pool test-pool])
    (run-tests easy-tests)))
