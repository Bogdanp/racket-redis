#lang racket/base

(require rackunit
         redis
         "common.rkt")

(provide
 zset-tests)

(define zset-tests
  (test-suite
   "zset"

   (test-commands "basic sorted set commands"
     (check-equal? (redis-zset-add! test-client "a" "a" 1 "b" 2) 2)
     (check-equal? (redis-zset-add! test-client "a" "a" 1) 0)
     (check-equal? (redis-zset-count test-client "a") 2)
     (check-equal? (redis-zset-count test-client "a" #:min -inf.0 #:max +inf.0) 2)
     (check-equal? (redis-zset-count test-client "a" #:min 2 #:max +inf.0) 1)
     (check-equal? (redis-zset-count test-client "a" #:min -inf.0 #:max 1) 1)
     (check-equal? (redis-zset-count test-client "a" #:min 5 #:max 5) 0)
     (check-equal? (redis-zset-score test-client "a" "a") 1)
     (check-equal? (redis-zset-score test-client "a" "b") 2)
     (check-equal? (redis-zset-incr! test-client "a" "b" 1.5) 3.5)
     (check-equal? (redis-zset-incr! test-client "a" "b" -1.0) 2.5)
     (check-equal? (redis-zset-score test-client "a" "b") 2.5)
     (check-equal? (redis-zset-remove! test-client "a" "b") 1)
     (check-equal? (redis-zset-remove! test-client "a" "b") 0)
     (check-false (redis-zset-score test-client "a" "b"))
     (check-equal? (redis-zset-count test-client "a") 1))))

(module+ test
  (require rackunit/text-ui)
  (run-tests zset-tests))
