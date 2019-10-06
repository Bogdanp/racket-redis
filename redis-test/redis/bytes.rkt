#lang racket/base

(require racket/sequence
         rackunit
         redis
         "common.rkt")

(provide
 bytes-tests)

(define bytes-tests
  (test-suite
   "bytes"

   (test-commands "bit commands"
     (check-equal? (redis-bytes-ref/bit test-client "a" 10) 0)
     (check-equal? (redis-bytes-set/bit! test-client "a" 10 1) 0)
     (check-equal? (redis-bytes-set/bit! test-client "a" 10 0) 1)
     (check-equal? (redis-bytes-ref/bit test-client "a" 9) 0))

   (test-commands "byte ranges"
     (redis-bytes-set! test-client "a" "Hello World!")
     (check-equal? (redis-bytes-copy! test-client "a" 6 "Redis") 12)
     (check-equal? (redis-bytes-get test-client "a") #"Hello Redis!"))))

(module+ test
  (require rackunit/text-ui)
  (run-tests bytes-tests))
