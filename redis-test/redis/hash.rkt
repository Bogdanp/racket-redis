#lang racket/base

(require rackunit
         redis
         "common.rkt")

(provide
 hash-tests)

(define hash-tests
  (test-suite
   "hash"

   (test-commands "HMSET"
     (check-exn
      exn:fail:contract?
      (lambda _
        (redis-hash-set! test-client "a" "a" "1" "b")))

     (redis-hash-set! test-client "a" "a" "1" "b" "2")
     (check-equal? (redis-hash-get test-client "a")
                   (hash #"a" #"1"
                         #"b" #"2")))))

(module+ test
  (require rackunit/text-ui)
  (run-tests hash-tests))
