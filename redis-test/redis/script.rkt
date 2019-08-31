#lang racket/base

(require rackunit
         redis
         "common.rkt")

(provide
 script-tests)

(define script-tests
  (test-suite
   "script"

   (test-case "can define and use script functions"
     (define s1 (make-redis-script test-client "return 1"))
     (define s2 (make-redis-script test-client "return {ARGV[1], ARGV[2], 3}"))
     (check-equal? (s1 test-client) 1)
     (check-equal? (s2 test-client #:args '("a" "b")) '(#"a" #"b" 3)))))

(module+ test
  (require rackunit/text-ui)
  (run-tests script-tests))
