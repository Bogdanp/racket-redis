#lang racket/base

(require rackunit
         redis
         "common.rkt")

(define set-tests
  (test-suite
   "set"

   (test-commands "all the set commands"
     (redis-set-add! test-client "a" "1")
     (check-true (redis-set-member? test-client "a" "1"))
     (check-false (redis-set-member? test-client "a" "2"))
     (check-equal? (redis-set-members test-client "a") '(#"1"))

     (redis-set-add! test-client "a" "2")
     (check-equal? (sort (redis-set-members test-client "a") bytes<?) '(#"1" #"2"))

     (check-equal? (redis-set-count test-client "a") 2)
     (redis-set-remove! test-client "a" "2")
     (check-equal? (redis-set-members test-client "a") '(#"1"))
     (check-equal? (redis-set-count test-client "a") 1)

     (redis-set-add! test-client "b" "1" "2")
     (check-equal? (sort (redis-set-union test-client "a" "b") bytes<?) '(#"1" #"2"))
     (check-equal? (sort (redis-set-intersect test-client "a" "b") bytes<?) '(#"1"))
     (check-equal? (sort (redis-set-difference test-client "a" "b") bytes<?) '())
     (check-equal? (sort (redis-set-difference test-client "b" "a") bytes<?) '(#"2"))

     (let-values ([(cursor members) (redis-set-scan test-client "b")])
       (check-equal? members '(#"1" #"2"))))))

(module+ test
  (require rackunit/text-ui)
  (run-tests set-tests))
