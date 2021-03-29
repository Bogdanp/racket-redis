#lang racket/base

(require (for-syntax racket/base)
         rackunit
         redis)

(provide
 url-tests)

(define-syntax-rule (test-cases [url (username password host port db)] ...)
  (begin
    (check-equal?
     (call-with-values
      (lambda ()
        (parse-redis-url url))
      list)
     (list username password host port db)) ...))

(define url-tests
  (test-suite
   "parse-redis-url"

   (check-exn
    #rx"must have a host"
    (lambda ()
      (parse-redis-url "redis://")))

   (check-exn
    #rx"in the range \\[0, 16\\]"
    (lambda ()
      (parse-redis-url "redis://127.0.0.1/26")))

   (test-cases
    ["redis://127.0.0.1" (#f #f "127.0.0.1" 6379 0)]
    ["redis://127.0.0.1/2" (#f #f "127.0.0.1" 6379 2)]
    ["redis://127.0.0.1:8630/2" (#f #f "127.0.0.1" 8630 2)]
    ["redis://bogdan@127.0.0.1" ("bogdan" #f "127.0.0.1" 6379 0)]
    ["redis://bogdan:@127.0.0.1" ("bogdan" #f "127.0.0.1" 6379 0)]
    ["redis://bogdan:pass@127.0.0.1" ("bogdan" "pass" "127.0.0.1" 6379 0)]
    ["redis://bogdan:pass@127.0.0.1:8000/3" ("bogdan" "pass" "127.0.0.1" 8000 3)])))

(module+ test
  (require rackunit/text-ui)
  (run-tests url-tests))
