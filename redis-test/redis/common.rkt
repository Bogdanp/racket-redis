#lang racket/base

(require rackunit
         redis)

(provide
 test-host
 test-client
 test-commands)

(define test-host
  (or (getenv "REDIS_HOST") "127.0.0.1"))

(define test-client
  (make-redis #:host test-host))

(define-syntax-rule (test-commands message e0 e ...)
  (test-case message
    (redis-flush-all! test-client)
    e0 e ...))
