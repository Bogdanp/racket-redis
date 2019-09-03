#lang racket/base

(require rackunit
         redis)

(provide
 test-host
 test-client
 test-pool
 test-commands)

(define test-host
  (or (getenv "REDIS_HOST") "127.0.0.1"))

(define test-client
  (make-redis #:host test-host))

(define test-pool
  (make-redis-pool #:host test-host
                   #:pool-size 2))

(define-syntax-rule (test-commands message e0 e ...)
  (test-case message
    (redis-flush-all! test-client)
    e0 e ...))
