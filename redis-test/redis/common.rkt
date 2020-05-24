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

(define test-port
  (string->number (or (getenv "REDIS_PORT") "6379")))

(define test-client
  (make-redis #:host test-host
              #:port test-port))

(define test-pool
  (make-redis-pool #:host test-host
                   #:port test-port
                   #:pool-size 2))

(define-syntax-rule (test-commands message e0 e ...)
  (test-case message
    (redis-flush-all! test-client)
    e0 e ...))
