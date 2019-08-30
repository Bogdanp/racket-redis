#lang racket/base

(require racket/random
         redis)

(define message-set
  (list "hello!" "ola!" "howdy!" "wassup?" "servus!"))

(define client (make-redis))

(let loop ()
  (redis-stream-add! client "messages" "message" (random-ref message-set))
  (loop))
