#lang racket/base

(require racket/format
         racket/random
         redis)

(define client (make-redis))
(define message-set
  (list "hello!" "ola!" "howdy!" "wassup?" "servus!"))

(define start-time (current-seconds))
(define produced 0)

(void
 (thread
  (lambda ()
    (let loop ()
      (sleep 1)
      (define delta (- (current-seconds) start-time))
      (displayln (format "produced: ~a rate: ~a/s" produced (~r (/ produced delta))))
      (loop)))))

(let loop ()
  (redis-stream-add! client "messages" "message" (random-ref message-set))
  (set! produced (add1 produced))
  (loop))
