#lang racket/base

(require racket/class
         racket/list
         racket/tcp
         "error.rkt"
         "protocol.rkt")

(provide
 redis%)

(define redis%
  (class object%
    (init-field [host "127.0.0.1"]
                [port 6379]
                [timeout 1]
                [db 0])

    (field [out #f]
           [in #f])

    (super-new)

    (define/private (send proc)
      (parameterize ([current-output-port out])
        (proc)
        (flush-output)))

    (define/private (get-response)
      (define response-chan (make-channel))
      (thread
       (lambda _
         (channel-put response-chan (redis-read in))))

      (or
       (sync/timeout timeout response-chan)
       (raise (exn:fail:redis:timeout
               "timed out while waiting for response from Redis"
               (current-continuation-marks)) )))

    (define/public (emit cmd . args)
      (if (null? args)
          (send (lambda _
                  (display cmd)
                  (display "\r\n")))
          (send (lambda _
                  (redis-write (cons cmd args)))))

      (get-response))

    (define/public (set-timeout t)
      (set! timeout t))

    (define/public (connect)
      (define-values (i o)
        (tcp-connect host port))

      (set! in i)
      (set! out o)

      (void (emit "SELECT" (number->string db))))))

(module+ test
  (require rackunit)

  (define redis
    (new redis%))

  (send redis set-timeout 0.1)
  (send redis connect)

  (check-equal? (send redis emit "PING") "PONG")
  (check-equal? (send redis emit "SET" "a-number" "1") "OK"))
