#lang racket/base

(require racket/class
         racket/function
         racket/match
         racket/string
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
      (proc out)
      (flush-output out))

    (define/private (get-response)
      (define response-chan (make-channel))
      (thread
       (lambda _
         (channel-put response-chan (redis-read in))))

      (match (sync/timeout timeout response-chan)
        [#f
         (raise (exn:fail:redis:timeout
                 "timed out while waiting for response from Redis"
                 (current-continuation-marks)))]

        [(cons 'err message)
         (cond
           [(string-prefix? message "ERR")
            (raise (exn:fail:redis (substring message 4) (current-continuation-marks)))]

           [(string-prefix? message "WRONGTYPE")
            (raise (exn:fail:redis (substring message 10) (current-continuation-marks)))]

           [else
            (raise (exn:fail:redis message (current-continuation-marks)))])]

        [response response]))

    (define/public (emit cmd . args)
      (if (null? args)
          (send (lambda (out)
                  (display cmd out)
                  (display "\r\n" out)))
          (send (curry redis-write (cons cmd args))))

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
  (check-equal? (send redis emit "SET" "a-number" "1") "OK")
  (check-exn
   (lambda (e)
     (and (exn:fail:redis? e)
          (check-equal? (exn-message e) "Protocol error: expected '$', got ':'")))
   (lambda _
     (send redis emit "BITCOUNT" "a" 1 2))))
