#lang racket/base

(require racket/class
         racket/list
         racket/tcp
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
      (let loop ([resp ""])
        (let ([p (sync/timeout timeout in)])
          (if (input-port? p)
              (let ([s (read-line p)])
                (if (eof-object? s)
                    (if (not (equal? resp ""))
                        (redis-decode resp)
                        "ERR timed out")
                    (loop (string-append resp s "\n"))))
              (if (not (equal? resp ""))
                  (redis-decode resp)
                  "ERR timed out")))))

    (define/public (emit cmd . args)
      (if (null? args)
          (send (lambda _
                  (display cmd)
                  (display "\r\n")))
          (send (lambda _
                  (redis-encode (cons cmd args)))))

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
