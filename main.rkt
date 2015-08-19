#lang racket
(provide redis%)

(require "resp.rkt" racket/tcp)

(module+ test (require rackunit))

(define redis%
  (class
      object%
    (init-field [ip "127.0.0.1"] [port 6379] [timeout 1])
    (field [out null] [in null])
    (super-new)
    
    (define/private (send msg)
      (display msg out)
      (flush-output out))
    
    (define/private (get-response)
      (let loop ([resp ""])
        (let ([p (sync/timeout timeout in)])
          (if (input-port? p)
              (let ([s (read-line p)])
                (if (eof-object? s)
                    (redis-decode (string-append resp "\n"))
                    (loop (string-append resp s "\n"))))
              (redis-decode resp)))))
    
    (define/private (apply-cmd cmd args)
      (send (redis-encode-array (append (list cmd) (if (list? args) args (list args))))))

    (define/public (set-timeout t) (set! timeout t))
    
    (define/public (ping [msg ""])
      (if (equal? msg "")
          (send "PING\r\n")
          (apply-cmd "PING" (list msg)))
      (get-response))

    (define/public (set key value)
      (apply-cmd "SET" (list key value))
      (get-response))
    
    (define/public (get key)
      (apply-cmd "GET" key)
      (get-response))

    (define/public (mget keys)
      (apply-cmd "MGET" keys)
      (get-response))
    
    (define/public (getset key value)
      (apply-cmd "GETSET" (list key value))
      (get-response))
    
    (define/public (incr key)
      (apply-cmd "INCR" key)
      (get-response))

    (define/public (incrby key value)
      (apply-cmd "INCRBY" (list key value))
      (get-response))
    
    (define/public (decr key)
      (apply-cmd "DECR" key)
      (get-response))

    (define/public (decrby key value)
      (apply-cmd "DECRBY" (list key value))
      (get-response))
    
    (define/public (del key)
      (apply-cmd "DEL" key)
      (get-response))

    (define/public (setnx key value)
      (apply-cmd "SETNX" (list key value))
      (get-response))
    
    (define/public (lpush key value)
      (apply-cmd "LPUSH" (if (list? value)
                             (append (list key) value)
                             (list key value)))
      (get-response))

    (define/public (lrange key min max)
      (apply-cmd "LRANGE" (list key min max))
      (get-response))
    
    (define/public (init)
      (define-values (i o) (tcp-connect ip port))
      (set! in i)
      (set! out o))))

(module+ test
  (define redis (new redis%))
  (send redis set-timeout 0.01) ;this is ok for local testing, probably not for the net though
  (send redis init)
  (check-equal? (send redis ping) "PONG" )
  (check-equal? (send redis ping "yo watup")
                "yo watup"))

(module+ test
  (check-equal? (send redis set "a-number" "1") "OK")
  (check-equal? (send redis get "a-number") "1")
  (check-equal? (send redis incr "a-number") 2)
  (check-equal? (send redis getset "a-number" "4") "2")
  (check-equal? (send redis get "a-number") "4"))

(module+ test
  (check-equal? (send redis set "key1" "fksd") "OK")
  (check-equal? (send redis set "key2" "fdadsf") "OK")
  (check-equal? (send redis set "key3" "bdafg") "OK")
  (check-equal? (send redis mget (list "key1" "key2" "key3"))
                (list "fksd" "fdadsf" "bdafg")))

(module+ test
  (check-true (number? (send redis lpush "some-list" "1")))
  (check-true (number? (send redis lpush "some-list" (list "1" "2" "3" "4" "5"))))
  (check-true (list? (send redis lrange "some-list" "0" "-1"))))

(module+ test
  (check-equal? (send redis del "a-number") 1)
  (check-equal? (send redis set "a" "hey") "OK")
  (check-equal? (send redis set "b" "'ello") "OK")
  (check-equal? (send redis del (list "a" "b")) 2))

(module+ test
  (check-equal? (send redis del "new-key") 1)
  (check-equal? (send redis setnx "new-key" "Hello") 1)
  (check-equal? (send redis setnx "new-key" "World") 0))

(module+ test
  (check-equal? (send redis set "a-number" "1") "OK")
  (check-equal? (send redis decr "a-number") 0)
  (check-equal? (send redis incrby "a-number" "5") 5)
  (check-equal? (send redis decrby "a-number" "5") 0))
