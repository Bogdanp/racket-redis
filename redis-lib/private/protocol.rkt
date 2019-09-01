#lang racket/base

(provide
 redis-read
 redis-write)


;; read ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (redis-read [in (current-input-port)])
  (case (read-char in)
    [(#\$) (redis-read-bulk-string in)]
    [(#\*) (redis-read-array in)]
    [(#\:) (string->number (read-line in 'return-linefeed))]
    [(#\+) (read-line in 'return-linefeed)]
    [(#\-) (cons 'err (read-line in 'return-linefeed))]
    [else  (raise-argument-error 'redis-read "a valid response from Redis" in)]))

(define (redis-read-bulk-string in)
  (define n:str (read-line in 'return-linefeed))
  (cond
    [(string=? n:str "-1") #f]

    [else
     (begin0 (read-bytes (string->number n:str) in)
       (read-bytes 2 in))]))

(define (redis-read-array in)
  (define n:str (read-line in 'return-linefeed))
  (cond
    [(string=? n:str "-1") #f]
    [else
     (for/list ([_ (in-range (string->number n:str))])
       (redis-read in))]))


;; write ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (redis-write v [out (current-output-port)])
  (cond
    [(string? v)  (redis-write-bulk-string (string->bytes/utf-8 v) out)]
    [(bytes? v)   (redis-write-bulk-string v out)]
    [(integer? v) (redis-write-integer v out)]
    [(list? v)    (redis-write-array v out)]))

(define (redis-write-bulk-string s out)
  (display "$" out)
  (display (bytes-length s) out)
  (display "\r\n" out)
  (display s out)
  (display "\r\n" out))

(define (redis-write-integer n out)
  (display ":" out)
  (display n out)
  (display "\r\n" out))

(define (redis-write-array l out)
  (display "*" out)
  (display (length l) out)
  (display "\r\n" out)
  (for ([item (in-list l)])
    (redis-write item out)))
