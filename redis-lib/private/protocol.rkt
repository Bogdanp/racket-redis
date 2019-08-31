#lang racket/base

(require racket/contract
         racket/function
         racket/match)

(provide
 redis-value/c
 redis-null
 redis-null?
 redis-write
 redis-read)


;; common ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define redis-null
  (make-parameter 'null))

(define (redis-null? v)
  (equal? (redis-null) v))

(define redis-value/c
  (make-flat-contract
   #:name 'redis-value/c
   #:first-order (lambda (v)
                   ((or/c bytes? exact-integer? (listof redis-value/c) redis-null?) v))))


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
  (for-each (curryr redis-write out) l))


;; read ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (redis-read [in (current-input-port)])
  (case (peek-char in)
    [(#\+) (redis-read-simple-string in)]
    [(#\$) (redis-read-bulk-string in)]
    [(#\:) (redis-read-integer in)]
    [(#\*) (redis-read-array in)]
    [(#\-) (redis-read-error in)]
    [else  (raise-argument-error 'redis-read "a valid response from Redis" in)]))

(define simple-string-re      #rx"^\\+([^\r]*)\r\n")
(define bulk-string-prefix-re #rx"^\\$(\\-?(0|[1-9][0-9]*))\r\n")
(define integer-re            #rx"^\\:(\\-?(0|[1-9][0-9]*))\r\n")
(define array-re              #rx"^\\*(\\-?(0|[1-9][0-9]*))\r\n")
(define error-re              #rx"^\\-([^\r]*)\r\n")

(define (redis-read-simple-string in)
  (match (regexp-match simple-string-re in)
    [(list _ s) (bytes->string/utf-8 s)]
    [#f (raise-argument-error 'redis-read-simple-string "a valid simple string response from Redis" in)]))

(define (redis-read-bulk-string in)
  (match (regexp-match bulk-string-prefix-re in)
    [(list _ #"-1" _)
     (redis-null)]

    [(list _ n:str _)
     (begin0 (read-bytes (string->number (bytes->string/utf-8 n:str)) in)
       (read-bytes 2 in))]

    [#f (raise-argument-error 'redis-read-bulk-string "a valid bulk string response from Redis" in)]))

(define (redis-read-integer in)
  (match (regexp-match integer-re in)
    [(list _ n:str _)
     (string->number (bytes->string/utf-8 n:str))]

    [#f (raise-argument-error 'redis-read-integer "a valid integer response from Redis" in)]))

(define (redis-read-array in)
  (match (regexp-match array-re in)
    [(list _ n:str _)
     (for/list ([_ (in-range (string->number (bytes->string/utf-8 n:str)))])
       (redis-read in))]

    [#f (raise-argument-error 'redis-read-array "a valid array response from Redis" in)]))

(define (redis-read-error in)
  (match (regexp-match error-re in)
    [(list _ err) (cons 'err (bytes->string/utf-8 err))]
    [#f (raise-argument-error 'redis-read-error "a valid error response from Redis" in)]))
