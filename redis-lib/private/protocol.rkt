#lang racket/base

(require racket/contract
         racket/function
         racket/match
         racket/port)

(provide
 redis-value/c
 redis-null
 redis-null?
 redis-write
 redis-read)

(module+ test
  (require rackunit))


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

(define (redis-write-simple-string s out)
  (display "+" out)
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

(module+ test
  (define (redis-write/string v)
    (call-with-output-string
      (curry redis-write v)))

  (define (redis-write-simple/string v)
    (with-output-to-string
      (lambda _
        (redis-write-simple-string v))))

  (check-equal? (redis-write/string "")         "$0\r\n\r\n")
  (check-equal? (redis-write/string "OK")       "$2\r\nOK\r\n")
  (check-equal? (redis-write/string "foobar")   "$6\r\nfoobar\r\n")
  (check-equal? (redis-write/string "hello\n")  "$6\r\nhello\n\r\n")
  (check-equal? (redis-write/string #"")        "$0\r\n\r\n")
  (check-equal? (redis-write/string #"OK")      "$2\r\nOK\r\n")
  (check-equal? (redis-write/string #"foobar")  "$6\r\nfoobar\r\n")
  (check-equal? (redis-write/string #"hello\n") "$6\r\nhello\n\r\n")

  (check-equal? (redis-write/string 0)    ":0\r\n")
  (check-equal? (redis-write/string 1)    ":1\r\n")
  (check-equal? (redis-write/string 1024) ":1024\r\n")

  (check-equal? (redis-write/string (list #"foo" #"bar")) "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))


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
(define bulk-string-prefix-re #rx"^\\$([^\r]+)\r\n")
(define integer-re            #rx"^\\:(\\-?(0|[1-9][0-9]*))\r\n")
(define array-re              #rx"^\\*(\\-?(0|[1-9][0-9]*))\r\n")
(define error-re              #rx"^\\-([^\r]*)\r\n")

(define (redis-read-simple-string in)
  (match (regexp-match simple-string-re in)
    [(list _ s) (bytes->string/utf-8 s)]
    [#f (raise-argument-error 'redis-read-simple-string "a valid simple string response from Redis" in)]))

(define (redis-read-bulk-string in)
  (match (regexp-match bulk-string-prefix-re in)
    [(list _ #"-1")
     (redis-null)]

    [(list _ n:str)
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

(module+ test
  (check-equal? (redis-read (open-input-string "+\r\n")) "")
  (check-equal? (redis-read (open-input-string "+OK\r\n")) "OK")
  (check-exn
   exn:fail:contract?
   (lambda _
     (redis-read (open-input-string ""))))

  (check-equal? (redis-read (open-input-string "$-1\r\n\r\n")) (redis-null))
  (check-equal? (redis-read (open-input-string "$0\r\n\r\n")) #"")
  (check-equal? (redis-read (open-input-string "$5\r\nhello\r\n")) #"hello")

  (check-equal? (redis-read (open-input-string ":0\r\n")) 0)
  (check-equal? (redis-read (open-input-string ":1024\r\n")) 1024)
  (check-equal? (redis-read (open-input-string ":-1024\r\n")) -1024)
  (check-exn
   exn:fail:contract?
   (lambda _
     (redis-read (open-input-string ":01\r\n"))))


  (check-equal? (redis-read (open-input-string "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
                (list #"foo" #"bar"))

  (check-equal? (redis-read (open-input-string "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"))
                (list (list 1 2 3) (list "Foo" (cons 'err "Bar"))))

  (check-equal? (redis-read (open-input-string "-ERR Fatal\r\n")) (cons 'err "ERR Fatal")))
