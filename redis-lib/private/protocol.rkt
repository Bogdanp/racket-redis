#lang racket/base

;; read ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 redis-read)

(define (redis-read [in (current-input-port)])
  (define p (read-char in))
  (define v (read-line in 'return-linefeed))
  (case p
    [(#\$)
     (if (string=? v "-1")
         #f
         (begin0 (read-bytes (string->number v) in)
           (read-bytes 2 in)))]

    [(#\*)
     (if (string=? v "-1")
         #f
         (for/list ([_ (in-range (string->number v))])
           (redis-read in)))]

    [(#\+) v]
    [(#\:) (string->number v)]
    [(#\-) (cons 'err v)]

    [else
     (raise-argument-error 'redis-read "(or/c #\\$ #\\* #\\+ #\\: #\\-)" p)]))


;; write ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 redis-write)

(define str-prefix #"$")
(define arr-prefix #"*")
(define int-prefix #":")
(define crlf    #"\r\n")

(define (redis-write v [out (current-output-port)])
  (cond
    [(string? v)
     (redis-write (string->bytes/utf-8 v) out)]

    [(bytes? v)
     (write-bytes str-prefix out)
     (write-integer (bytes-length v) out)
     (write-bytes crlf out)
     (write-bytes v out)
     (write-bytes crlf out)]

    [(list? v)
     (write-bytes arr-prefix out)
     (write-integer (length v) out)
     (write-bytes crlf out)
     (for ([item (in-list v)])
       (redis-write item out))]

    [(integer? v)
     (write-bytes int-prefix out)
     (write-integer v out)
     (write-bytes crlf out)]

    [else
     (raise-argument-error 'redis-write "(or/c bytes? string? list?)" v)]))

(define (write-integer v out)
  (write-string (number->string v) out))
