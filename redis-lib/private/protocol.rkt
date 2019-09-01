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

(define string-prefix #"$")
(define array-prefix #"*")
(define integer-prefix #":")
(define clrf #"\r\n")

(define (redis-write v [out (current-output-port)])
  (cond
    [(string? v)
     (display string-prefix out)
     (display (string-length v) out)
     (display clrf out)
     (display v out)
     (display clrf out)]

    [(bytes? v)
     (display string-prefix out)
     (display (bytes-length v) out)
     (display clrf out)
     (display v out)
     (display clrf out)]

    [(list? v)
     (display array-prefix out)
     (display (length v) out)
     (display clrf out)
     (for ([item (in-list v)])
       (redis-write item out))]

    [(integer? v)
     (display integer-prefix out)
     (display v out)
     (display clrf out)]

    [else
     (raise-argument-error 'redis-write "(or/c bytes? string? list?)" v)]))
