#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/port
         racket/sequence
         racket/string)

(provide
 (contract-out
  [redis-null (parameter/c any/c)]
  [redis-null? (-> any/c boolean?)]
  [redis-result? (-> any/c boolean?)]
  [maybe-redis-value/c (-> any/c boolean?)]
  [redis-encode (-> redis-value/c void?)]
  [redis-decode (-> redis-result? maybe-redis-value/c)]))

(module+ test
  (require rackunit))


;; common ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define NULL-BULK-STRING
  "$-1\r\n")

(define redis-null
  (make-parameter 'null))

(define (redis-null? v)
  (equal? (redis-null) v))

(define redis-value/c
  (make-flat-contract
   #:name 'redis-value/c
   #:first-order (lambda (v)
                   ((or/c string? exact-integer? (listof redis-value/c)) v))))

(define maybe-redis-value/c
  (or/c redis-value/c redis-null?))


;; encode ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (redis-encode v)
  (cond
    [(string? v)
     (redis-encode-bulk-string v)]

    [(bytes? v)
     (redis-encode-bulk-string v)]

    [(exact-integer? v)
     (redis-encode-integer v)]

    [(list? v)
     (redis-encode-array v)]))

(define (redis-encode-bulk-string bs-or-s)
  (define s
    (if (bytes? bs-or-s)
        (bytes->string/utf-8 bs-or-s)
        bs-or-s))

  (display "$")
  (display (string-length s))
  (display "\r\n")
  (display s)
  (display "\r\n"))

(define (redis-encode-simple-string s)
  (display "+")
  (display s)
  (display "\r\n"))

(define (redis-encode-integer n)
  (display ":")
  (display n)
  (display "\r\n"))

(define (redis-encode-array l)
  (display "*")
  (display (length l))
  (display "\r\n")
  (for-each redis-encode l))

(module+ test
  (define (redis-encode/string v)
    (with-output-to-string
      (lambda _
        (redis-encode v))))

  (define (redis-encode-simple/string v)
    (with-output-to-string
      (lambda _
        (redis-encode-simple-string v))))

  (check-equal? (redis-encode/string "")                    "$0\r\n\r\n")
  (check-equal? (redis-encode/string "OK")                  "$2\r\nOK\r\n")
  (check-equal? (redis-encode/string "foobar")              "$6\r\nfoobar\r\n")
  (check-equal? (redis-encode/string "hello\n")             "$6\r\nhello\n\r\n")
  (check-equal? (redis-encode/string #"")                   "$0\r\n\r\n")
  (check-equal? (redis-encode/string #"OK")                 "$2\r\nOK\r\n")
  (check-equal? (redis-encode/string #"foobar")             "$6\r\nfoobar\r\n")
  (check-equal? (redis-encode/string #"hello\n")            "$6\r\nhello\n\r\n")

  (check-equal? (redis-encode/string 0)    ":0\r\n")
  (check-equal? (redis-encode/string 1)    ":1\r\n")
  (check-equal? (redis-encode/string 1024) ":1024\r\n")

  (check-equal? (redis-encode/string (list #"foo" #"bar")) "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))


;; decode ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; String Char -> Boolean
; returns true if the given string is prefixed with the given character
(define (string-prefixed? s c)
  (eq? c (string-ref s 0)))

(module+ test
  (check-true (string-prefixed? "abc" #\a)))

; Any -> Boolean
(define (redis-encoded? v)
  (or (redis-simple-string? v)
      (redis-bulk-string? v)
      (redis-integer? v)
      (redis-array? v)))

(define (redis-result? v)
  (or (redis-encoded? v)
      (redis-error? v)))

; String -> Boolean
(define (redis-simple-string? s)
  (string-prefixed? s #\+))

(module+ test
  (check-true (redis-simple-string? (redis-encode-simple/string "abc"))))

; String -> Boolean
(define (redis-error? s)
  (string-prefixed? s #\-))

(module+ test
  (check-true (redis-error? "-abc\r\n")))

; String -> Boolean
(define (redis-integer? s)
  (string-prefixed? s #\:))

(module+ test
  (check-true (redis-integer? (redis-encode/string 2))))

; String -> Boolean
(define (redis-bulk-string? s)
  (string-prefixed? s #\$))

(module+ test
  (check-true (redis-bulk-string? NULL-BULK-STRING))
  (check-true (redis-bulk-string? (redis-encode/string #"WHAT A TEST"))))

; String -> Boolean
(define (redis-array? a)
  (string-prefixed? a #\*))

(module+ test
  (check-true (redis-array? (redis-encode/string (list 2)))))

; String -> String
(define (redis-decode-simple-string s)
  (unless (redis-simple-string? s)
    (raise-argument-error 'redis-decode-simple-string "redis-simple-string?" s))
  (string-trim (string-trim s "\r\n" #:left? #f) "+" #:right? #f))

(module+ test
  (let ([s "hello"])
    (check-equal? (redis-decode-simple-string
                   (redis-encode-simple/string s))
                  s)))

; String -> String
(define (redis-decode-error e)
  (unless (redis-error? e)
    (raise-argument-error 'redis-decode-error "redis-error?" e))
  (string-trim (string-trim e "\r\n" #:left? #f) "-" #:right? #f))

(module+ test
  (let ([e "FATAL ERROR: Core Trumped"])
    (check-equal? (redis-decode-error
                   "-FATAL ERROR: Core Trumped\r\n")
                  e)))

; String -> Integer
(define (redis-decode-integer i)
  (unless (redis-integer? i)
    (raise-argument-error 'redis-integer "redis-integer?" i))
  (string->number (string-trim (string-trim i "\r\n" #:left? #f) ":" #:right? #f)))

(module+ test
  (let ([i 1729])
    (check-equal? (redis-decode-integer
                   (redis-encode/string i))
                  i)))

; String -> String
(define (redis-decode-bulk-string bs)
  (unless (redis-bulk-string? bs)
    (raise-argument-error 'redis-decode-bulk-string "redis-bulk-string?" bs))
  (second (string-split
           (string-trim (string-trim bs "\r\n" #:left? #f) "+" #:right? #f)
           "\r\n")))

(module+ test
  (let ([bs "Hello, Redis!"])
    (check-equal? (redis-decode-bulk-string
                   (redis-encode/string (string->bytes/utf-8 bs)))
                  bs)))

; String -> Any
(define (redis-decode v)
  (cond
    [(redis-simple-string? v)
     (redis-decode-simple-string v)]

    [(redis-bulk-string? v)
     (redis-decode-bulk-string v)]

    [(redis-integer? v)
     (redis-decode-integer v)]

    [(redis-array? v)
     (redis-decode-array v)]

    [(redis-error? v)
     (redis-decode-error v)]

    [else
     (raise-argument-error 'redis-decode "redis-result?" v)]))

; List -> String
(define (redis-decode-array a)
  (define (group-bulk-strings parts)
    (let loop ([ps parts] [result null])
      (if (null? ps) result
          (if (and (not (equal? (first ps) "")) (string-prefixed? (first ps) #\$))
              (loop (rest (rest ps)) (append result (list (string-append (first ps) "\r\n" (second ps) "\r\n"))))
              (loop (rest ps) (if (equal? "" (first ps)) result
                                  (append result (list (first ps)))))))))

  (define (build-sub-array parts size)
    (let* ([tmp-elts (add-between (take (rest parts) size) "\r\n")]
           [tmp (string-append "*" (number->string size) "\r\n" (foldr string-append "" tmp-elts))])
      (redis-decode-array tmp)))

  (let* ([parts  (group-bulk-strings (string-split a "\r\n"))]
         [num-elts (string->number (string-trim (first parts) "*" #:right? #f))])
    (let build-result ([ps (rest parts)] [result null])
      (if (null? ps) result
          (if (string-prefixed? (first ps) #\*)
              (let* ([n (string->number (string-trim (first ps) "*" #:right? #f))])
                (build-result (drop (rest ps) n) (append result (list (build-sub-array ps n)))))
              (build-result (rest ps) (append result (list (redis-decode (first ps))))))))))

(module+ test
  (check-equal? (redis-decode-array
                 "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
                (list "foo" "bar"))
  (check-equal? (redis-decode-array
                 "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n")
                (list (list 1 2 3) (list "Foo" "Bar"))))


;; the help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (string-has-char? s c)
  (sequence-ormap (curry char=? c) (in-string s)))

(module+ test
  (check-true (string-has-char? "cba" #\a))
  (check-false (string-has-char? "akn" #\f)))
