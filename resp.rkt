#lang racket
(provide
 (contract-out
  [redis-encode (-> any/c (or/c redis-simple-string?
                                redis-bulk-string?
                                redis-integer?
                                redis-array?))]
  [redis-encode-simple-string (-> string? string?)]
  [redis-encode-error (-> string? string?)]
  [redis-encode-integer (-> integer? string?)]
  [redis-encode-bulk-string (-> (or/c bytes? string?) string?)]
  [redis-encode-array (-> list? string?)]
  [redis-null-bulk-string string?]
  [redis-simple-string? (-> string? boolean?)]
  [redis-error? (-> string? boolean?)]
  [redis-integer? (-> string? boolean?)]
  [redis-bulk-string? (-> string? boolean?)]
  [redis-array? (-> string? boolean?)]
  [redis-decode (-> (or/c redis-simple-string?
                                redis-bulk-string?
                                redis-integer?
                                redis-array?
                                redis-error?)
                    (or/c string? integer? list?))]
  [redis-decode-simple-string (-> redis-simple-string? string?)]
  [redis-decode-error (-> redis-error? string?)]
  [redis-decode-integer (-> redis-integer? integer?)]
  [redis-decode-bulk-string (-> redis-bulk-string? string?)]
  [redis-decode-array (-> redis-array? list?)]))

(module+ test (require rackunit))

; Char -> String
(define (char-in-string? c s)
  (let ([s-len (string-length s)])
    (let loop ([i 0])
      (if (= s-len i) #f
          (if (equal? (string-ref s i) c) #t
              (loop (add1 i)))))))

(module+ test
  (check-true (char-in-string? #\a "cba"))
  (check-false (char-in-string? #\f "akn")))

; String -> Boolean
; True if the string contains a newline character
(define (has-newline? s) (char-in-string? #\newline s))

; String -> Boolean
; True if the string contains a return character
(define (has-return? s) (char-in-string? #\return s))

; String -> Boolean
; True if the string has no newlines or returns
(define (string-ok? s) (and (not (has-newline? s))
                            (not (has-return? s))))

; Any -> String
(define (redis-encode v)
  (cond
    [(string? v) (redis-encode-bulk-string v)]
    [(bytes? v) (redis-encode-bulk-string v)]
    [(integer? v) (redis-encode-integer v)]
    [(list? v) (redis-encode-array v)]
    [else ""]))

; String -> String
(define (redis-encode-simple-string s)
  (when (or (char-in-string? #\r s) (char-in-string? #\n s))
    (raise-argument-error 'redis-encode-simple-string "string without carriage returns and line-feeds" s))
  (string-append "+" s "\r\n"))

(module+ test
  (check-equal? (redis-encode-simple-string "OK") "+OK\r\n"))

; String -> String
(define (redis-encode-error s)
  (when (or (char-in-string? #\return s)
            (char-in-string? #\newline s))
    (raise-argument-error 'redis-encode-error "string without carriage returns and line-feeds" s))
  (string-append "-" s "\r\n"))

(module+ test
  (check-equal? (redis-encode-error "Error message") "-Error message\r\n"))

; Integer -> String
(define (redis-encode-integer n)
  (unless (integer? n)
    (raise-argument-error 'redis-encode-integer "integer?" n))
  (string-append ":" (number->string n) "\r\n"))

(module+ test
  (check-equal? (redis-encode-integer 0) ":0\r\n"))

; (String or Byte-String) -> String
(define (redis-encode-bulk-string bs)
  (let ([s (if (bytes? bs) (bytes->string/utf-8 bs) bs)])
    (string-append "$" (number->string (string-length s)) "\r\n" s "\r\n")))

(module+ test
  (check-equal? (redis-encode-bulk-string (string->bytes/utf-8 "OK")) "$2\r\nOK\r\n")
  (check-equal? (redis-encode-bulk-string "foobar") "$6\r\nfoobar\r\n")
  (check-equal? (redis-encode-bulk-string "") "$0\r\n\r\n"))

; List -> String
(define (redis-encode-array l)
  (unless (list? l)
    (raise-argument-error 'redis-encode-integer "list?" l))
  (string-append "*" (number->string (length l)) "\r\n"
                 (foldr string-append "" (map redis-encode l))))

(module+ test
  (check-equal? (redis-encode-array (list "foo"
                                          "bar"))
                "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))

; the null bulk string is used to signal the non-existence of a value
(define redis-null-bulk-string "$-1\r\n") 
; 
; String Char -> Boolean
; returns true if the given string is prefixed with the given character
(define (string-prefixed? s c)
  (eq? c (string-ref s 0)))

(module+ test
  (check-true (string-prefixed? "abc" #\a)))

; Any -> Boolean
(define (redis-value? v)
  (or (redis-simple-string? v)
      (redis-bulk-string? v)
      (redis-integer? v)
      (redis-error? v)
      (redis-array? v)))

; String -> Boolean
(define (redis-simple-string? s)
  (string-prefixed? s #\+))

(module+ test
  (check-true (redis-simple-string? (redis-encode-simple-string "abc"))))

; String -> Boolean
(define (redis-error? s)
  (string-prefixed? s #\-))

(module+ test
  (check-true (redis-error? (redis-encode-error "abc"))))

; String -> Boolean
(define (redis-integer? s)
  (string-prefixed? s #\:))

(module+ test
  (check-true (redis-integer? (redis-encode-integer 2))))

; String -> Boolean
(define (redis-bulk-string? s)
  (string-prefixed? s #\$))

(module+ test
  (check-true (redis-bulk-string? redis-null-bulk-string))
  (check-true (redis-bulk-string? (redis-encode-bulk-string "WHAT A TEST"))))

; String -> Boolean
(define (redis-array? a)
  (string-prefixed? a #\*))

(module+ test
  (check-true (redis-array? (redis-encode-array (list 2)))))

; String -> String
(define (redis-decode-simple-string s)
  (unless (redis-simple-string? s)
    (raise-argument-error 'redis-decode-simple-string "redis-simple-string?" s))
  (string-trim (string-trim s "\r\n" #:left? #f) "+" #:right? #f))

(module+ test
  (let ([s "Hello, Redis!"])
    (check-equal? (redis-decode-simple-string
                   (redis-encode-simple-string s))
                  s)))

; String -> String
(define (redis-decode-error e)
  (unless (redis-error? e)
    (raise-argument-error 'redis-decode-error "redis-error?" e))
  (string-trim (string-trim e "\r\n" #:left? #f) "-" #:right? #f))

(module+ test
  (let ([e "FATAL ERROR: Core Trumped"])
    (check-equal? (redis-decode-error
                   (redis-encode-error e))
                  e)))

; String -> Integer
(define (redis-decode-integer i)
  (unless (redis-integer? i)
    (raise-argument-error 'redis-integer "redis-integer?" i))
  (string->number (string-trim (string-trim i "\r\n" #:left? #f) ":" #:right? #f)))

(module+ test
  (let ([i 1729])
    (check-equal? (redis-decode-integer
                   (redis-encode-integer i))
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
                   (redis-encode-bulk-string bs))
                  bs)))

; String -> Any
(define (redis-decode v)
  (cond
    [(redis-simple-string? v) (redis-decode-simple-string v)]
    [(redis-bulk-string? v) (redis-decode-bulk-string v)]
    [(redis-integer? v) (redis-decode-integer v)]
    [(redis-array? v) (redis-decode-array v)]
    [(redis-error? v) (redis-decode-error v)]
    [else (raise-argument-error 'redis-decode
                                "redis-value?"
                                v)]))

; List -> String
(define (redis-decode-array a)
  (define (group-bulk-strings parts)
    (let loop ([ps parts] [result null])
      (if (null? ps) result
          (if (string-prefixed? (first ps) #\$)
              (loop (rest (rest ps)) (append result (list (string-append (first ps) "\r\n" (second ps) "\r\n"))))
              (loop (rest ps) (append result (list (first ps))))))))
  
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