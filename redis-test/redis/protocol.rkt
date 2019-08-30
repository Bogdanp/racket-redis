#lang racket/base

(require racket/function
         racket/port
         rackunit
         redis/private/protocol)

(provide
 protocol-tests)

(define (redis-write/string v)
  (call-with-output-string
   (curry redis-write v)))

(define protocol-tests
  (test-suite
   "protocol"

   (test-suite
    "read"

    (test-suite
     "redis-read-simple-string"

     (check-equal? (redis-read (open-input-string "+\r\n")) "")
     (check-equal? (redis-read (open-input-string "+OK\r\n")) "OK")
     (check-exn
      exn:fail:contract?
      (lambda _
        (redis-read (open-input-string "")))))

    (test-suite
     "redis-read-bulk-string"

     (check-equal? (redis-read (open-input-string "$-1\r\n\r\n")) (redis-null))
     (check-equal? (redis-read (open-input-string "$0\r\n\r\n")) #"")
     (check-equal? (redis-read (open-input-string "$5\r\nhello\r\n")) #"hello"))

    (test-suite
     "redis-read-integer"

     (check-equal? (redis-read (open-input-string ":0\r\n")) 0)
     (check-equal? (redis-read (open-input-string ":1024\r\n")) 1024)
     (check-equal? (redis-read (open-input-string ":-1024\r\n")) -1024)
     (check-exn
      exn:fail:contract?
      (lambda _
        (redis-read (open-input-string ":01\r\n")))))


    (test-suite
     "redis-read-array"

     (check-equal? (redis-read (open-input-string "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
                   (list #"foo" #"bar"))

     (check-equal? (redis-read (open-input-string "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"))
                   (list (list 1 2 3) (list "Foo" (cons 'err "Bar")))))

    (test-suite
     "redis-read-error"

     (check-equal? (redis-read (open-input-string "-ERR Fatal\r\n")) (cons 'err "ERR Fatal"))))

   (test-suite
    "write"

    (test-suite
     "redis-write-bulk-string"

     (check-equal? (redis-write/string "")         "$0\r\n\r\n")
     (check-equal? (redis-write/string "OK")       "$2\r\nOK\r\n")
     (check-equal? (redis-write/string "foobar")   "$6\r\nfoobar\r\n")
     (check-equal? (redis-write/string "hello\n")  "$6\r\nhello\n\r\n")
     (check-equal? (redis-write/string #"")        "$0\r\n\r\n")
     (check-equal? (redis-write/string #"OK")      "$2\r\nOK\r\n")
     (check-equal? (redis-write/string #"foobar")  "$6\r\nfoobar\r\n")
     (check-equal? (redis-write/string #"hello\n") "$6\r\nhello\n\r\n"))

    (test-suite
     "redis-write-integer"

     (check-equal? (redis-write/string 0)    ":0\r\n")
     (check-equal? (redis-write/string 1)    ":1\r\n")
     (check-equal? (redis-write/string 1024) ":1024\r\n"))

    (test-suite
     "redis-write-array"

     (check-equal? (redis-write/string (list #"foo" #"bar")) "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")))))

(module+ test
  (require rackunit/text-ui)
  (run-tests protocol-tests))
