#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/class
         racket/contract
         racket/list
         racket/math
         racket/string
         "connection.rkt"
         "protocol.rkt")


;; client ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 make-redis
 redis?)

(struct redis (connection))

(define/contract (make-redis #:host [host "127.0.0.1"]
                             #:port [port 6379]
                             #:timeout [timeout 5]
                             #:db [db 0])
  (->* ()
       (#:host non-empty-string?
        #:port (integer-in 0 65535)
        #:timeout (and/c real? positive?)
        #:db (integer-in 0 16))
       redis?)

  (define conn
    (new redis%
         [host host]
         [port port]
         [timeout timeout]
         [db db]))

  (send conn connect)
  (redis conn))


;; commands ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (redis-emit r command . args)
  (send/apply (redis-connection r) emit command args))

(begin-for-syntax
  (define (stx->command stx)
    (datum->syntax stx (string-upcase (symbol->string (syntax->datum stx)))))

  (define-syntax-class arg
    (pattern name:id
             #:with e #'name
             #:with contract #'any/c)

    (pattern [name:id contract:expr]
             #:with e #'name)

    (pattern [name:id contract:expr #:converter converter:expr]
             #:with e #'(converter name))))

(define-syntax (define/contract/provide stx)
  (syntax-parse stx
    ([_ (name:id arg ...) ctr:expr e ...+]
     #'(begin
         (define/contract (name arg ...) ctr e ...)
         (provide name)))))

(define-syntax (define-simple-command stx)
  (syntax-parse stx
    ([_ (name:id arg:arg ...)
        (~optional (~seq #:command-name command-name:expr) #:defaults ([command-name (stx->command #'name)]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'any/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(define/contract/provide (fn-name client arg.name ...)
           (-> redis? arg.contract ... res-contract)

           (define res-name
             (redis-emit client command-name arg.e ...))

           e ...)))))

(define-syntax (define-variadic-command stx)
  (syntax-parse stx
    ([_ (name:id arg:arg ... . vararg:arg)
        (~optional (~seq #:command-name command-name:expr) #:defaults ([command-name (stx->command #'name)]))
        (~optional (~seq #:result-contract res-contract:expr) #:defaults ([res-contract #'any/c]))
        (~optional (~seq #:result-name res-name:id) #:defaults ([res-name #'res]))
        (~optional (~seq e:expr ...+) #:defaults ([(e 1) (list #'res)]))]
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)])
       #'(begin
           (define/contract (fn-name client arg.name ... . vararg.name)
             (->* (redis? arg.contract ...)
                  #:rest (listof vararg.contract)
                  res-contract)

             (define res-name
               (apply redis-emit client command-name arg.e ... vararg.e))

             e ...)

           (provide fn-name))))))

(define (ok? v)
  (string=? v "OK"))

(define-syntax-rule (define-simple-command/ok e0 e ...)
  (define-simple-command e0 e ...
    #:result-contract boolean?
    #:result-name res
    (ok? res)))

(define-simple-command (ping)
  #:result-contract string?)

(define-simple-command (auth [password string?])
  #:result-contract string?)

(define-simple-command (echo [message string?])
  #:result-contract string?)

(define-simple-command/ok (select [db (integer-in 0 15) #:converter number->string]))
(define-simple-command/ok (quit))

(define-simple-command (has-key? [key string?])
  #:command-name "EXISTS"
  #:result-contract boolean?
  #:result-name res
  (equal? res (list 1)))

(define-variadic-command (has-keys . [key string?])
  #:command-name "EXISTS"
  #:result-contract exact-nonnegative-integer?)

(define/contract/provide (redis-set client key value
                                    #:expires-in [expires-in #f]
                                    #:unless-exists? [unless-exists? #f]
                                    #:when-exists? [when-exists? #f])
  (->* (redis? string? string?)
       (#:expires-in (or/c false/c exact-positive-integer?)
        #:unless-exists? boolean?
        #:when-exists? boolean?)
       boolean?)
  (ok? (apply redis-emit
              client
              "SET" key value
              (flatten (list (if expires-in
                                 (list "PX" expires-in)
                                 (list))
                             (if unless-exists?
                                 (list "NX")
                                 (list))
                             (if when-exists?
                                 (list "XX")
                                 (list)))))))

(define-simple-command (get [key string?])
  #:result-contract maybe-redis-value/c)

(module+ test
  (require rackunit)

  (define client
    (make-redis #:timeout 0.1))

  (check-true (redis-select client 0))

  (check-equal? (redis-ping client) "PONG")
  (check-equal? (redis-auth client "hunter2") "ERR Client sent AUTH, but no password is set") ;; TODO: raise exn
  (check-equal? (redis-echo client "hello") "hello")

  (check-false (redis-has-key? client "a"))
  (check-true (redis-set client "a" "1"))
  (check-false (redis-set client "b" "2" #:when-exists? #t))

  (check-equal? (redis-get client "a") "1")
  (check-equal? (redis-get client "b") (redis-null))

  (check-equal? (redis-has-keys client "a" "b") 1)

  (check-true (redis-quit client)))
