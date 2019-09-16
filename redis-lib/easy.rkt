#lang racket/base

(require (for-syntax racket/base
                     racket/require-transform
                     racket/syntax)
         racket/contract
         "private/pool.rkt")

(provide
 (all-from-out "private/pool.rkt")
 current-redis-pool)

(define/contract current-redis-pool
  (parameter/c (or/c false/c redis-pool?))
  (make-parameter #f))

(define (compose-with-implicit-pool f)
  (make-keyword-procedure
   (lambda (kws kw-args . args)
     (define pool (current-redis-pool))
     (unless pool
       (raise-user-error (object-name f) "no redis pool installed"))

     (call-with-redis-client pool
       (lambda (client)
         (keyword-apply f kw-args kws client args))))))

(define-syntax (make-easy-version stx)
  (syntax-case stx ()
    [(_ id)
     (with-syntax ([orig-id (format-id #'id "orig:~a" #'id)])
       #'(define id (compose-with-implicit-pool orig-id)))]))

(define-syntax-rule (provide-easy-version id ...)
  (begin
    (begin (make-easy-version id) ...)
    (provide id ...)))

(define-syntax (provide-easy-versions stx)
  (syntax-case stx ()
    [(_ mod)
     #'(provide-easy-versions mod #:except ())]

    [(_ mod #:except (except-id ...))
     (with-syntax ([(id ...) (let-values ([(imports _) (expand-import #'mod)])
                               (define exceptions (syntax->datum #'(except-id ...)))
                               (for*/list ([imp (in-list imports)]
                                           [id (in-value (import-local-id imp))]
                                           #:unless (member (syntax->datum id) exceptions))
                                 id))])
       #'(begin
           (require (prefix-in orig: mod))
           (provide-easy-version id ...)))]))

(provide-easy-versions
 "private/client.rkt"
 #:except (make-redis
           redis?
           redis-disconnect!
           redis-connect!
           redis-string/c
           redis-key/c
           redis-value/c))

(provide-easy-versions
 "private/script.rkt"
 #:except (redis-script/c))
