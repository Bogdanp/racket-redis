#lang racket/base

(require review/ext
         syntax/parse/pre)

#|review: ignore|#

(provide
 should-review-syntax?
 review-syntax)

(define (should-review-syntax? stx)
  (syntax-case stx (define-command define-command/ok define-command/1 define-command*)
    [(define-command . _rest) #t]
    [(define-command/ok . _rest) #t]
    [(define-command/1 . _rest) #t]
    [(define-command* . _rest) #t]
    [_ #f]))

(define-syntax-class command-arg
  (pattern name:id
           #:do [(track-binding #'name #:check-usages? #f)])
  (pattern [name:id _ctc:expression]
           #:do [(track-binding #'name #:check-usages? #f)])
  (pattern [name:id _ctc:expression #:converter _converter:expression]
           #:do [(track-binding #'name #:check-usages? #f)]))

(define-syntax-class command-definition
  #:datum-literals (define-command define-command/ok define-command/1)
  (pattern ({~or define-command
                 define-command/ok
                 define-command/1}
             ~!
             (name:id
              {~do (push-scope)}
              _arg:command-arg ...)
             {~do (push-scope)}
             {~optional {~seq #:command (_command:expression ...+)}}
             {~optional {~seq #:result-contract _res-ctc:expression}}
             {~optional {~seq #:result-name res-name:id}}
             {~do (track-binding #'{~? res-name res} "~a" #:check-usages? #f)}
             {~do (push-scope)}
             {~optional {~seq _e:expression ...+}})
           #:do [(pop-scope)
                 (pop-scope)
                 (pop-scope)
                 (track-binding #'name "~a" #:check-usages? #f)]))

(define-syntax-class command*-definition
  #:datum-literals (define-command*)
  (pattern (define-command*
             ~!
             (name:id
              {~do (push-scope)}
              _arg:command-arg ...
              . _vararg:command-arg)
             {~do (push-scope)}
             {~optional {~seq #:command (_command:expression ...+)}}
             {~optional {~seq #:result-contract _res-ctc:expression}}
             {~optional {~seq #:result-name res-name:id}}
             {~do (track-binding #'{~? res-name res} "~a" #:check-usages? #f)}
             {~do (push-scope)}
             {~optional {~seq _e:expression ...+}})
           #:do [(pop-scope)
                 (pop-scope)
                 (pop-scope)
                 (track-binding #'name "~a" #:check-usages? #f)]))

(define (review-syntax stx)
  (syntax-parse stx
    [d:command-definition #'d]
    [D:command*-definition #'D]
    [_ (track-error stx "expected command definition")]))
