#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         scribble/manual)

(provide
 defcmd)

(define-syntax (defcmd stx)
  (syntax-parse  stx
    [(_ (name:id arg ...)
        res-contract:expr
        (~optional (~seq pre-flow ...) #:defaults ([(pre-flow 1) null])))
     (with-syntax ([fn-name (format-id #'name "racket-~a" #'name)])
       #'(defproc
           (fn-name arg ...)
           res-contract
           pre-flow ...))]))
