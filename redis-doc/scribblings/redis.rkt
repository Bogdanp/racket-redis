#lang racket/base

(require (for-syntax racket/base
                     racket/list
                     racket/syntax
                     syntax/parse)
         scribble/manual)

(provide
 defcmd
 defcmd*)

(begin-for-syntax
  (define (commands->strings stx)
    (datum->syntax stx
                   (add-between
                    (map symbol->string
                         (syntax->datum stx))
                    ", "))))

(define-syntax (defcmd stx)
  (syntax-parse  stx
    [(_ ((command:id ...)
         (name:id arg ...)
         res-contract:expr)
        pre-flow ...)
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)]
                   [(command:str ...) (commands->strings #'(command ...))])
       #'(defproc
           (fn-name arg ...)
           res-contract
           pre-flow ...
           (para (emph "Commands used by this function: ") (exec command:str) ...)))]

    [(_ (name:id arg ...)
        res-contract:expr
        pre-flow ...)
     #'(defcmd (() (name arg ...) res-contract) pre-flow ...)]))

(define-syntax (defcmd* stx)
  (syntax-parse stx
    [(_ ((command:id ...)
         (prototype ...))
        pre-flow ...)
     (with-syntax ([(command:str ...) (commands->strings #'(command ...))])
       #'(defproc*
           (prototype ...)
           pre-flow ...
           (para (emph "Commands used by this function: ") (exec command:str) ...)))]))
