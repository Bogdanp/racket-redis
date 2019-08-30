#lang racket/base

(require (for-syntax racket/base
                     racket/list
                     racket/syntax
                     syntax/parse)
         scribble/manual)

(provide
 defcmd)

(define-syntax (defcmd stx)
  (syntax-parse  stx
    [(_ ((command:id ...)
         (name:id arg ...)
         res-contract:expr)
        pre-flow ...)
     (with-syntax ([fn-name (format-id #'name "redis-~a" #'name)]
                   [(command:str ...) (datum->syntax #'(command ...)
                                                     (add-between
                                                      (map symbol->string
                                                           (syntax->datum #'(command ...)))
                                                      ", "))])
       #'(defproc
           (fn-name arg ...)
           res-contract
           pre-flow ...
           (para (emph "Commands used by this function: ") (exec command:str) ...)))]

    [(_ (name:id arg ...)
        res-contract:expr
        pre-flow ...)
     #'(defcmd (() (name arg ...) res-contract) pre-flow ...)]))
