#lang info
(define collection "rackdis")
(define deps '("base"
               "rackunit-lib"))
(define build-deps '("scribble-lib" "racket-doc"))
(define scribblings '(("scribblings/rackdis.scrbl" ())))
(define pkg-desc "Redis bindings")
(define version "0.0")
(define pkg-authors '(eu90h))
