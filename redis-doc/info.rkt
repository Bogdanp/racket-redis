#lang info

(define collection "redis")
(define deps '("base"))
(define build-deps '("racket-doc"
                     "redis-lib"
                     "scribble-lib"))
(define update-implies '("redis-lib"))
(define scribblings '(("scribblings/redis.scrbl")))
