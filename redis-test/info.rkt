#lang info

(define license 'MIT)
(define collection "tests")
(define deps '())
(define build-deps '("base"
                     "rackunit-lib"
                     "redis-lib"))
(define update-implies '("redis-lib"))
