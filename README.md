# rackdis
Redis bindings for Racket.

Example Usage
=============
Create a redis object: `(define redis (new-redis%))`

Initialize it: `(send redis init)`

Send a command: `(send redis ping)`
