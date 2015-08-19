# rackdis
Redis bindings for Racket. The documentation still needs to be written, but for now see the `tests.rkt` file to see example
usage. Currently the entire set of Redis V1 commands are implemented.

Example Usage
=============
Create a redis object: `(define redis (new redis%))`

Initialize it: `(send redis init)`

Send a command: `(send redis ping)`
