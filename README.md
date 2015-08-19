# rackdis
Redis client for Racket. The documentation still needs to be written, but for now see the `tests.rkt` file to see example
usage. Currently the entire set of Redis v1 commands is implemented, along with various bits of v2 and v3.

Example Usage
=============
Create a redis object: `(define redis (new redis%))`

Initialize it: `(send redis init)`

Send a command: `(send redis ping)`

Installation
============
Execute `raco pkg install git://github.com/eu90h/rackdis` or use DrRacket.

To uninstall, run `raco pkg remove rackdis`
