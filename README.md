# rackdis
Redis bindings for Racket.

Example Usage
=============
Let's ping redis:

`
(define redis (new redis%))
`

`
(send redis init)
`

`
(send redis ping)
`
