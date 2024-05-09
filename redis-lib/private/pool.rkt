#lang racket/base

(require data/pool
         racket/contract/base
         racket/string
         "client.rkt"
         "error.rkt")

(provide
 (contract-out
  [make-redis-pool
   (->* []
        [#:client-name non-empty-string?
         #:unix-socket (or/c #f path-string?)
         #:host non-empty-string?
         #:port (integer-in 0 65536)
         #:timeout exact-nonnegative-integer?
         #:db (integer-in 0 16)
         #:username (or/c #f non-empty-string?)
         #:password (or/c #f non-empty-string?)
         #:pool-size exact-positive-integer?
         #:idle-ttl exact-positive-integer?]
        redis-pool?)]
  [redis-pool? (-> any/c boolean?)]
  [redis-pool-take!
   (->* [redis-pool?]
        [(or/c #f exact-nonnegative-integer?)]
        (or/c #f redis?))]
  [redis-pool-release!
   (-> redis-pool? redis? void?)]
  [redis-pool-shutdown!
   (-> redis-pool? void?)]
  [call-with-redis-client
    (->* [redis-pool? (-> redis? any)]
         [#:timeout (or/c #f exact-nonnegative-integer?)]
         any)]))

(define-logger redis-pool)

(struct redis-pool (cust impl))

(define (make-redis-pool #:client-name [client-name "racket-redis"]
                         #:unix-socket [socket-path #f]
                         #:host [host "127.0.0.1"]
                         #:port [port 6379]
                         #:timeout [timeout 5000]
                         #:db [db 0]
                         #:username [username #f]
                         #:password [password #f]
                         #:pool-size [pool-size 4]
                         #:idle-ttl [idle-ttl 3600000])
  (define cust (make-custodian))
  (define impl
    (parameterize ([current-custodian cust])
      (make-pool
       #:max-size pool-size
       #:idle-ttl idle-ttl
       (lambda ()
         (make-redis
          #:client-name client-name
          #:unix-socket socket-path
          #:host host
          #:port port
          #:timeout timeout
          #:db db
          #:username username
          #:password password))
       redis-disconnect!)))
  (redis-pool cust impl))

(define (redis-pool-take! p [timeout #f])
  (pool-take! (redis-pool-impl p) timeout))

(define (redis-pool-release! p c)
  (pool-release! (redis-pool-impl p) c))

(define (redis-pool-shutdown! p)
  (pool-close! (redis-pool-impl p))
  (custodian-shutdown-all (redis-pool-cust p)))

(define (call-with-redis-client
          p proc
          #:timeout [timeout #f])
  (with-handlers ([exn:fail:pool?
                   (lambda (e)
                     (raise (exn:fail:redis:pool:timeout
                             (exn-message e)
                             (exn-continuation-marks e))))])
    (call-with-pool-resource (redis-pool-impl p)
      #:timeout timeout
      (lambda (c)
        (unless (redis-connected? c)
          (parameterize ([current-custodian (redis-pool-cust p)])
            (log-redis-pool-debug "reconnecting")
            (redis-connect! c)))
        (proc c)))))
