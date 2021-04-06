#lang racket/base

(require data/pool
         racket/contract
         racket/string
         "client.rkt"
         "error.rkt")

(provide
 make-redis-pool
 redis-pool?
 redis-pool-take!
 redis-pool-release!
 redis-pool-shutdown!
 call-with-redis-client)

(define-logger redis-pool)

(define (redis-pool? v)
  (pool? v))

(define/contract (make-redis-pool #:client-name [client-name "racket-redis"]
                                  #:unix-socket [socket-path #f]
                                  #:host [host "127.0.0.1"]
                                  #:port [port 6379]
                                  #:timeout [timeout 5000]
                                  #:db [db 0]
                                  #:username [username #f]
                                  #:password [password #f]
                                  #:pool-size [pool-size 4]
                                  #:idle-ttl [idle-ttl 3600000])
  (->* ()
       (#:client-name non-empty-string?
        #:unix-socket (or/c #f path-string?)
        #:host non-empty-string?
        #:port (integer-in 0 65536)
        #:timeout exact-nonnegative-integer?
        #:db (integer-in 0 16)
        #:username (or/c #f non-empty-string?)
        #:password (or/c #f non-empty-string?)
        #:pool-size exact-positive-integer?
        #:idle-ttl exact-positive-integer?)
       redis-pool?)

  (make-pool
   #:max-size pool-size
   #:idle-ttl idle-ttl
   (lambda ()
     (make-redis #:client-name client-name
                 #:unix-socket socket-path
                 #:host host
                 #:port port
                 #:timeout timeout
                 #:db db
                 #:username username
                 #:password password))
   redis-disconnect!))

(define/contract (redis-pool-take! p [timeout #f])
  (->* (redis-pool?)
       ((or/c #f exact-nonnegative-integer?))
       (or/c #f redis?))
  (pool-take! p timeout))

(define/contract (redis-pool-release! p c)
  (-> redis-pool? redis? void?)
  (pool-release! p c))

(define/contract (redis-pool-shutdown! p)
  (-> redis-pool? void?)
  (pool-close! p))

(define/contract (call-with-redis-client
                   p proc
                   #:timeout [timeout #f])
  (->* (redis-pool? (-> redis? any))
       (#:timeout (or/c #f exact-nonnegative-integer?))
       any)
  (with-handlers ([exn:fail:pool?
                   (lambda (e)
                     (raise (exn:fail:redis:pool:timeout
                             (exn-message e)
                             (exn-continuation-marks e))))])
    (call-with-pool-resource p
      #:timeout timeout
      (lambda (c)
        (unless (redis-connected? c)
          (redis-connect! c))
        (proc c)))))
