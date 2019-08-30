#lang racket/base

(require racket/async-channel
         racket/contract
         racket/format
         racket/string
         "client.rkt"
         "error.rkt")

(provide
 make-redis-pool
 redis-pool?
 redis-pool-take!
 redis-pool-release!
 call-with-redis-client)

(struct redis-pool (clients))

(define/contract (make-redis-pool #:client-name [client-name "racket-redis"]
                                  #:host [host "127.0.0.1"]
                                  #:port [port 6379]
                                  #:timeout [timeout 5]
                                  #:db [db 0]
                                  #:password [password #f]
                                  #:pool-size [pool-size 4]
                                  #:idle-ttl [idle-ttl 3600])
  (->* ()
       (#:client-name non-empty-string?
        #:host non-empty-string?
        #:port (integer-in 0 65536)
        #:timeout exact-nonnegative-integer?
        #:db (integer-in 0 16)
        #:password (or/c false/c non-empty-string?)
        #:pool-size exact-positive-integer?
        #:idle-ttl exact-positive-integer?)
       redis-pool?)

  (define (make-client client-name)
    (make-redis #:client-name client-name
                #:host host
                #:port port
                #:timeout timeout
                #:db db
                #:password password))

  (define clients
    (make-async-channel pool-size))

  (for ([id pool-size])
    (async-channel-put clients
                       (let ([client #f]
                             [deadline #f])
                         (lambda ()
                           (define the-client
                             (cond
                               [(and deadline (or (not (redis-connected? client))
                                                  (> (current-inexact-milliseconds) deadline)))
                                (begin0 client
                                  (redis-disconnect! client)
                                  (redis-connect! client))]

                               [client
                                client]

                               [else
                                (define client* (make-client (~a client-name "-" id)))
                                (begin0 client*
                                  (set! client client*))]))

                           (begin0 the-client
                             (set! deadline (+ (current-inexact-milliseconds) idle-ttl)))))))

  (redis-pool clients))

(define/contract (redis-pool-take! pool [timeout #f])
  (->* (redis-pool?)
       ((or/c false/c (and/c real? (not/c negative?))))
       (or/c false/c (-> redis?)))
  (sync/timeout timeout (redis-pool-clients pool)))

(define/contract (redis-pool-release! pool client-fn)
  (-> redis-pool? (-> redis?) void?)
  (async-channel-put (redis-pool-clients pool) client-fn))

(define/contract (call-with-redis-client
                   pool proc
                   #:timeout [timeout #f])
  (->* (redis-pool? (-> redis? any))
       (#:timeout (or/c false/c (and/c real? (not/c negative?))))
       any)

  (define client-fn #f)
  (dynamic-wind
    (lambda _
      (define the-client-fn (redis-pool-take! pool timeout))
      (unless the-client-fn
        (raise (exn:fail:redis:pool:timeout "timed out waiting for a client to become available" (current-continuation-marks))))

      (set! client-fn the-client-fn))
    (lambda _
      (proc (client-fn)))
    (lambda _
      (redis-pool-release! pool client-fn))))
