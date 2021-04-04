#lang racket/base

(require racket/contract
         racket/format
         racket/match
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

(struct redis-pool (custodian thd))

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

  (define (make-client client-name)
    (make-redis #:client-name client-name
                #:unix-socket socket-path
                #:host host
                #:port port
                #:timeout timeout
                #:db db
                #:username username
                #:password password))

  (define custodian (make-custodian))
  (parameterize ([current-custodian custodian])
    (define thd
      (thread
       (lambda ()
         (define deadlines (make-hasheq))
         (define (reset-deadline! c)
           (hash-set! deadlines c (+ (current-inexact-milliseconds) idle-ttl)))
         (define (remove-deadline! c)
           (hash-remove! deadlines c))

         (let loop ([total 0]
                    [busy null]
                    [idle null]
                    [waiters null])
           (with-handlers ([exn:fail?
                            (lambda (e)
                              (log-redis-pool-error "unexpected error: ~a" (exn-message e))
                              (loop total busy idle waiters))])
             (sync
              (handle-evt
               (thread-receive-evt)
               (lambda (_)
                 (match (thread-receive)
                   [`(lease ,ch)
                    (cond
                      [(null? idle)
                       (cond
                         [(< total pool-size)
                          (define c (make-client (~a client-name "-" total)))
                          (channel-put ch c)
                          (loop (add1 total) (cons c busy) idle waiters)]

                         [else
                          (loop total busy idle (cons ch waiters))])]

                      [else
                       (define c (car idle))
                       (channel-put ch c)
                       (remove-deadline! c)
                       (loop total (cons c busy) (cdr idle) waiters)])]

                   [`(release ,c)
                    (cond
                      [(memq c busy)
                       (cond
                         [(null? waiters)
                          (reset-deadline! c)
                          (loop total (remq c busy) (cons c idle) waiters)]

                         [else
                          (define ch (car waiters))
                          (channel-put ch c)
                          (loop total busy idle (cdr waiters))])]

                      [else
                       (log-redis-pool-warning "ignoring unknown connection ~e" c)
                       (loop total busy idle waiters)])]

                   [`(shutdown ,ch)
                    (cond
                      [(null? busy)
                       (for-each redis-disconnect! idle)
                       (channel-put ch 'ok)]

                      [else
                       (channel-put ch (exn:fail:redis:pool
                                        (current-continuation-marks)
                                        "shutdown called before all connections were returned to the pool"))
                       (loop total busy idle waiters)])])))

              (if (hash-empty? deadlines)
                  never-evt
                  (handle-evt
                   (alarm-evt (+ (current-inexact-milliseconds) 30000))
                   (lambda (_)
                     (define now (current-inexact-milliseconds))
                     (for ([(c deadline) (in-hash deadlines)])
                       (when (< deadline now)
                         (log-redis-pool-debug "disconnecting idle connection ~e" c)
                         (redis-disconnect! c)
                         (hash-remove! deadlines c)))
                     (loop total busy idle waiters))))))))))

    (redis-pool custodian thd)))

(define-syntax-rule (dispatch p id arg ...)
  (thread-send (redis-pool-thd p) (list 'id arg ...)))

(define-syntax-rule (dispatch/blocking p id arg ...)
  (let ([ch (make-channel)])
    (dispatch p id arg ... ch)
    (define maybe-exn (sync ch))
    (when (exn? maybe-exn)
      (raise maybe-exn))
    maybe-exn))

(define/contract (redis-pool-take! pool [timeout #f])
  (->* (redis-pool?)
       ((or/c #f exact-nonnegative-integer?))
       (or/c #f redis?))
  (define ch (make-channel))
  (define (sink!)
    (begin0 #f
      (thread
       (lambda ()
         (dispatch pool release (channel-get ch))))))
  (dispatch pool lease ch)
  (with-handlers ([exn:break? (λ (_) (sink!))])
    (sync
     ch
     (if timeout
         (handle-evt
          (alarm-evt (+ (current-inexact-milliseconds) timeout))
          (lambda (_)
            (sink!)))
         never-evt))))

(define/contract (redis-pool-release! pool c)
  (-> redis-pool? redis? void?)
  (void (dispatch pool release c)))

(define/contract (redis-pool-shutdown! pool)
  (-> redis-pool? void?)
  (void (dispatch/blocking pool shutdown)))

(define/contract (call-with-redis-client
                   pool proc
                   #:timeout [timeout #f])
  (->* (redis-pool? (-> redis? any))
       (#:timeout (or/c #f exact-nonnegative-integer?))
       any)

  (define c #f)
  (dynamic-wind
    (lambda _
      (set! c (redis-pool-take! pool timeout))
      (unless c
        (raise (exn:fail:redis:pool:timeout "timed out waiting for a client to become available" (current-continuation-marks)))))
    (lambda _
      (unless (redis-connected? c)
        (redis-connect! c))
      (proc c))
    (lambda _
      (redis-pool-release! pool c))))
