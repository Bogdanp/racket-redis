#lang racket/base

(require rackunit
         redis
         "common.rkt")

(define pubsub-tests
  (test-suite
   "pubsub"

   (test-commands "subscribe and unsubscribe to/from channels"
     (call-with-redis-client test-pool
       (lambda (c)
         (define p (make-redis-pubsub c))
         (redis-pubsub-subscribe! p "a" "b")
         (check-false (sync/timeout 0 p))

         (call-with-redis-client test-pool
           (lambda (c)
             (redis-pubsub-publish! c "a" "hello from a")
             (redis-pubsub-publish! c "b" "hello from b")))

         (let ([messages (sort (list (sync p)
                                     (sync p)) bytes<? #:key cadr)])
           (check-equal? messages (list (list #"a" #"hello from a")
                                        (list #"b" #"hello from b"))))

         (redis-pubsub-unsubscribe! p "b")
         (redis-pubsub-unsubscribe! p "c")
         (call-with-redis-client test-pool
           (lambda (c)
             (redis-pubsub-publish! c "a" "hello from a")
             (redis-pubsub-publish! c "b" "hello from b")))

         (let ([messages (list (sync p)
                               (sync/timeout 0.1 p))])
           (check-equal? messages (list (list #"a" #"hello from a") #f)))

         (redis-pubsub-unsubscribe! p "a")
         (redis-pubsub-unsubscribe! p)
         (redis-pubsub-kill! p)

         (check-equal? (redis-ping c) "PONG")
         (check-equal? (redis-ping c) "PONG"))))

   (test-commands "subscribe and unsubscribe to/from patterns"
     (call-with-redis-client test-pool
       (lambda (c)
         (call-with-redis-pubsub c
           (lambda (p)
             (redis-pubsub-subscribe! p "a*a" "b*" #:patterns? #t)
             (check-false (sync/timeout 0 p))

             (call-with-redis-client test-pool
               (lambda (c)
                 (redis-pubsub-publish! c "aba" "a")
                 (redis-pubsub-publish! c "aha" "b")
                 (redis-pubsub-publish! c "blu" "c")
                 (redis-pubsub-publish! c "foo" "d")))

             (let ([messages (list (sync p)
                                   (sync p)
                                   (sync p)
                                   (sync/timeout 0.1 p))])
               (check-equal? messages (list (list #"a*a" #"aba" #"a")
                                            (list #"a*a" #"aha" #"b")
                                            (list #"b*"  #"blu" #"c")
                                            #f)))))

         (check-equal? (redis-ping c) "PONG")
         (check-equal? (redis-ping c) "PONG"))))

   (test-commands "clients can recover from being in pubsub mode"
     (call-with-redis-pubsub test-client void)
     (check-equal? (redis-ping test-client) "PONG")
     (check-equal? (redis-ping test-client) "PONG"))

   (test-commands "clients can recover from being in pubsub mode, after subscribing to a channel"
     (call-with-redis-pubsub test-client
       (lambda (p)
         (redis-pubsub-subscribe! p "a")))
     (check-equal? (redis-ping test-client) "PONG")
     (check-equal? (redis-ping test-client) "PONG"))

   (test-commands "clients can recover from being in pubsub mode, after subscribing to a pattern"
     (call-with-redis-pubsub test-client
       (lambda (p)
         (redis-pubsub-subscribe! p "a*" #:patterns? #t)))
     (check-equal? (redis-ping test-client) "PONG")
     (check-equal? (redis-ping test-client) "PONG"))))

(module+ test
  (require rackunit/text-ui)
  (run-tests pubsub-tests))
