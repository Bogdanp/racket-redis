#lang racket
(require "main.rkt" rackunit)
(define redis (new redis%))
(send redis set-timeout 0.3) ;this is ok for local testing, probably not for the net though
(send redis init)

(check-equal? (send redis config-resetstat) "OK")
(check-equal? (send redis ping) "PONG" )
(check-equal? (send redis ping "yo watup") "yo watup")
(check-equal? (send redis echo "HEYY") "HEYY")
(check-equal? (send redis select "1") "OK")
; (check-equal? (send redis auth "password") "OK"))

(check-equal? (send redis set "a-number" "1") "OK")
(check-equal? (send redis exists "a-number") 1)
(check-equal? (send redis exists "some crap") 0)
(check-equal? (send redis get "a-number") "1")
(check-equal? (send redis incr "a-number") 2)
(check-equal? (send redis getset "a-number" "4") "2")
(check-equal? (send redis get "a-number") "4")

(check-equal? (send redis set "key1" "fksd") "OK")
(check-equal? (send redis set "key2" "fdadsf") "OK")
(check-equal? (send redis set "key3" "bdafg") "OK")
;(check-true (send redis exists (list "key1" "key2" "key3")))
(check-equal? (send redis mget (list "key1" "key2" "key3"))
              (list "fksd" "fdadsf" "bdafg"))

(check-true (number? (send redis lpush "some-list" "1")))
(check-true (number? (send redis lpush "some-list" (list "1" "2" "3" "4" "5"))))
(check-true (list? (send redis lrange "some-list" "0" "-1")))

(check-equal? (send redis del "a-number") 1)
(check-equal? (send redis set "a" "hey") "OK")
(check-equal? (send redis set "b" "'ello") "OK")
(check-equal? (send redis del (list "a" "b")) 2)

(check-true (list? (member (send redis del "new-key") (list 0 1))))
(check-equal? (send redis setnx "new-key" "Hello") 1)
(check-equal? (send redis setnx "new-key" "World") 0)
(check-equal? (send redis strlen "new-key") 5)
(check-equal? (send redis concat "new-key" " world") 11)
(check-equal? (send redis get "new-key") "Hello world")

(check-equal? (send redis set "a-number" "1") "OK")
(check-equal? (send redis decr "a-number") 0)
(check-equal? (send redis incrby "a-number" "5") 5)
(check-equal? (send redis decrby "a-number" "5") 0)

(check-equal? (send redis hmset "blah" '("a-number" "1")) "OK")
(check-equal? (send redis hsetnx "blah" "a-number" "2") 0)
(check-equal? (send redis hget "blah" "a-number") "1")
(check-equal? (send redis hgetall "blah") (list "a-number" "1"))
(check-equal? (send redis hexists "blah" "a-number") 1)
(check-equal? (send redis hexists "blah" "another-number") 0)
(check-equal? (send redis hincrby "blah" "a-number" "2") 3)
(check-equal? (send redis type "blah") "hash")
(check-equal? (send redis hlen "blah") 1)

(check-equal? (send redis watch "blah") "OK")
(check-equal? (send redis unwatch) "OK")

(check-equal? (send redis bitcount "a-number") 2)
(check-equal? (send redis bitop "AND" "a-number" "a-number") 1)
(check-equal? (send redis bitpos "a-number" "1") 2)
(check-equal? (send redis getrange "a-number" "0" "1") "0")

(check-equal? (send redis zadd "zset" (list "1" "one")) 1)
(check-equal? (send redis zadd "zset" (list "1" "uno")) 1)
(check-equal? (send redis zadd "zset" (list "2" "two" "3" "three")) 2)

(check-equal? (send redis zrem "zset" "one") 1)
(check-equal? (send redis zrem "zset" "uno") 1)
(check-equal? (send redis zrem "zset" (list "two" "three")) 2)

(check-equal? (send redis quit) "OK")