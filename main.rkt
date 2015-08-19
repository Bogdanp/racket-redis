#lang racket
(provide redis%)

(require "resp.rkt" racket/tcp)

(define redis%
  (class
      object%
    (init-field [ip "127.0.0.1"] [port 6379] [timeout 1])
    (field [out null] [in null])
    (super-new)
    
    (define/private (send msg)
      (display msg out)
      (flush-output out))
    
    (define/private (get-response)
      (let loop ([resp ""])
        (let ([p (sync/timeout timeout in)])
          (if (input-port? p)
              (let ([s (read-line p)])
                (if (eof-object? s)
                    (if (not (equal? resp ""))
                        (redis-decode resp)
                        "ERR timed out")
                    (loop (string-append resp s "\n"))))
              (if (not (equal? resp ""))
                  (redis-decode resp)
                  "ERR timed out")))))
    
    (define/private (apply-cmd cmd [args null])
      (if (null? args)
          (send (string-append cmd "\r\n"))
          (send (redis-encode-array (append (list cmd) (if (list? args) args (list args)))))))

    (define/public (set-timeout t) (set! timeout t))
    
    (define/public (ping [msg null])
      (apply-cmd "PING" msg)
      (get-response))

    (define/public (auth password)
      (apply-cmd "AUTH" password)
      (get-response))
    
    (define/public (echo msg)
      (apply-cmd "ECHO" msg)
      (get-response))

    (define/public (select index)
      (apply-cmd "SELECT" index)
      (get-response))
    
    (define/public (quit)
      (apply-cmd "QUIT")
      (get-response))

    (define/public (exists keys)
      (apply-cmd "EXISTS" keys)
      (get-response))
    
    (define/public (set key value)
      (apply-cmd "SET" (list key value))
      (get-response))
    
    (define/public (get key)
      (apply-cmd "GET" key)
      (get-response))

    (define/public (mget keys)
      (apply-cmd "MGET" keys)
      (get-response))

    (define/public (mset data)
      (apply-cmd "MSET" data)
      (get-response))

    (define/public (msetnx data)
      (apply-cmd "MSETNX" data)
      (get-response))
    
    (define/public (getset key value)
      (apply-cmd "GETSET" (list key value))
      (get-response))
    
    (define/public (incr key)
      (apply-cmd "INCR" key)
      (get-response))

    (define/public (incrby key value)
      (apply-cmd "INCRBY" (list key value))
      (get-response))
    
    (define/public (decr key)
      (apply-cmd "DECR" key)
      (get-response))

    (define/public (decrby key value)
      (apply-cmd "DECRBY" (list key value))
      (get-response))
    
    (define/public (del key)
      (apply-cmd "DEL" key)
      (get-response))

    (define/public (setnx key value)
      (apply-cmd "SETNX" (list key value))
      (get-response))
    
    (define/public (lpush key value)
      (apply-cmd "LPUSH" (if (list? value)
                             (append (list key) value)
                             (list key value)))
      (get-response))

    (define/public (rpush key value)
      (apply-cmd "RPUSH" (if (list? value)
                             (append (list key) value)
                             (list key value)))
      (get-response))
    
    (define/public (lrange key min max)
      (apply-cmd "LRANGE" (list key min max))
      (get-response))

    (define/public (ltrim key start end)
      (apply-cmd "LTRIM" (list key start end))
      (get-response))

    (define/public (lindex key index)
      (apply-cmd "LINDEX" (list key index))
      (get-response))

    (define/public (lset key index value)
      (apply-cmd "LSET" (list key index value))
      (get-response))

    (define/public (lpop key string)
      (apply-cmd "LPOP" (list key string))
      (get-response))

    (define/public (rpop key string)
      (apply-cmd "RPOP" (list key string))
      (get-response))

    (define/public (blpop keys timeout)
      (apply-cmd "BLPOP" (append keys (list timeout)))
      (get-response))
    
    (define/public (brpop keys timeout)
      (apply-cmd "BRPOP" (append keys (list timeout)))
      (get-response))

    (define/public (rpoplpush srckey destkey)
      (apply-cmd "RPOPLPUSH" (list srckey destkey))
      (get-response))

    (define/public (sadd key member)
      (apply-cmd "SADD" (list key member))
      (get-response))

    (define/public (srem key member)
      (apply-cmd "SREM" (list key member))
      (get-response))
    
    (define/public (spop key)
      (apply-cmd "SPOP" key)
      (get-response))
    
    (define/public (srandmember key)
      (apply-cmd "SRANDMEMBER" key)
      (get-response))

    (define/public (smove srckey destkey member)
      (apply-cmd "SREM" (list srckey destkey member))
      (get-response))

    (define/public (scard key)
      (apply-cmd "SCARD" key)
      (get-response))

    (define/public (sismember key member)
      (apply-cmd "SISMEMBER" (list key member))
      (get-response))

    (define/public (sinter keys)
      (apply-cmd "SINTER" keys)
      (get-response))

    (define/public (sinterstore destkey srckeys)
      (apply-cmd "SINTERSTORE" (list destkey srckeys))
      (get-response))
    
    (define/public (sunion keys)
      (apply-cmd "SUNION" keys)
      (get-response))

    (define/public (sunionstore destkey srckeys)
      (apply-cmd "SUNIONSTORE" (list destkey srckeys))
      (get-response))

    (define/public (sdiff keys)
      (apply-cmd "SDIFF" keys)
      (get-response))

    (define/public (sdiffstore destkey srckeys)
      (apply-cmd "SDIFFSTORE" (list destkey srckeys))
      (get-response))

    (define/public (smembers key)
      (apply-cmd "SMEMBERS" key)
      (get-response))

    (define/public (zadd key score member)
      (apply-cmd "ZADD" (list key score member))
      (get-response))

    (define/public (zrem key member)
      (apply-cmd "ZREM" (list key member))
      (get-response))

    (define/public (zincrby key incr member)
      (apply-cmd "ZINCRBY" (list key incr member))
      (get-response))

    (define/public (zrange key start end)
      (apply-cmd "ZRANGE" (list key start end))
      (get-response))

    (define/public (zrevrange key start end)
      (apply-cmd "ZREVRANGE" (list key start end))
      (get-response))

    (define/public (zrangebyscore key min max)
      (apply-cmd "ZRANGEBYSCORE" (list key min max))
      (get-response))
    
    (define/public (zremrangebyscore key min max)
      (apply-cmd "ZREMRANGEBYSCORE" (list key min max))
      (get-response))

    (define/public (zcard key)
      (apply-cmd "ZCARD" key)
      (get-response))

    (define/public (zscore key member)
      (apply-cmd "ZSCORE" (list key member))
      (get-response))

    (define/public (hmset key data)
      (apply-cmd "HMSET" (append (list key) data))
      (get-response))

    (define/public (hvals key)
      (apply-cmd "HVALS" key)
      (get-response))

    (define/public (hdel key fields)
      (apply-cmd "HDEL" (append (list key) fields))
      (get-response))
    
    (define/public (hsetnx key field value)
      (apply-cmd "HSETNX" (list key field value))
      (get-response))
    
    (define/public (hget key field)
      (apply-cmd "HGET" (list key field))
      (get-response))

    (define/public (hgetall key)
      (apply-cmd "HGETALL" key)
      (get-response))

    (define/public (hincrby key field increment)
      (apply-cmd "HINCRBY" (list key field increment))
      (get-response))

    (define/public (hexists key field)
      (apply-cmd "HEXISTS" (list key field))
      (get-response))

    (define/public (hkeys key)
      (apply-cmd "HKEYS" key)
      (get-response))

    (define/public (hlen key)
      (apply-cmd "HLEN" key)
      (get-response))

    (define/public (type key)
      (apply-cmd "TYPE" key)
      (get-response))

    (define/public (keys pattern)
      (apply-cmd "KEYS" pattern)
      (get-response))

    (define/public (randomkey)
      (apply-cmd "RANDOMKEY")
      (get-response))
    
    (define/public (rename oldkey newkey)
      (apply-cmd "RENAME" (list oldkey newkey))
      (get-response))

    (define/public (renamex oldkey newkey)
      (apply-cmd "RENAMEX" (list oldkey newkey))
      (get-response))

    (define/public (config-get parameter)
      (apply-cmd "CONFIG GET" parameter)
      (get-response))

    (define/public (config-set parameter value)
      (apply-cmd "CONFIG SET" (list parameter value))
      (get-response))

    (define/public (config-rewrite)
      (apply-cmd "CONFIG REWRITE")
      (get-response))

    (define/public (config-resetstat)
      (apply-cmd "CONFIG RESETSTAT")
      (get-response))

    (define/public (dbsize)
      (apply-cmd "DBSIZE")
      (get-response))

    (define/public (expire key seconds)
      (apply-cmd "EXPIRE" (list key seconds))
      (get-response))

    (define/public (expireat key unixtime)
      (apply-cmd "EXPIREAT" (list key unixtime))
      (get-response))

    (define/public (ttl key)
      (apply-cmd "TTL" key)
      (get-response))

    (define/public (move key index)
      (apply-cmd "MOVE" (list key index))
      (get-response))

    (define/public (flushdb)
      (apply-cmd "FLUSHDB")
      (get-response))
    
    (define/public (flushall)
      (apply-cmd "FLUSHALL")
      (get-response))

    (define/public (save)
      (apply-cmd "SAVE")
      (get-response))

    (define/public (bgsave)
      (apply-cmd "BGSAVE")
      (get-response))

    (define/public (lastsave)
      (apply-cmd "LASTSAVE")
      (get-response))

    (define/public (bgrewriteaof)
      (apply-cmd "BGREWRITEAOF")
      (get-response))

    (define/public (shutdown)
      (apply-cmd "SHUTDOWN")
      (get-response))

    (define/public (info)
      (apply-cmd "INFO")
      (get-response))

    (define/public (monitor)
      (apply-cmd "MONITOR")
      (get-response))

    (define/public (object subcommand [args null])
      (apply-cmd "OBJECT" (append (list subcommand) args))
      (get-response))
    
    (define/public (slaveof host port)
      (apply-cmd "SLAVEOF" (list host port))
      (get-response))
    
    (define/public (init)
      (define-values (i o) (tcp-connect ip port))
      (set! in i)
      (set! out o))))