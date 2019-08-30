#lang racket/base

(require racket/format
         redis)

(define client (make-redis))
(define stream-name "messages")
(define group "messages-group")
(define consumer "a-consumer")

;; wait for the stream to exist
(define stream-info
  (let loop ()
    (with-handlers ([(lambda (e)
                       (and (exn:fail:redis? e)
                            (string=? "no such key" (exn-message e))))
                     (lambda _
                       (displayln "stream does not exist yet. waiting...")
                       (sleep 1)
                       (loop))])
      (redis-stream-get client stream-name))))

;; create the consumer group if it doesn't exist
(define known-group-names
  (map (compose1 bytes->string/utf-8 redis-stream-group-name)
       (redis-stream-groups client stream-name)))

(unless (member group known-group-names)
  (redis-stream-group-create! client stream-name group "$"))

(define start-time (current-seconds))
(define processed 0)

(define (handle entry)
  (set! processed (add1 processed))
  #;(displayln (hash-ref (redis-stream-entry-fields entry) #"message"))
  )

(void
 (thread
  (lambda _
    (let loop ()
      (sleep 1)
      (define delta (- (current-seconds) start-time))
      (displayln (format "processed: ~a rate: ~a/s" processed (~r (/ processed delta))))
      (loop)))))

;; process the entries
(let loop ([last-id "0"])
  (define entries/by-stream
    (redis-stream-group-read! client stream-name last-id
                              #:group group
                              #:consumer consumer
                              #:limit 1000
                              #:block? #t
                              #:timeout 60000))

  (cond
    [(redis-null? entries/by-stream)
     (loop last-id)]

    [else
     (for* ([pair (in-list entries/by-stream)]
            [entry (in-list (cadr pair))])
       (handle entry)
       (redis-stream-ack! client stream-name group (redis-stream-entry-id entry)))
     (loop ">")]))
