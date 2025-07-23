#lang info

(define license 'BSD-3-Clause)
(define collection "redis")
(define deps
  '("base"
    "review"))
(define review-exts
  '((redis/review should-review-syntax? review-syntax)))
