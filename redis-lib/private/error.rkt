#lang racket/base

(provide
 (struct-out exn:fail:redis)
 (struct-out exn:fail:redis:timeout))

(struct exn:fail:redis exn:fail ())
(struct exn:fail:redis:timeout exn:fail:redis ())
