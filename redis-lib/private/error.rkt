#lang racket/base

(provide
 (struct-out exn:fail:redis)
 (struct-out exn:fail:redis:timeout)
 (struct-out exn:fail:redis:type)
 (struct-out exn:fail:redis:script)
 (struct-out exn:fail:redis:script:missing)
 (struct-out exn:fail:redis:pool)
 (struct-out exn:fail:redis:pool:timeout))

(struct exn:fail:redis exn:fail ())
(struct exn:fail:redis:timeout exn:fail:redis ())
(struct exn:fail:redis:type exn:fail:redis ())
(struct exn:fail:redis:script exn:fail:redis ())
(struct exn:fail:redis:script:missing exn:fail:redis ())
(struct exn:fail:redis:pool exn:fail:redis ())
(struct exn:fail:redis:pool:timeout exn:fail:redis:pool ())
