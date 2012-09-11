(in-package #:wimelib-sql)

(define-special-op :l (processor args)
  (raw-string processor "(")
  (intersperse processor ", " args)
  (raw-string processor ")"))

(define-special-op := (processor args)
  (destructuring-bind (left right) args
    (process-sql processor left)
    (raw-string processor " = ")
    (process-sql processor right)))

(define-special-op :like (processor args)
  (destructuring-bind (left right) args
    (process-sql processor left)
    (raw-string processor " like ")
    (process-sql processor right)))

(define-special-op :<= (processor args)
  (destructuring-bind (left right) args
    (process-sql processor left)
    (raw-string processor " <= ")
    (process-sql processor right)))

(define-sql-op :alter)

(define-sql-op :analyze)

(define-sql-op :attach)

(define-sql-op :begin)

(define-sql-op :commit)

(define-sql-op :create)

(define-sql-op :delete)

(define-sql-op :detach)

(define-sql-op :drop)

(define-sql-op :end)

(define-sql-op :explain)

(define-sql-op :insert)

(define-sql-op :pragma)

(define-sql-op :reindex)

(define-sql-op :release)

(define-sql-op :rollback)

(define-sql-op :savepoint)

(define-sql-op :select)

(define-sql-op :update)

(define-sql-op :vacuum)
