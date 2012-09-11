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

(add-sql-op :alter)

(add-sql-op :analyze)

(add-sql-op :attach)

(add-sql-op :begin)

(add-sql-op :commit)

(add-sql-op :create)

(add-sql-op :delete)

(add-sql-op :detach)

(add-sql-op :drop)

(add-sql-op :end)

(add-sql-op :explain)

(add-sql-op :insert)

(add-sql-op :pragma)

(add-sql-op :reindex)

(add-sql-op :release)

(add-sql-op :rollback)

(add-sql-op :savepoint)

(add-sql-op :select)

(add-sql-op :update)

(add-sql-op :vacuum)
