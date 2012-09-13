(in-package #:wimelib-sql)

(defmacro in-parentheses (processor &body body)
  `(progn
     (raw-string ,processor "(")
     ,@body
     (raw-string ,processor ")")))

;; Arithmetic operators

(define-special-op :+ (processor args)
  (in-parentheses processor
    (intersperse processor " + " args)))

(define-special-op :- (processor args)
  (in-parentheses processor
    (intersperse processor " - " args)))

(define-special-op :* (processor args)
  (in-parentheses processor
    (intersperse processor " * " args)))

(define-special-op :/ (processor args)
  (in-parentheses processor
    (intersperse processor " / " args)))

(define-special-op := (processor args)
  (destructuring-bind (left right) args
    (raw-string processor "(")
    (process-sql processor left)
    (raw-string processor " = ")
    (process-sql processor right)
    (raw-string processor ")")))

(define-special-op :like (processor args)
  (destructuring-bind (left right) args
    (raw-string processor "(")
    (process-sql processor left)
    (raw-string processor " LIKE ")
    (process-sql processor right)
    (raw-string processor ")")))

(define-special-op :<= (processor args)
  (destructuring-bind (left right) args
    (raw-string processor "(")
    (process-sql processor left)
    (raw-string processor " <= ")
    (process-sql processor right)
    (raw-string processor ")")))

(define-special-op :set (processor args)
  (raw-string processor "SET ")
  (intersperse processor ", " args
	       :key (lambda (processor pair)
		      (intersperse processor " = " pair))))

(define-special-op :between (processor args)
  (destructuring-bind (var lower upper) args
    (raw-string processor "(")
    (intersperse processor " " (list var :between lower :and upper))
    (raw-string processor ")")))

(define-special-op :and (processor args)
  (raw-string processor "(")
  (intersperse processor " AND " args)
  (raw-string processor ")"))

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
