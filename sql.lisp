(in-package #:wimelib-sqlite3)

;; Classes

(defclass sqlite3-processor (sql-processor) ())

(defclass sqlite3-interpreter (sqlite3-processor sql-interpreter-mixin) ())

(defclass sqlite3-compiler (sqlite3-processor sql-compiler-mixin) ())

(defun get-sql-interpreter ()
  (or *sql-interpreter* (make-instance 'sqlite3-interpreter)))

(defun get-sql-compiler ()
  (or *sql-compiler* (make-instance 'sqlite3-compiler)))

;; Macros

(defmacro define-sql-ops (processor &rest ops)
  `(progn ,@(mapcar #'(lambda (op) `(define-sql-op ,op ,processor)) ops)))

(defmacro in-parentheses (processor &body body)
  `(progn
     (raw-string ,processor "(")
     ,@body
     (raw-string ,processor ")")))

(defmacro define-binary-op (op processor)
  `(define-special-op ,op ((processor ,processor) args)
     (destructuring-bind (left right) args
       (in-parentheses processor
	 (process-sql processor left)
	 (raw-string processor ,(concatenate 'string " " (symbol-name op) " "))
	 (process-sql processor right)))))

(defmacro define-binary-ops (processor &rest ops)
  `(progn ,@(mapcar #'(lambda (op) `(define-binary-op ,op ,processor)) ops)))

(defmacro define-conc-op (op processor)
  `(define-special-op ,op ((processor ,processor) args)
     (in-parentheses processor
       (intersperse processor ,(concatenate 'string " " (symbol-name op) " ")
		    args))))

(defmacro define-conc-ops (processor &rest ops)
  `(progn ,@(mapcar #'(lambda (op) `(define-conc-op ,op ,processor)) ops)))

;; Operators

(define-sql-ops sqlite3-processor
    :alter :analyze :attach :begin :commit :create :delete :detach :drop :end
    :explain :insert :pragma :reindex :release :rollback :savepoint :select
    :update :vacuum)

(define-binary-ops sqlite3-processor
    := :like :<= :>= :< :> :== :!= :<> :is :in :glob)

(define-special-op :is-not ((processor sqlite3-processor) args)
     (destructuring-bind (left right) args
       (in-parentheses processor
	 (process-sql processor left)
	 (raw-string processor " IS NOT ")
	 (process-sql processor right))))

(define-special-op :not-in ((processor sqlite3-processor) args)
     (destructuring-bind (left right) args
       (in-parentheses processor
	 (process-sql processor left)
	 (raw-string processor " NOT IN ")
	 (process-sql processor right))))

(define-conc-ops sqlite3-processor
    :* :/ :and :or :\|\| :% :<< :>> :& :\|)

(define-special-op :- ((processor sqlite3-processor) args)
  (if (cdr args)
      (in-parentheses processor
	(intersperse processor " - " args))
      (progn
	(raw-string processor "-")
	(process-sql processor (car args)))))

(define-special-op :+ ((processor sqlite3-processor) args)
  (if (cdr args)
      (in-parentheses processor
	(intersperse processor " + " args))
      (progn
	(raw-string processor "+")
	(process-sql processor (car args)))))

(define-special-op :~ ((processor sqlite3-processor) args)
  (destructuring-bind (arg) args
    (in-parentheses processor
      (raw-string processor "~")
      (raw-string processor arg))))

#+nil(define-special-op :not ((processor sqlite3-processor) args)
  (destructuring-bind (arg) args
    (in-parentheses processor
      (raw-string processor "NOT ")
      (raw-string processor arg))))

(define-special-op :case ((processor sqlite3-processor) args)
  (in-parentheses processor
    (raw-string processor "CASE ")
    (intersperse processor " " args)))

(define-special-op :between ((processor sqlite3-processor) args)
  (destructuring-bind (var lower upper) args
    (in-parentheses processor
      (intersperse processor " " (list var :between lower :and upper)))))

(define-special-op :not-between ((processor sqlite3-processor) args)
  (destructuring-bind (var lower upper) args
    (in-parentheses processor
      (intersperse processor " " (list var :not :between lower :and upper)))))

(define-special-op :exists ((processor sqlite3-processor) args)
  (in-parentheses processor
    (raw-string processor "EXISTS ")
    (process-sql processor args)))

(define-special-op :not-exists ((processor sqlite3-processor) args)
  (in-parentheses processor
    (raw-string processor "NOT EXISTS ")
    (process-sql processor args)))

;; Syntactic ops

(define-special-op :set ((processor sqlite3-processor) args)
  (raw-string processor "SET ")
  (intersperse processor ", " args
	       :key (lambda (processor pair)
		      (intersperse processor " = " pair))))

(define-special-op :bind ((processor sqlite3-processor) args)
  (destructuring-bind (id) args
    (raw-string processor "$")
    (let ((*sql-identifier-quote* nil))
      (process-sql processor id))))

;; Reader syntax

(defun enable-bind-reader-syntax ()
  "Enables the reader syntax for host parameters in
prepared queries. Host parameters in an SQL expression
can then be denoted by prepending a '$' to the symbol."
  (set-macro-character #\$
    (lambda (stream char)
      (declare (ignorable char))
      (list :bind (read stream)))))

(defun disable-bind-reader-syntax ()
  "Disables the reader syntax for host parameters in
prepared queries."
  (set-macro-character #\$ nil))

;; SQL production

(defmacro ssql (sexp)
  "Compiles an SQL sexp to lisp expressions which return the
corresponding SQL string."
  (multiple-value-bind (code embed-p) (compile-sql (get-sql-compiler) sexp)
    (let ((code `(with-output-to-string (*sql-output*) ,code)))
      (if embed-p
	  `(let ((*sql-interpreter* (make-instance 'sqlite3-interpreter))) ,code)
	  code))))

(defun ssql* (sexp)
  "Translates an SQL sexp to an SQL string."
  (with-output-to-string (*sql-output*)
    (interprete-sql (get-sql-interpreter) sexp)))
