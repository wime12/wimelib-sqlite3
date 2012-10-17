(in-package #:wimelib-sqlite3)

(defmacro in-parentheses (processor &body body)
  `(progn
     (raw-string ,processor "(")
     ,@body
     (raw-string ,processor ")")))

(defmacro binary-op (processor op-string args)
  (let ((left (gensym))
	(right (gensym)))
    `(destructuring-bind (,left ,right) ,args
       (in-parentheses ,processor
	 (process-sql ,processor ,left)
	 (raw-string ,processor ,op-string)
	 (process-sql ,processor ,right)))))

;; Arithmetic operators

(defclass sqlite3-processor (sql-processor) ())

(defclass sqlite3-interpreter (sqlite3-processor sql-interpreter-mixin) ())

(defclass sqlite3-compiler (sqlite3-processor sql-compiler-mixin) ())

(defun get-sql-interpreter ()
  (or *sql-interpreter* (make-instance 'sqlite3-interpreter)))

(defun get-sql-compiler ()
  (or *sql-compiler* (make-instance 'sqlite3-compiler)))

(define-special-op :+ ((processor sqlite3-processor) args)
  (in-parentheses processor
    (intersperse processor " + " args)))

(define-special-op :- ((processor sqlite3-processor) args)
  (in-parentheses processor
    (intersperse processor " - " args)))

(define-special-op :* ((processor sqlite3-processor) args)
  (in-parentheses processor
    (intersperse processor " * " args)))

(define-special-op :/ ((processor sqlite3-processor) args)
  (in-parentheses processor
    (intersperse processor " / " args)))

(define-special-op := ((processor sqlite3-processor) args)
  (binary-op processor " = " args))

(define-special-op :like ((processor sqlite3-processor) args)
  (binary-op processor " LIKE " args))

(define-special-op :<= ((processor sqlite3-processor) args)
  (binary-op processor " <= " args))

(define-special-op :>= ((processor sqlite3-processor) args)
  (binary-op processor " >= " args))

(define-special-op :set ((processor sqlite3-processor) args)
  (raw-string processor "SET ")
  (intersperse processor ", " args
	       :key (lambda (processor pair)
		      (intersperse processor " = " pair))))

(define-special-op :between ((processor sqlite3-processor) args)
  (destructuring-bind (var lower upper) args
    (raw-string processor "(")
    (intersperse processor " " (list var :between lower :and upper))
    (raw-string processor ")")))

(define-special-op :and ((processor sqlite3-processor) args)
  (raw-string processor "(")
  (intersperse processor " AND " args)
  (raw-string processor ")"))

(define-special-op :bind ((processor sqlite3-processor) args)
  (destructuring-bind (id) args
    (raw-string processor "$")
    (let ((*sql-identifier-quote* nil))
      (process-sql processor id))))

(define-special-op :primary-key ((processor sqlite3-processor) args)
  (raw-string processor "PRIMARY KEY (")
  (intersperse processor ", " args)
  (raw-string processor ")"))

(define-sql-op :alter sqlite3-processor)

(define-sql-op :analyze sqlite3-processor)

(define-sql-op :attach sqlite3-processor)

(define-sql-op :begin sqlite3-processor)

(define-sql-op :commit sqlite3-processor)

(define-sql-op :create sqlite3-processor)

(define-sql-op :delete sqlite3-processor)

(define-sql-op :detach sqlite3-processor)

(define-sql-op :drop sqlite3-processor)

(define-sql-op :end sqlite3-processor)

(define-sql-op :explain sqlite3-processor)

(define-sql-op :insert sqlite3-processor)

(define-sql-op :pragma sqlite3-processor)

(define-sql-op :reindex sqlite3-processor)

(define-sql-op :release sqlite3-processor)

(define-sql-op :rollback sqlite3-processor)

(define-sql-op :savepoint sqlite3-processor)

(define-sql-op :select sqlite3-processor)

(define-sql-op :update sqlite3-processor)

(define-sql-op :vacuum sqlite3-processor)

(defun enable-bind-reader-syntax ()
  (set-macro-character #\$
    (lambda (stream char)
      (declare (ignorable char))
      (list :bind (read stream)))))

(defun disable-bind-reader-syntax ()
  (set-macro-character #\$ nil))

(defmacro ssql (sexp)
  (multiple-value-bind (code embed-p) (compile-sql (get-sql-compiler) sexp)
    (let ((code `(with-output-to-string (*sql-output*) ,code)))
      (if embed-p
	  `(let ((*sql-interpreter* (make-instance 'sqlite3-interpreter))) ,code)
	  code))))

(defun ssql* (sexp)
  (with-output-to-string (*sql-output*)
    (interprete-sql (get-sql-interpreter) sexp)))
