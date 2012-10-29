;;;; wimelib-sqlite3.lisp

(in-package #:wimelib-sqlite3-ffi)

;;; Foreign Types

(defctype string-utf8 (:string :encoding :utf-8))

(defcstruct sqlite3)

(define-foreign-type sqlite3-result-code ()
  ()
  (:actual-type :int)
  (:simple-parser sqlite3-result-code))

(defmethod expand-from-foreign (value (type sqlite3-result-code))
  `(let ((value ,value))
     (check-sqlite3-error value) value))

(defmethod expand-to-foreign (value (type sqlite3-result-code))
  value)

(defcstruct sqlite3-stmt)

(defctype sqlite3-column-type :int)

;;; Result codes and error handling

(macrolet ((lines (&rest lines)
	     `(progn
		,@(mapcar (lambda (line)
			    (destructuring-bind (name code description) line
			      (declare (ignorable description))
			      `(defconstant ,name ,code)))
			  lines)
		(defun sqlite3-result-code-description (code)
		  (case code
		    ,@(mapcar (lambda (line)
				(destructuring-bind (name code description)
				    line
				  (declare (ignorable name))
				  `((,code) ,description)))
			      lines))))))
  (lines
   (+sqlite3-ok+ 0 "Successful result")
   (+sqlite3-error+ 1 "SQL error or missing database")
   (+sqlite3-internal+ 2 "Internal logic error in SQLite")
   (+sqlite3-perm+ 3 "Access permission denied")
   (+sqlite3-abort+ 4 "Callback routine requested an abort")
   (+sqlite3-busy+ 5 "The database file is locked")
   (+sqlite3-locked+ 6 "A table in the database is locked")
   (+sqlite3-nomem+ 7 "A malloc() failed")
   (+sqlite3-readonly+ 8 "Attempt to write a readonly database")
   (+sqlite3-interrupt+ 9 "Operation terminated by sqlite3_interrupt()")
   (+sqlite3-ioerr+ 10 "Some kind of disk I/O error occurred")
   (+sqlite3-corrupt+ 11 "The database disk image is malformed")
   (+sqlite3-notfound+ 12 "Table or record not found")
   (+sqlite3-full+ 13 "Insertion failed because database is full")
   (+sqlite3-cantopen+ 14 "Unable to open the database file")
   (+sqlite3-protocol+ 15 "Database lock protocol error")
   (+sqlite3-empty+ 16 "Database is empty")
   (+sqlite3-schema+ 17 "The database schema changed")
   (+sqlite3-toobig+ 18 "Too much data for one row")
   (+sqlite3-constraint+ 19 "Abort due to constreint violation")
   (+sqlite3-mismatch+ 20 "Data type mismatch")
   (+sqlite3-misuse+ 21 "Library used incorrectly")
   (+sqlite3-nolfs+ 22 "Uses OS features not supported on host")
   (+sqlite3-auth+ 23 "Authorization denied")
   (+sqlite3-format+ 24 "Auxiliary database format error")
   (+sqlite3-range+ 25 "2nd parameter to sqlite3_bind out of range")
   (+sqlite3-notadb+ 26 "File opened that is not a database file")
   (+sqlite3-row+ 100 "sqlite3_step() has another row ready")
   (+sqlite3-done+ 101 "sqlite3_step() has finished executing")))

(defcfun (sqlite3-errcode "sqlite3_errcode")
    :int
  (db (:pointer sqlite3)))

(defcfun (sqlite3-errmsg "sqlite3_errmsg")
    string-utf8
  (db (:pointer sqlite3)))

(define-condition sqlite3-error-condition (error)
  ((error-code :initarg :error-code :reader sqlite3-error-code)
   (db :initarg :db :reader sqlite3-error-db))
  (:report (lambda (condition stream)
	     (with-slots (error-code db) condition
	       (format stream "SQLite3 error ~A: ~A"
		       (sqlite3-error-code condition)
		       (sqlite3-error-message condition))))))

(defgeneric sqlite3-error-message (condition)
  (:method ((condition sqlite3-error-condition))
    (with-slots (error-code db) condition
      (if db
	  (sqlite3-errmsg db)
	  (sqlite3-result-code-description error-code)))))

(defun signal-sqlite3-error (code db)
  (error 'sqlite3-error-condition :error-code code :db db))

(defun sqlite3-error-p (code)
  (declare (fixnum code))
  (< +sqlite3-ok+ code +sqlite3-row+))

(defun check-sqlite3-error (code &optional db)
  (when (sqlite3-error-p code)
    (signal-sqlite3-error code db)))

(defcfun (%sqlite3-open "sqlite3_open")
    :int
  (name string-utf8)
  (db (:pointer sqlite3)))

(defcfun sqlite3-close
    sqlite3-result-code
  (db (:pointer sqlite3)))

(defun sqlite3-open (filename)
  (with-foreign-object (db-place '(:pointer (:pointer sqlite3)))
    (let* ((code (%sqlite3-open filename db-place))
	   (db (mem-ref db-place '(:pointer sqlite3))))
      (if (sqlite3-error-p code)
	  (unwind-protect
	       (signal-sqlite3-error code db)
	    (sqlite3-close db))
	  db))))

(defcfun (%sqlite3-prepare "sqlite3_prepare_v2")
    sqlite3-result-code
  (db (:pointer sqlite3))
  (sql string-utf8)
  (length :int)
  (stmt (:pointer (:pointer sqlite3-stmt)))
  (tail (:pointer string-utf8)))

(defun sqlite3-prepare (db sql &optional (length -1))
  (with-foreign-objects ((stmt '(:pointer sqlite3-stmt))
			 (tail '(:pointer string-utf8)))
    (%sqlite3-prepare db sql length stmt tail) db
    (values
     (mem-ref (mem-ref stmt :pointer) 'sqlite3-stmt) 
     (mem-ref tail 'string-utf8))))

(defcfun sqlite3-step
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt)))

(defcfun sqlite3-column-blob
    :pointer
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun sqlite3-column-bytes
    :int
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun (sqlite3-column-float "sqlite3_column_double")
    :double
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun (sqlite3-column-integer "sqlite3_column_int64")
    :int64
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun sqlite3-column-text
    string-utf8
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun sqlite3-column-name
    string-utf8
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defconstant +sqlite3-integer+ 1)
(defconstant +sqlite3-float+ 2)
(defconstant +sqlite3-text+ 3)
(defconstant +sqlite3-blob+ 4)
(defconstant +sqlite3-null+ 5)

(defcfun sqlite3-column-type
    sqlite3-column-type
  (stmt (:pointer sqlite3-stmt))
  (column-index :int))

(defcfun sqlite3-data-count
    :int
  (stmt (:pointer sqlite3-stmt)))

(defmacro define-bind-destructor (name (obj) &body body)
  `(defcallback ,name
       :void
       ((,obj :pointer))
     ,@body))

(defcfun (%sqlite3-bind-blob "sqlite3_bind_blob")
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int)
  (blob :pointer)
  (bytes :int)
  (destructor :pointer))

(defvar sqlite3-bind-destructor-static (make-pointer 0))
(defvar sqlite3-bind-destructor-transient
  (inc-pointer sqlite3-bind-destructor-static -1))

(defun sqlite3-bind-blob (stmt parameter-index blob bytes
			  &optional (destructor sqlite3-bind-destructor-static))
  (%sqlite3-bind-blob stmt parameter-index blob bytes destructor))

(defcfun (sqlite3-bind-float "sqlite3_bind_double")
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int)
  (value :double))

(defcfun (sqlite3-bind-integer "sqlite3_bind_int64")
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int)
  (value :int64))

(defcfun sqlite3-bind-null
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int))

(defcfun (%sqlite3-bind-text "sqlite3_bind_text")
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int)
  (text string-utf8)
  (bytes :int)
  (destructor :pointer))

(defun sqlite3-bind-text (stmt parameter-index text
			  &key (bytes -1) (destructor
					   sqlite3-bind-destructor-static))
  (%sqlite3-bind-text stmt parameter-index text bytes destructor))

(defcfun sqlite3-bind-parameter-index
    :int
  (stmt (:pointer sqlite3-stmt))
  (parameter-name string-utf8))

(defcfun sqlite3-bind-parameter-count
    :int
  (stmt (:pointer sqlite3-stmt)))

(defcfun sqlite3-bind-parameter-name
    string-utf8
  (stmt (:pointer sqlite3-stmt))
  (parameter-index :int))

(defcfun sqlite3-clear-bindings
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt)))

(defcfun sqlite3-reset
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt)))

(defcfun sqlite3-finalize
    sqlite3-result-code
  (stmt (:pointer sqlite3-stmt)))

(defun sqlite3-do-rows (stmt &optional fun)
  (do ((result (sqlite3-step stmt) (sqlite3-step stmt)))
      ((/= result +sqlite3-row+) nil)
    (when fun (funcall fun stmt))))

(defun sqlite3-exec (db sql &optional fun)
  (let ((stmt (sqlite3-prepare db sql)))
    (unwind-protect
	 (progn
	   (sqlite3-do-rows stmt fun)
	   (check-sqlite3-error (sqlite3-errcode db) db))
      (sqlite3-finalize stmt))))

(defun sqlite3-row-values (stmt)
  (loop
     for i from 0 below (sqlite3-data-count stmt)
     collecting (sqlite3-column-text stmt i)))

(defun sqlite3-column-names (stmt)
  (loop
     for i from 0 below (sqlite3-data-count stmt)
     collecting (sqlite3-column-name stmt i)))

(defun sqlite3-dump-table (db table &optional (stream *standard-output*))
  (let ((stmt (sqlite3-prepare db (format nil "select * from ~A;" table))))
    (unwind-protect
	 (progn
	   (sqlite3-step stmt)
	   (format stream "~%~{~a~^ | ~}" (sqlite3-column-names stmt))
	   (sqlite3-reset stmt)
	   (sqlite3-do-rows stmt
			    (lambda (stmt)
			      (format stream "~&~{~S~^ | ~}"
				      (sqlite3-row-values stmt)))))
      (sqlite3-finalize stmt))))

(defcfun sqlite3-extended-errcode
    :int
  (db :pointer))
