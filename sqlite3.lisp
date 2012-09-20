(in-package #:wimelib-sqlite3)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (enable-embed-reader-syntax)
  (enable-column-reader-syntax))

(defvar *db*)

(defmacro ssql (sexp)
  `(let ((*sql-interpreter* (make-instance 'sqlite3-interpreter)))
     (with-output-to-string (*sql-output*)
       ,(compile-sql (get-sql-compiler) sexp))))

(defun ssql* (sexp)
  (with-output-to-string (*sql-output*)
    (interprete-sql (get-sql-interpreter) sexp)))

(defun open-db (name)
  (setf *db* (wimelib-sqlite3:sqlite3-open name)))

(defun close-db (&optional (db *db*))
  (sqlite3-close db))

(defun column-value (stmt index)
  (ecase (wimelib-sqlite3:sqlite3-column-type stmt index)
    ((#.wimelib-sqlite3:+sqlite3-integer+)
     (wimelib-sqlite3:sqlite3-column-integer stmt index))
    ((#.wimelib-sqlite3:+sqlite3-float+)
     (wimelib-sqlite3:sqlite3-column-float stmt index))
    ((#.wimelib-sqlite3:+sqlite3-text+)
     (wimelib-sqlite3:sqlite3-column-text stmt index))
    ((#.wimelib-sqlite3:+sqlite3-blob+)
     (wimelib-sqlite3:sqlite3-column-blob stmt index))
    ((#.wimelib-sqlite3:+sqlite3-null+)
     nil)))

(defun column-values (stmt)
  (loop
     for i from 0 below (wimelib-sqlite3:sqlite3-data-count stmt)
     collect (column-value stmt i)))

(defmacro exec (sexp)
  `(wimelib-sqlite3:sqlite3-exec *db* (ssql ,sexp)))

(defmacro do-rows (stmt sexp &body body)
  `(wimelib-sqlite3:sqlite3-exec
    *db* (ssql ,sexp)
    (lambda (,stmt) ,@body)))

(defmacro query (sexp &key flatp)
  (let ((stmt (gensym "STMT")))
    `(let ((result nil))
       (do-rows ,stmt ,sexp
	 (when (not result)
	   (push (wimelib-sqlite3:sqlite3-column-names ,stmt) result))
	 (push (column-values ,stmt) result))
       (let ((res (nreverse result)))
	 (values
	  ,(if flatp
	       `(mapcan #'identity (cdr res))
	       `(cdr res))
	  (car res))))))

(defun begin-transaction ()
  (exec (:begin :transaction)))

(defun end-transaction ()
  (exec (:end :transaction)))

(defun rollback ()
  (exec (:rollback)))

(defmacro with-transaction (&body body)
  `(progn
     (begin-transaction)
     (restart-case (progn ,@body (end-transaction))
       (rollback ()
	 :report "Undo all changes and end the transaction."
	 (rollback))
       (end-transaction ()
	 :report "End the transaction and keep all changes"
	 (end-transaction)))))

(defun list-tables ()
  (mapcar #'car
	  (query (:select name
		       :from sqlite-master :where (:= type "table")))))

(defun list-indices ()
  (query (:select [name tbl-name sql]
		  :from sqlite-master :where (:= type "index"))))

(defun list-attributes (table)
  (query (:pragma (:table-info @table))))

(defun attribute-name (attribute-list)
  (second attribute-list))

(defun attribute-type (attribute-list)
  (third attribute-list))


;;; Test

(defun test* ()
  (ssql* `(:and
	   ,@(loop
		for table in '(thistime nexttime sometime never)
		for count from 42
		collect `(:between (:dot ,table bar) (:* hip hop) ,count)
		collect `(:like (:dot ,table baz) ,(symbol-name table))))))

(defmacro test ()
  (let ((sexp `(:and
		,@(loop
		     for table in '(thistime nexttime sometime never)
		     for count from 42
		     collect `(:between (:dot ,table bar) (:* hip hop) ,count)
		     collect `(:like (:dot ,table baz) ,(symbol-name table))))))
    `(ssql ,sexp)))
