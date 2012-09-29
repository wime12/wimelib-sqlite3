(in-package #:wimelib-sqlite3)

(declaim (optimize (debug 3)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (enable-embed-reader-syntax)
  (enable-column-reader-syntax))

(defvar *db*)

;; SQL generation

(defmacro ssql (sexp)
  (multiple-value-bind (code embed-p) (compile-sql (get-sql-compiler) sexp)
    (let ((code `(with-output-to-string (*sql-output*) ,code)))
      (if embed-p
	  `(let ((*sql-interpreter* (make-instance 'sqlite3-interpreter))) ,code)
	  code))))

(defun ssql* (sexp)
  (with-output-to-string (*sql-output*)
    (interprete-sql (get-sql-interpreter) sexp)))

;; Open and close a database

(defun open-db (name)
  (setf *db* (wimelib-sqlite3:sqlite3-open name)))

(defun close-db (&optional (db *db*))
  (sqlite3-close db))

;; Queries

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
     for i from 0 below (sqlite3-data-count stmt)
     collect (column-value stmt i)))

(defmacro exec (sexp)
  `(sqlite3-exec *db* (ssql ,sexp)))

(defmacro do-rows (stmt sexp &body body)
  `(sqlite3-exec
    *db* (ssql ,sexp)
    (lambda (,stmt) ,@body)))

(defmacro do-query ((&rest columns) sexp &body body)
  (let ((stmt (gensym)))
    `(do-rows ,stmt ,sexp
       (multiple-value-bind ,columns
	   (values-list (sqlite3-row-values ,stmt))
	 ,@body))))

(defmacro map-query (sequence-type fun sexp)
  (let ((stmt (gensym))
	(result (gensym)))
    `(let ((,result nil))
       (do-rows ,stmt ,sexp
	 (push (funcall ,fun (column-values ,stmt)) ,result))
       (coerce (nreverse ,result) ,sequence-type))))

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

;; Transactions and savepoints

(defun begin-transaction ()
  (exec (:begin :transaction)))

(defun commit-transaction ()
  (exec (:commit :transaction)))

(defun savepoint (savepoint)
  (exec (:savepoint @savepoint)))

(defun release-savepoint (savepoint)
  (exec (:release :savepoint @savepoint)))

(defun rollback (&optional savepoint)
  (if savepoint
      (exec (:rollback :to @savepoint))
      (exec (:rollback))))

(defmacro with-transaction (&body body)
  `(progn
     (begin-transaction)
     (restart-case (progn ,@body (end-transaction))
       (rollback ()
	 :report "Undo all changes and end the transaction."
	 (rollback))
       (commit-transaction ()
	 :report "End the transaction and keep all changes"
	 (end-transaction)))))

(defmacro with-savepoint ((savepoint) &body body)
  `(progn
     (savepoint ,savepoint)
     (restart-case (progn ,@body (release-savepoint ,savepoint))
       (rollback ()
	 :report "Undo all changes and end the transaction."
	 (rollback ,savepoint))
       (release-savepoint ()
	 :report "Release the savepoint keeping all changes"
	 (release-savepoint ,savepoint)))))

;; Database introspection

(defun list-tables ()
  (mapcar #'car
	  (query (:select name
		       :from sqlite-master :where (:= type "table")))))

(defun sql-identifier (id)
  (if (symbolp id)
      (let ((*sql-identifier-quote* nil))
	(with-output-to-string (*sql-output*)
	  (process-sql (get-sql-interpreter) id)))
      id))

(defun sql-symbol (id)
  (if (stringp id)
      (intern (string-upcase (substitute #\- #\_ id)))
      id))

(defun table-exists-p (table)
  (not (not (member (sql-identifier table)
		    (list-tables) :test #'equalp))))

(defun list-indices ()
  (query (:select [name tbl-name sql]
		  :from sqlite-master :where (:= type "index"))))

(defun attribute-list (table)
  (query (:pragma (:table-info @table))))

(defun list-attributes (table)
  (mapcar #'attribute-name (attribute-list table)))

(defun attribute-name (attribute-list)
  (second attribute-list))

(defun attribute-type (attribute table)
  (third (find (sql-identifier attribute)
	       (attribute-list table)
	       :key #'attribute-name
	       :test #'equalp)))

;; Prepared queries

(defvar *prepared-queries* (make-hash-table :weak :value))

(defun call-prepared (stmt fun args)
  (do ((i 1 (1+ i))
       (args args (cdr args)))
      ((null args))
    (bind-parameter stmt i (car args)))
  (do ((result (sqlite3-step stmt) (sqlite3-step stmt)))
      ((= result +sqlite3-done+))
    (when (and fun (= result +sqlite3-row+))
      (funcall fun stmt)))
  (sqlite3-reset stmt)
  (sqlite3-clear-bindings stmt))

(defmacro prepared-aux (name sexp)
  `(let ((stmt-id (gensym)))
     (,@name
      (fun &rest args)
      (let ((stmt (or (gethash stmt-id *prepared-queries*)
		      (setf (gethash stmt-id *prepared-queries*)
			    (sqlite3-prepare *db* (ssql ,sexp))))))
	(call-prepared stmt fun args)))))

(defmacro defprepared (name sexp)
  `(prepared-aux (defun ,name) ,sexp))

(defmacro prepare (sexp)
  `(prepared-aux (lambda) ,sexp))

(defgeneric bind-parameter (stmt i arg)
  (:method (stmt i (arg integer))
    (sqlite3-bind-integer stmt i arg))
  (:method (stmt i (arg string))
    (sqlite3-bind-text stmt i arg))
  (:method (stmt i (arg float))
    (sqlite3-bind-float stmt i arg))
  (:method (stmt i (arg null))
    (sqlite3-bind-null stmt i)))
