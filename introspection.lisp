(in-package #:wimelib-sqlite3)

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
