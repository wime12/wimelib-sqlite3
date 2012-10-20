(in-package #:wimelib-sqlite3)

(defun list-tables ()
  "List the table names in the database."
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
  "Returns T if the table given as string or symbol
exists in the database, NIL otherwise."
  (not (not (member (sql-identifier table)
		    (list-tables) :test #'equalp))))

(defun list-indices ()
  "Lists the indices in the database."
  (query (:select (:row  name tbl-name sql)
		  :from sqlite-master :where (:= type "index"))))

(defun find-attribute (table column)
  (find (sql-identifier column)
	       (list-attributes table)
	       :key #'attribute-name
	       :test #'equalp))

(defun list-attributes (table)
  "Returns a list of the attributes of the columns in a table."
  (query (:pragma (:table-info @table))))

(defun list-column-names (table)
  "Returns a list of column names"
  (mapcar #'attribute-name (list-attributes table)))

(defun attribute-name (attribute)
  "Extracts the column name from a list of attributes."
  (second attribute))

(defun attribute-type (attribute)
  "Extracts the column type from a list of attributes."
  (third attribute))

(defun attribute-maybe-null-p (attribute)
  "Returns if an attribute may be null."
  (zerop (fourth attribute)))

(defun attribute-default-value (attribute)
  "Returns the default value of an attribute."
  (fifth attribute))
