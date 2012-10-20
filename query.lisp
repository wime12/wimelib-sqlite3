(in-package #:wimelib-sqlite3)

(defun column-value (stmt index)
  "Retrieves the nth value of the result of an SQL query
and converts it to a lisp value."
  (ecase (sqlite3-column-type stmt index)
    ((#.+sqlite3-integer+)
     (sqlite3-column-integer stmt index))
    ((#.+sqlite3-float+)
     (sqlite3-column-float stmt index))
    ((#.+sqlite3-text+)
     (sqlite3-column-text stmt index))
    ((#.+sqlite3-blob+)
     (sqlite3-column-blob stmt index))
    ((#.+sqlite3-null+)
     nil)))

(defun column-values (stmt)
  "Retrieves all results after a successful SQL query
and converts them to lisp values."
  (loop
     for i from 0 below (sqlite3-data-count stmt)
     collect (column-value stmt i)))

(defun column-names (stmt)
  "Retrieves the column names of the result values after
a successful SQL query."
  (sqlite3-column-names stmt))

(defmacro exec (sexp)
  "Executes an SQL statement. No results are returned."
  `(sqlite3-exec *db* (ssql ,sexp)))

(defun exec* (sexp)
  "Executes an SQL statement. No results are returned.
The SQL sexp is converted to an SQL string every time."
  (sqlite3-exec *db* (ssql* sexp)))

(defmacro do-rows (stmt sexp &body body)
  "Executes an SQL statement. Each result row, which is
represented by the first argument, is processed by the body."
  `(sqlite3-exec
    *db* (ssql ,sexp)
    (lambda (,stmt) ,@body)))

(defmacro do-query ((&rest columns) sexp &body body)
  "Executes an SQL statement and binds the values of
each result row to the identifiers given as the first
argument while the body is executes."
  (let ((stmt (gensym)))
    `(do-rows ,stmt ,sexp
       (multiple-value-bind ,columns
	   (values-list (column-values ,stmt))
	 ,@body))))

(defmacro map-query (sequence-type fun sexp)
  "Returns a sequence of the given type, which contains
the result of applying the function on the result rows of
the SQL query."
  (let ((stmt (gensym)))
    `(coerce
      (collecting
	(do-rows ,stmt ,sexp
	  (collect (funcall ,fun (column-values ,stmt)))))
      ,sequence-type)))

;;; TODO: collect into right sequence?

(defmacro query (sexp &key flat)
  "Executes the query given as SQL form. Either a list of the result
rows or a concatenation of all result rows (a flat list) is returned."
  `(let* ((column-names nil)
	  (result (collecting
		    (do-rows stmt ,sexp
		      (unless column-names
			(setf column-names (column-names stmt)))
		      (collect (column-values stmt))))))
     ,(if flat
	  `(values (apply #'nconc result) column-names)
	  '(values result column-names))))
