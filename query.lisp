(in-package #:wimelib-sqlite3)

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

(defun column-names (stmt)
  (sqlite3-column-names stmt))

(defmacro exec (sexp)
  `(sqlite3-exec *db* (ssql ,sexp)))

(defun exec* (sexp)
  (sqlite3-exec *db* (ssql* sexp)))

(defmacro do-rows (stmt sexp &body body)
  `(sqlite3-exec
    *db* (ssql ,sexp)
    (lambda (,stmt) ,@body)))

(defmacro do-query ((&rest columns) sexp &body body)
  (let ((stmt (gensym)))
    `(do-rows ,stmt ,sexp
       (multiple-value-bind ,columns
	   (values-list (column-values ,stmt))
	 ,@body))))

(defmacro map-query (sequence-type fun sexp)
  (let ((stmt (gensym)))
    `(coerce
      (collecting
	(do-rows ,stmt ,sexp
	  (collect (funcall ,fun (column-values ,stmt)))))
      ,sequence-type)))

;;; TODO: collect into right sequence?

(defmacro query (sexp &key flatp)
  `(let* ((column-names nil)
	  (result (collecting
		    (do-rows stmt ,sexp
		      (unless column-names
			(setf column-names (column-names stmt)))
		      (collect (column-values stmt))))))
     ,(if flatp
	  `(values (apply #'nconc result) column-names)
	  '(values result column-names))))
