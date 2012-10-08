(in-package #:wimelib-sqlite3)

;; TODO: Finalize prepared queries

(defvar *prepared-queries* (make-hash-table #+nil :weak #+nil :value))

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

(defun print-row (stmt)
  (print (column-values stmt)))

(defun prepared-results (prepared-stmt &rest args)
  (let ((result nil))
    (apply prepared-stmt (lambda (stmt)
			   (push (column-values stmt) result))
	   args)
    (nreverse result)))
