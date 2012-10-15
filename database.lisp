(in-package #:wimelib-sqlite3)

(defvar *db*)

(defun open-db (name &key toplevel)
  (let ((db (sqlite3-open (namestring name))))
    (when toplevel (setf *db* db))
    db))

(defun close-db (db)
  (sqlite3-close db))

(defmacro with-db (db &body body)
  `(let ((*db* ,db))
     ,@body))

(defmacro with-open-db (db-name &body body)
  `(let ((*db* (open-db ,db-name)))
     (unwind-protect
	  (progn ,@body)
       (close-db *db*))))
