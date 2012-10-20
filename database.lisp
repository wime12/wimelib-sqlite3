(in-package #:wimelib-sqlite3)

(defvar *db*)

(defun open-db (name &key toplevel)
  "Opens the database specified by NAME and returns its handle.
If TOPLEVEL is true then the database will also be set as default."
  (let ((db (sqlite3-open (namestring name))))
    (when toplevel (setf *db* db))
    db))

(defun close-db (db)
  "Closes a database."
  (sqlite3-close db))

(defmacro with-db (db &body body)
  "Sets a database as the default for the body."
  `(let ((*db* ,db))
     ,@body))

(defmacro with-open-db (db-name &body body)
  "Opens a database and sets it as default for the body "
  `(let ((*db* (open-db ,db-name)))
     (unwind-protect
	  (progn ,@body)
       (close-db *db*))))
