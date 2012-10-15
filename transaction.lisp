(in-package #:wimelib-sqlite3)

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
