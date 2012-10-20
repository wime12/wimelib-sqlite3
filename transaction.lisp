(in-package #:wimelib-sqlite3)

(defun begin-transaction ()
  "Starts a transaction."
  (exec (:begin :transaction)))

(defun commit-transaction ()
  "Commits a transaction."
  (exec (:commit :transaction)))

(defun savepoint (savepoint)
  "Establishes a savepoint."
  (exec (:savepoint @savepoint)))

(defun release-savepoint (savepoint)
  "Commits all changes up to a savepoint and releases it."
  (exec (:release :savepoint @savepoint)))

(defun rollback (&optional savepoint)
  "Undoes all changes made in the current transaction
or up to the given savepoint."
  (if savepoint
      (exec (:rollback :to @savepoint))
      (exec (:rollback))))

(defmacro with-transaction (&body body)
  "Encloses the body in a transaction which is committed
when the body ends. Two restarts for rolling back or committing
the transaction are established in case any error occurs."
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
  "Creates a savepoint which is released when the body ends.
Two restarts are established for rolling back to or releasing
the savepoint in case any error occurs."
  `(progn
     (savepoint ,savepoint)
     (restart-case (progn ,@body (release-savepoint ,savepoint))
       (rollback ()
	 :report "Undo all changes and end the transaction."
	 (rollback ,savepoint))
       (release-savepoint ()
	 :report "Release the savepoint keeping all changes"
	 (release-savepoint ,savepoint)))))
