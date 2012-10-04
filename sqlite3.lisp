(in-package #:wimelib-sqlite3)

(declaim (optimize (debug 3)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (enable-embed-reader-syntax)
  (enable-column-reader-syntax)
  (enable-bind-reader-syntax))

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
  (setf *db* (wimelib-sqlite3:sqlite3-open (namestring name))))

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

(defun print-row (stmt)
  (print (column-values stmt)))

(defun prepared-result (prepared-stmt &rest args)
  (let ((result nil))
    (apply prepared-stmt (lambda (stmt)
			   (push (column-values stmt) result))
	   args)
    (nreverse result)))

;; DAO

#+nil(defclass species-list ()
  ((local-name :coltype :text :initarg :local-name :accessor local-name)
   (scientific-name :coltype :text :initarg :scientific-name :accessor scientific-name)
   (species-id :coltype :integer :primary-key t))
  (:metaclass dao-class))


(defclass da-class (standard-class)
  (#+nil(columns :reader da-class-columns :initarg :columns)
	#+nil(primary-keys :reader da-class-primary-keys :initarg :primary-keys)
	#+nil(not-null-columns :reader da-class-not-null-columns :initarg :not-null-columns))
  (:documentation "Metaclass for database access objects."))

(defmethod validate-superclass ((class da-class) (super-class standard-class))
  t)

(defclass da-standard-direct-slot-definition (standard-direct-slot-definition)
  ((column-type :initform nil :initarg :column-type :reader da-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-not-null)))

(defclass da-standard-effective-slot-definition (standard-effective-slot-definition)
  ((column-type :initform nil :initarg :column-type :reader da-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-not-null)))

(defmethod da-column-type (slot-definition)
  (declare (ignorable slot-definition))
  nil)

(defmethod da-primary-key (slot-definition)
  (declare (ignorable slot-definition))
  nil)

(defmethod da-not-null (slot-definition)
  (declare (ignorable slot-definition))
  nil)

(defmethod direct-slot-definition-class ((class da-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'da-standard-direct-slot-definition))

(defmethod effective-slot-definition-class ((class da-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'da-standard-effective-slot-definition))

(defmethod compute-effective-slot-definition :around
    ((class da-class) slot-name direct-slot-definitions)
  (declare (ignorable slot-name))
  (let ((slotd (call-next-method)))
    (with-slots (column-type not-null primary-key) slotd
      (setf column-type (some #'da-column-type direct-slot-definitions))
      (setf primary-key (some #'da-primary-key direct-slot-definitions))
      (setf not-null (some #'da-not-null direct-slot-definitions)))
    slotd))

#+nil(defmethod initialize-instance :after ((class da-class) &rest all-keys)
  (declare (ignorable all-keys))
  (setf (slot-value class 'columns)
	(mapcar #'slot-definition-name (class-slots class)))
  (setf (slot-value class 'primary-keys)
	(mapcar #'slot-definition-name
		(remove-if-not #'da-primary-key (class-slots class))))
  (setf (slot-value class 'not-null-columns)
	(mapcar #'slot-definition-name
		(remove-if-not #'(lambda (sd)
				   (or (da-primary-key sd) (da-not-null sd)))
			       (class-slots class)))))

(defgeneric da-schema (class-designator)
  (:method ((class-designator da-class))
    `(:create :table ,(class-name class-designator)
	      ,(remove-if #'null
			  (mapcar #'column-definition-from-slot-definition
				  (class-slots class-designator)))))
  (:method ((class-designator symbol))
    (get-schema (find-class class-designator))))

(defun column-definition-from-slot-definition (slot-definition)
  (when (da-column-type slot-definition)
    `(,(slot-definition-name slot-definition)
       ,(da-column-type slot-definition)
       ,@(cond ((da-primary-key slot-definition) '(:primary :key))
	       ((da-not-null slot-definition) '(:not :null))))))

(defgeneric insert-da (da))

(defgeneric update-da (da))

(defgeneric delete-da (da))

(defgeneric get-da (class-name &rest args &key &allow-other-keys))

(defun analyze-slot-definition (slot-definition)
  (destructuring-bind (slot-name &key column-type primary-key not-null
				 &allow-other-keys)
      slot-definition
    (values slot-name column-type primary-key not-null)))

(defun analyze-slot-definitions (slot-definitions)
  (let ((primary-keys nil)
	(not-null nil)
	(others nil)
	(all nil))
    (mapc (lambda (sd)
	    (multiple-value-bind (slot-name column-type primary-key not-null)
		(analyze-slot-definition sd)
	      (when column-type
		(push slot-name all)
		(cond (primary-key (push slot-name primary-keys))
		      (not-null (push slot-name not-null))
		      (t (push slot-name others))))))
	  slot-definitions)
    (values (nreverse all)
	    (nreverse primary-keys)
	    (nreverse not-null)
	    (nreverse others))))

(defmacro define-da-class (class-name superclasses slots &rest class-options)
  (multiple-value-bind (all primary-keys not-null others)
      (analyze-slot-definitions slots)
    `(progn
       (defclass ,class-name ,superclasses
	 ,slots
	 (:metaclass da-class)
	 ,@class-options)
       (defmethod insert-da ((da ,class-name))
	 (with-slots ,all da
	   (exec (:insert :into ,class-name ,all :values
			  ,(mapcar (lambda (cname)
				     `(:embed ,cname))
				   all)))))
       (defmethod update-da ((da ,class-name))
	 (with-slots ,all da
	   (exec (:update ,class-name :set
			  (:columns ,@(mapcar (lambda (cname)
						`(,cname := (:embed ,cname)))
					      (append not-null others)))
			  :where (:and ,@(mapcar (lambda (cname)
						    `(:= ,cname (:embed ,cname)))
						  primary-keys))))))
       (defmethod delete-da ((da ,class-name))
	 (with-slots ,primary-keys da
	   (exec (:delete :from ,class-name
			  :where (:and ,@(mapcar (lambda (cname)
						   `(:= ,cname (:embed ,cname)))
						 primary-keys))))))
       (defmethod get-da ((da-class (eql ,class-name))
			  &key ,@primary-keys)
	 (let ((data (car
		      (exec (:select (:columns ,@all)
				     :from ,class-name
				     :where (:and ,@(mapcar (lambda (cname)
							      `(:= ,cname (:embed ,cname)))
							    primary-keys))))))
	       (new-da (make-instance ',class-name)))
	   (destructuring-bind ,all data
	     ,@(mapcar (lambda (cname)
			 `(setf (slot-value new-da ',cname) ,cname))
		       all))
	   new-da)))))
