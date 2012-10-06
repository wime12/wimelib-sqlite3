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

(defun open-db (name &key toplevel)
  (let ((db (sqlite3-open (namestring name))))
    (when toplevel (setf *db* db))
    db))

(defun close-db (db)
  (sqlite3-close db))

(defmacro with-db ((db) &body body)
  `(let ((*db* ,db))
     ,@body))

(defmacro with-open-db ((name) &body body)
  `(let ((*db* (open-db ,name)))
     (unwind-protect
	  (progn ,@body)
       (close-db *db*))))

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

(defun prepared-result (prepared-stmt &rest args)
  (let ((result nil))
    (apply prepared-stmt (lambda (stmt)
			   (push (column-values stmt) result))
	   args)
    (nreverse result)))

;; DAO

(defclass da-class (standard-class)
  ()
  (:documentation "Metaclass for database access objects."))

(defmethod validate-superclass ((class da-class) (super-class standard-class))
  t)

(defun table-name (da-class)
  (class-name da-class))

(defgeneric da-column-type (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defgeneric da-primary-key (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defgeneric da-not-null (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defclass da-standard-direct-slot-definition (standard-direct-slot-definition)
  ((column-name :initform nil :initarg :column-name :reader da-column-name)
   (column-type :initform nil :initarg :column-type :reader da-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-not-null)))

(defclass da-standard-effective-slot-definition
    (standard-effective-slot-definition)
  ((column-name :initform nil :initarg :column-name :reader da-column-name)
   (column-type :initform nil :initarg :column-type :reader da-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-not-null)))

(defun column-name (slot-definition)
  (or (da-column-name slot-definition) (slot-definition-name slot-definition)))

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
      (let ((ct (some #'da-column-type direct-slot-definitions))
	    (pk (some #'da-primary-key direct-slot-definitions))
	    (nn (some #'da-not-null direct-slot-definitions)))
	(setf column-type (or ct (not (null pk)) (not (null nn))))
	(setf primary-key pk)
	(setf not-null nn)))
    slotd))

(defun persistent-slots (da-class)
  (remove-if-not #'da-column-type (class-slots da-class)))

(defun primary-key-slots (da-class)
  (remove-if-not #'da-primary-key (class-slots da-class)))

(defun not-null-slots (da-class)
  (remove-if-not #'da-not-null (class-slots da-class)))

(defmethod finalize-inheritance :after ((class da-class))
  (funcall (compile nil `(lambda () ,@(make-defmethod-exps class)))))

(defgeneric da-schema (class-designator)
  (:method ((class-designator da-class))
    `(:create :table ,(class-name class-designator)
	      ,(remove-if #'null
			  (mapcar #'column-definition-from-slot-definition
				  (class-slots class-designator)))))
  (:method ((class-designator symbol))
    (da-schema (find-class class-designator))))

(defun column-definition-from-slot-definition (slot-definition)
  (when (da-column-type slot-definition)
    `(,(slot-definition-name slot-definition)
       ,@(let ((column-type (da-column-type slot-definition)))
	      (cond ((eql column-type t) nil)
		    (t (list column-type))))
       ,@(cond ((da-primary-key slot-definition) '(:primary :key))
	       ((da-not-null slot-definition) '(:not :null))))))

(defgeneric insert-da (da))

(defgeneric update-record (da))

(defgeneric refresh-da (da))

(defgeneric delete-da (da))

(defgeneric get-da (class-name &rest args &key &allow-other-keys)
  (:method ((class-name symbol) &rest args &key &allow-other-keys)
    (let ((class (find-class class-name)))
      (if (class-finalized-p class)
	  (error "No primary keys specified for class ~A" class-name)
	  (finalize-inheritance class))
      (apply #'get-da class-name args))))

(defgeneric get-all-das (class-name)
  (:method ((class-name symbol))
    (let ((class (find-class class-name)))
      (unless (class-finalized-p class)
	(finalize-inheritance class)))
    (funcall #'get-all-das class-name)))

(defun make-defmethod-exps (class)
  (let ((class-name (class-name class))
	(table-name (table-name class))
	(all-columns (mapcar #'column-name (persistent-slots class)))
	(primary-keys (mapcar #'column-name (primary-key-slots class))))
    (when all-columns
      (list* (make-insert-da-exp class table-name all-columns)
	     (make-get-all-das-exp class-name table-name all-columns)
	     (when primary-keys
	       (list
		(make-update-record-exp class table-name all-columns primary-keys)
		(make-refresh-da-exp class table-name all-columns primary-keys)
		(make-delete-da-exp class table-name primary-keys)
		(make-get-da-exp class-name table-name all-columns primary-keys)))))))

(defun make-insert-da-exp (class-name table-name all-columns)
  (let ((da (make-symbol "DA")))
    `(defmethod insert-da ((,da ,class-name))
       (with-slots ,all-columns ,da
	 (exec (:insert :into ,table-name ,all-columns
			:values ,(mapcar (lambda (cname)
					   `(:embed ,cname))
					 all-columns)))))))

(defun make-update-record-exp (class-name table-name all-columns primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod update-record ((,da ,class-name))
       (with-slots ,all-columns ,da
	 (exec (:update ,table-name
			:set (:columns ,@(mapcar (lambda (cname)
						   `(,cname := (:embed ,cname)))
						 all-columns))
			:where (:and ,@(mapcar (lambda (cname)
						 `(:= ,cname (:embed ,cname)))
					       primary-keys))))))))

(defun make-refresh-da-exp (class table-name all-columns primary-keys)
  (let ((da (make-symbol "DA"))
	(result (make-symbol "RESULT")))
    `(defmethod refresh-da ((,da ,class))
       (let ((,result
	      (with-slots ,primary-keys ,da
		(car
		 (query
		  (:select
		   (:columns ,@all-columns)
		   :from ,table-name
		   :where (:and
			   ,@(mapcar
			      (lambda (cname) `(:= ,cname (:embed ,cname)))
			      primary-keys))))))))
	 (destructuring-bind ,all-columns ,result
	     ,@(mapcar (lambda (cname)
			 `(setf (slot-value ,da ',cname) ,cname))
		       all-columns)))
       ,da)))

(defun make-delete-da-exp (class-name table-name primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod delete-da ((,da ,class-name))
       (with-slots ,primary-keys ,da
	 (exec (:delete :from ,table-name
			:where (:and ,@(mapcar (lambda (cname)
						 `(:= ,cname (:embed ,cname)))
					       primary-keys))))))))

(defun make-get-da-exp (class-name table-name all-columns primary-keys)
  (let ((data (make-symbol "DATA"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod get-da ((da-class (eql ',class-name)) &key ,@primary-keys)
       (let ((,data (car
		     (query
		      (:select (:columns ,@all-columns)
			       :from ,table-name
			       :where (:and ,@(mapcar (lambda (cname)
							`(:= ,cname (:embed ,cname)))
						      primary-keys))))))
	     (,new-da (make-instance ',class-name)))
	 (destructuring-bind ,all-columns ,data
	   ,@(mapcar (lambda (cname)
		       `(setf (slot-value ,new-da ',cname) ,cname))
		     all-columns))
	 ,new-da))))

(defun make-get-all-das-exp (class-name table-name all-columns)
  (let ((result (make-symbol "RESULT"))
	(stmt (make-symbol "STMT"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod get-all-das ((da-class (eql ',class-name)))
       (let ((,result nil))
	 (do-rows ,stmt (:select (:columns ,@all-columns)
				:from ,table-name)
	   (destructuring-bind ,all-columns (column-values ,stmt)
	     (let ((,new-da (make-instance ',class-name)))
	       ,@(mapcar (lambda (cname)
			   `(setf (slot-value ,new-da ',cname) ,cname))
			 all-columns)
	       (push ,new-da ,result))))
	 (nreverse ,result)))))
