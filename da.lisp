(in-package #:wimelib-sqlite3)

;; TODO: default values

(defclass da-class (standard-class)
  ((table-name :initarg :table-name :initform nil)
   (primary-key :initarg :primary-key :initform nil
		:accessor da-class-primary-key)
   (foreign-keys :initarg :foreign-keys :initform nil
		 :accessor da-class-foreign-keys)
   (unique :initarg :unique :initform nil
	    :accessor da-class-unique)
   (slot-column-mapping :initarg :slot-column-mapping
			:accessor da-class-slot-column-mapping))
  (:documentation "The metaclass for database access objects.

Classes of this metaclass accept two new class options: :TABLE-NAME and
:PRIMARY-KEY. The argument to :TABLE-NAME must be a symbol. It is
converted to an SQL identifier, which links the class to a table in a
database. The symbols following :PRIMARY-KEY designate the slot names
of the class which together form the primary key of the database table.

The slot definitions of database access classes accept the keywords
:COLUMN-TYPE, :UNIQUE, :NOT-NULL, :COLUMN-NAME and :REFERENCE in addition
to the standard keywords.

The argument to :COLUMN-TYPE can be any valid SQL type specifier
denoting the type affinity of the column in the database, T for
no type affinity or NIL meaning the slot is not connected to a
column in the database table. The default is T. It will also be
set to T if any of :UNIQUE, :NOT-NULL, :COLUMN-NAME or :REFERENCE
are not NIL and :COLUMN-TYPE was NIL in the slot definition.

The argument to :COLUMN-NAME, which must be a symbol, is converted to
an SQL identifier to access the corresponding column in the database
table.

:UNIQUE, :NOT-NULL and :REFERENCE are only used for generating the database
schema from the class definition. They denote if a column can contain
only unqique values, if it must not contain null values or if it references
a slot in another da-class, respectively. They do not impose any restrictions
on the values in the slot.

The argument to :REFERENCE is a list of two elements. The first element
is a class name and the second one a slot name."))

(defmethod validate-superclass ((class da-class) (super-class standard-class))
  t)

(defclass da-object ()
  ())

(defmethod print-object ((da da-object) stream)
  (print-unreadable-object (da stream :type t :identity t)
    (format stream "~{~{:~A ~A~}~^ ~}"
	    (mapcar (lambda (p) `(,(car p) ,(cdr p)))
		    (primary-key da)))))

(defmethod initialize-instance :around
    ((class da-class) &rest initargs &key direct-superclasses)
  (declare (dynamic-extent initargs))
  (let ((da-object-class (find-class 'da-object)))
    (if (some #'(lambda (c) (typep c da-object-class)) direct-superclasses)
	(call-next-method)
	(apply #'call-next-method
	       class
	       :direct-superclasses (append direct-superclasses
					    (list da-object-class))
	       initargs))))

(defmethod reinitialize-instance :around
    ((class da-class)
     &rest initargs
     &key (direct-superclasses '() direct-superclasses-p))
  (declare (dynamic-extent initargs))
  (if direct-superclasses-p
      (let ((da-object-class (find-class 'da-object)))
	(if (some #'(lambda (c) (subtypep c da-object-class)) direct-superclasses)
	    (call-next-method)
	    (apply #'call-next-method
		   class
		   :direct-superclasses (append direct-superclasses
						(list da-object-class))
		   initargs)))
      (call-next-method)))

;; TODO: (check-schema da-class) einführen

(defclass da-standard-direct-slot-definition (standard-direct-slot-definition)
  ((column-name :initarg :column-name :reader da-slot-column-name)
   (column-type :initarg :column-type :reader da-slot-column-type)
   (not-null :initarg :not-null :reader da-slot-not-null)))

(defclass da-standard-effective-slot-definition
    (standard-effective-slot-definition)
  ((column-name :initarg :column-name :reader da-slot-column-name)
   (column-type :initarg :column-type :reader da-slot-column-type)
   (not-null :initarg :not-null :reader da-slot-not-null)))

(defmethod direct-slot-definition-class ((class da-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'da-standard-direct-slot-definition))

(defmethod effective-slot-definition-class ((class da-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'da-standard-effective-slot-definition))

(defmethod compute-effective-slot-definition :around
    ((class da-class) slot-name direct-slot-definitions)
  (declare (optimize debug))
  (let ((slotd (call-next-method)))
    (with-slots (column-type not-null column-name) slotd
      (let ((ct-slot (find-if (lambda (sd) (slot-boundp sd 'column-type))
			      direct-slot-definitions))
	    (nn-slot (find-if (lambda (sd) (slot-boundp sd 'not-null))
			      direct-slot-definitions)))
	(let ((ct (if ct-slot (slot-value ct-slot 'column-type) t))
	      (nn (if nn-slot (slot-value nn-slot 'not-null) nil)))
	  (setf column-type (or ct nn
				(and (slot-boundp slotd 'column-name)
				     (not (null column-name)))))
	  (if column-type
	      (setf not-null nn
		    column-name (or (and (slot-boundp
                                          (car direct-slot-definitions)
                                          'column-name)
                                         (da-slot-column-name
				          (car direct-slot-definitions)))
				    slot-name))
	      (setf not-null nil
		    column-name nil)))))
    slotd))

(defun persistent-slots (da-class)
  (remove-if-not #'da-slot-column-type (class-slots da-class)))

(defun primary-key-slots (da-class)
  (let ((key-names (da-class-primary-key da-class)))
    (remove-if-not (lambda (sd-name) (member sd-name key-names))
		   (class-slots da-class) :key #'slot-definition-name)))

(defun not-null-slots (da-class)
  (remove-if-not #'da-slot-not-null (class-slots da-class)))

(defgeneric primary-key (da-or-da-class)
  (:documentation "Returns the slot-names constituting the primary key if a
class or class name is given or a plist of column names and values
if a database access object is given.")
  (:method ((da-name symbol))
    (primary-key (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-finalized da-class)
    (da-class-primary-key da-class))
  (:method (da)
    (map-slot-names-to-values da (primary-key (class-of da)))))

(defun slot-names (slotds)
  (mapcar #'slot-definition-name slotds))

(defgeneric persistent-columns (da-or-da-class)
  (:method ((da-name symbol))
    (persistent-columns (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-finalized da-class)
    (slot-column-names (persistent-slots da-class)))
  (:method ((da da-object))
    (map-slot-names-to-values da (persistent-columns (class-of da)))))

(defgeneric not-null-columns (da-or-da-class)
  (:method ((da-name symbol))
    (persistent-columns (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-finalized da-class)
    (slot-column-names (not-null-slots da-class)))
  (:method ((da da-object))
    (map-slot-names-to-values da (not-null-columns (class-of da)))))

(defun slot-column-names (slots)
  (mapcar #'da-slot-column-name slots))

(defun map-slot-names-to-values (da slot-names)
  (mapcar #'(lambda (slot-name) (cons slot-name (slot-value da slot-name)))
	  slot-names))

(defmethod finalize-inheritance :after ((class da-class))
  (let ((deserted-primary-key-slots
	 (set-difference (da-class-primary-key class)
			 (slot-names (persistent-slots class)))))
    (when deserted-primary-key-slots
      (error "Primary key for class ~A: no slot~p ~{~A~^, ~}"
	     (class-name class) (length deserted-primary-key-slots)
	     deserted-primary-key-slots)))
  (setf (da-class-slot-column-mapping class)
	(mapcar #'(lambda (slotd)
		    (cons (slot-definition-name slotd)
			  (da-slot-column-name slotd)))
		(persistent-slots class)))
  (funcall (compile nil `(lambda () ,@(make-defmethod-exps class)))))

(defun da-class-table-name (da-class)
  (or (car (slot-value da-class 'table-name)) (class-name da-class)))

(defgeneric da-class-column-name (class slot-name)
  (:method ((class da-class) slot-name)
    (ensure-finalized class)
    (cdr (assoc slot-name (da-class-slot-column-mapping class))))
  (:method ((class symbol) slot-name)
    (da-class-column-name (find-class class) slot-name)))

(defgeneric da-class-schema (class-designator)
  (:method ((class da-class))
    (ensure-finalized class)
    `(:create :table ,(da-class-table-name class)
	      ,(nconc (mapcar #'(lambda (slotd)
				  (column-definition-from-slot-definition
				   class slotd)) 
		       (persistent-slots class))
		      (primary-key-declaration class)
		      (unique-declaration class)
		      (foreign-key-declarations class))))
  (:method ((class-designator symbol))
    (da-class-schema (find-class class-designator))))

(defun column-definition-from-slot-definition (class slot-definition)
  (when (da-slot-column-type slot-definition)
    `(,(da-class-column-name class (slot-definition-name slot-definition))
       ,@(let ((column-type (da-slot-column-type slot-definition)))
	      (cond ((eql column-type t) nil)
		    (t (list column-type))))
       ,@(when (da-slot-not-null slot-definition) '(:not :null)))))

(defun primary-key-declaration (class)
  (let ((primary-key (da-class-primary-key class)))
    (when primary-key
      `((:splice :primary :key ,(mapcar #'(lambda (pk)
					    (da-class-column-name class pk))
					primary-key))))))

(defun unique-declaration (class)
  (let ((unique (da-class-unique class)))
    (when unique
      `((:splice :unique ,@(mapcar #'(lambda (un)
				       (da-class-column-name class un))
				   unique))))))

;; TODO: FOREIGN-KEY-DECLARATIONS komplettieren

(defun foreign-key-declarations (class)
  (let ((foreign-keys (da-class-foreign-keys class)))
    (when foreign-keys
      (collecting
	(do ((tail foreign-keys (cddr tail)))
	    ((endp tail))
	  (let* ((cols (mapcar (lambda (sn) (da-class-column-name class sn))
			       (ensure-list (car tail))))
		 (ref (ensure-list (cadr tail)))
		 (ref-table (da-class-table-name (find-class (car ref))))
		 (ref-columns (mapcar (lambda (sn) (da-class-column-name (car ref)
									 sn))
				      (cdr ref))))
	    (collect `(:splice :foreign :key ,cols
			       :references ,@(if (cdr ref)
						 (list ref-table ref-columns)
						 (list ref-table))))))))))

(defgeneric insert-da (da)
  (:documentation "Inserts a dabase access object into the database.
If any constraints are violated, an error is signalled."))

(defgeneric update-record (da)
  (:documentation "Updates an existing record in the database."))

(defgeneric %refresh-da (da))

(defgeneric refresh-da (da)
  (:documentation "Updates the slots of a database access object with
the values of its record in the database. If the update was
successful then the updated object is returned, NIL otherwise.")
  (:method (da)
    (%refresh-da da)))

(defgeneric delete-da (da)
  (:documentation "Deletes the record of a database object in the database."))

;; TODO: In GET-DA die Slotnamen oder die Initargs für die Parameter des
;; Schlüssels benutzen?

(defgeneric get-da (class-name &rest initargs &key &allow-other-keys)
  (:documentation "Creates a database access object from its record
in the database. The record is found by using the primary key given
as keyword arguments. If nothing is found the result is NIL.")
  (:method ((class-name symbol) &rest initargs &key &allow-other-keys)
    (let ((class (find-class class-name)))
      (ensure-finalized class)
      (%refresh-da (apply #'make-instance class initargs)))))

(defgeneric select-das (class-name &key where order-by limit)
  (:documentation "Returns a list of database access objects
of the given da-class. The list of objects can be restricted
by providing an SQL form for the WHERE clause and a number
for the LIMIT clause. The sorting is determind by the ODER-BY
clause.

Example
  (select-das 'customers :where '(:> age 21)
                         :order-by '(:random)
                         :limit 5)")
  (:method ((class-name symbol) &key where order-by limit)
    (ensure-finalized (find-class class-name))
    (funcall #'select-das class-name :where where
	     :order-by order-by :limit limit)))

;;; The following functions generate the code for the methods
;;; which are added during inheritance finalization.

(defun make-defmethod-exps (class)
  (let* ((class-name (class-name class))
	 (table-name (da-class-table-name class))
	 (persistent-slots (persistent-slots class))
	 (primary-key-slots (primary-key-slots class))
	 (all-column-slot-names (mapcar #'slot-definition-name persistent-slots))
	 (all-columns (mapcar #'da-slot-column-name persistent-slots))
	 (primary-key-slot-names (da-class-primary-key class))
	 (primary-key-column-names
	  (mapcar #'da-slot-column-name primary-key-slots)))
    (when all-columns
      (list* (make-insert-da-exp class table-name all-column-slot-names
				 all-columns)
	     (make-select-das-exp class-name table-name all-column-slot-names
				  all-columns)
	     (when primary-key-slot-names
	       (list
		(make-update-record-exp class table-name
					all-column-slot-names all-columns
					primary-key-slot-names
					primary-key-column-names)
		(make-%refresh-da-exp class table-name
				     all-column-slot-names all-columns
				     primary-key-slot-names
				     primary-key-column-names)
		(make-delete-da-exp class table-name
				    primary-key-slot-names
				    primary-key-column-names)))))))

(defun make-insert-da-exp (class-name table-name all-column-slot-names all-columns)
  (with-unique-names (da)
    `(defmethod insert-da ((,da ,class-name))
       (with-slots ,all-column-slot-names ,da
	 (exec (:insert :into ,table-name ,all-columns
			:values ,(mapcar (lambda (cname)
					   `(:embed ,cname))
					 all-column-slot-names)))))))

(defun make-update-record-exp (class-name table-name
			       all-column-slot-names all-columns
			       primary-key-slot-names primary-keys)
  (with-unique-names (da)
    `(defmethod update-record ((,da ,class-name))
       (with-slots ,all-column-slot-names ,da
	 (exec (:update ,table-name
			:set (:row ,@(make-assign-values-exp
				      all-column-slot-names all-columns))
			:where ,(where-exps
				 primary-key-slot-names primary-keys)))))))

(defun make-assign-values-exp (column-slots column-names)
  (mapcar (lambda (cslot cname)
	    `(,cname := (:embed ,cslot)))
	  column-slots column-names))

(defun make-%refresh-da-exp (class table-name
			    all-column-slot-names all-columns
			    primary-key-slot-names primary-keys)
  (with-unique-names (da result)
    `(defmethod %refresh-da ((,da ,class))
       (let ((,result
	      (with-slots ,primary-key-slot-names ,da
		(car
		 (query
		  (:select
		   (:row ,@all-columns)
		   :from ,table-name
		   :where ,(where-exps primary-key-slot-names primary-keys)))))))
	 (when ,result
	   (destructuring-bind ,all-column-slot-names ,result
	     ,(make-set-slots-exp da all-column-slot-names)
	     ,da))))))

(defun make-delete-da-exp (class-name table-name
			   primary-key-slot-names primary-keys)
  (with-unique-names (da)
    `(defmethod delete-da ((,da ,class-name))
       (with-slots ,primary-key-slot-names ,da
	 (exec (:delete :from ,table-name
			:where ,(where-exps
				 primary-key-slot-names primary-keys)))))))

(defun where-exp (column-slot column-name)
  `(:= ,column-name (:embed ,column-slot)))

(defun where-exps (column-slots column-names)
  (if (cdr column-names)
      `(:and ,@(mapcar #'where-exp column-slots column-names))
      (where-exp (car column-slots) (car column-names))))

(defun make-select-das-exp (class-name table-name all-column-slot-names
			    all-columns)
  (with-unique-names (da)
    `(defmethod select-das ((da-class (eql ',class-name))
			    &key where order-by limit)
       (collecting
	 (do-query ,all-column-slot-names
	     (:select (:row ,@all-columns)
		      :from ,table-name
		      (:embed (when where
				`(:splice :where ,where)))
		      (:embed (when order-by
				`(:splice :order :by ,order-by)))
		      (:embed (when limit
				`(:splice :limit ,limit))))
	   (let ((,da (make-instance ',class-name)))
	     ,(make-set-slots-exp da all-column-slot-names)
	     (collect ,da)))))))

(defun make-set-slots-exp (da all-columns)
  (when all-columns
    `(setf ,@(mapcan (lambda (column) `((slot-value ,da ',column) ,column))
		     all-columns))))

;;; Foreign Key References

(defun da-class-foreign-key-references (da-class)
  (let ((foreign-keys (da-class-foreign-keys da-class)))
    (collecting
      (do ((ref (cdr foreign-keys) (cddr ref)))
	  ((null ref))
	(collect (if (listp (car ref))
		     (caar ref)
		     (car ref)))))))
