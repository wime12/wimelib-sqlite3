(in-package #:wimelib-sqlite3)

(define-condition da-error (error)
  ((da :reader da-error-da)))

(define-condition da-does-not-exist-error (da-error)
  ())

(define-condition da-exists-error (da-error)
  ())

(defclass da-class (standard-class)
  ((table-name :initarg :table-name :initform nil :accessor da-class-table-name))
  (:documentation "Metaclass for database access objects."))

(defgeneric da-class-table-name (da-class)
  (:method ((class da-class))
    (or (car (slot-value class 'table-name)) (class-name class))))

(defgeneric (setf da-class-table-name) (da-class new-value)
  (:method ((class da-class) new-value)
    (setf (slot-value class 'table-name) (list new-value))))

(defmethod validate-superclass ((class da-class) (super-class standard-class))
  t)

(defgeneric da-slot-column-type (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defgeneric da-slot-primary-key (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defgeneric da-slot-not-null (slot-definition)
  (:method (sd)
    (declare (ignorable sd))
    nil))

(defclass da-standard-direct-slot-definition (standard-direct-slot-definition)
  ((column-name :initform nil :initarg :column-name :reader da-slot-column-name)
   (column-type :initform nil :initarg :column-type :reader da-slot-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-slot-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-slot-not-null)))

(defclass da-standard-effective-slot-definition
    (standard-effective-slot-definition)
  ((column-name :initform nil :initarg :column-name :reader da-slot-column-name)
   (column-type :initform nil :initarg :column-type :reader da-slot-column-type)
   (primary-key :initform nil :initarg :primary-key :reader da-slot-primary-key)
   (not-null :initform nil :initarg :not-null :reader da-slot-not-null)))

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
      (let ((ct (some #'da-slot-column-type direct-slot-definitions))
	    (pk (some #'da-slot-primary-key direct-slot-definitions))
	    (nn (some #'da-slot-not-null direct-slot-definitions)))
	(setf column-type (or ct pk nn))
	(setf primary-key pk)
	(setf not-null (or pk nn))))
    slotd))

(defun persistent-slots (da-class)
  (remove-if-not #'da-slot-column-type (class-slots da-class)))

(defun primary-key-slots (da-class)
  (remove-if-not #'da-slot-primary-key (class-slots da-class)))

(defun not-null-slots (da-class)
  (remove-if-not #'da-slot-not-null (class-slots da-class)))

(defgeneric ensure-class-finalized (class-designator)
  (:method ((class-name symbol))
    (ensure-class-finalized (find-class class-name)))
  (:method ((class standard-class))
    (unless (class-finalized-p class)
      (finalize-inheritance class)
      t)))

(defgeneric primary-keys (da-or-da-class)
  (:method ((da-name symbol))
    (ensure-class-finalized da-name)
    (primary-keys (find-class da-name)))
  (:method ((da-class da-class))
    (mapcar #'da-slot-column-name (primary-key-slots da-class)))
  (:method (da)
    (mapcar #'(lambda (key-name) (cons key-name (slot-value da key-name)))
	    (primary-keys (class-of da)))))

(defgeneric persistent-columns (da-or-da-class)
  (:method ((da-name symbol))
    (ensure-class-finalized da-name)
    (persistent-columns (find-class da-name)))
  (:method ((da-class da-class))
    (mapcar #'da-slot-column-name (persistent-slots da-class)))
  (:method (da)
    (mapcar #'(lambda (column-name) (cons column-name (slot-value da column-name)))
	    (persistent-columns (class-of da)))))

(defmethod finalize-inheritance :after ((class da-class))
  (if (slot-value class 'table-name)
      (destructuring-bind (table-name) (slot-value class 'table-name)
	(setf (slot-value class 'table-name) table-name))
      (setf (slot-value class 'table-name) (class-name class)))
  (dolist (pslot (persistent-slots class))
    (when (null (slot-value pslot 'column-name))
      (setf (slot-value pslot 'column-name)
	    (slot-definition-name pslot))))
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
  (when (da-slot-column-type slot-definition)
    `(,(slot-definition-name slot-definition)
       ,@(let ((column-type (da-slot-column-type slot-definition)))
	      (cond ((eql column-type t) nil)
		    (t (list column-type))))
       ,@(cond ((da-slot-primary-key slot-definition) '(:primary :key))
	       ((da-slot-not-null slot-definition) '(:not :null))))))

(defgeneric insert-da (da &optional deep))

(defgeneric update-record (da &optional deep))

(defgeneric refresh-da (da))

(defgeneric delete-da (da &optional deep))

(defgeneric get-da (class-name &rest args &key &allow-other-keys)
  (:method ((class-name symbol) &rest args &key &allow-other-keys)
    (unless (ensure-class-finalized class-name)
      (error "No primary keys specified for class ~A" class-name))
    (apply #'get-da class-name args)))

(defgeneric select-das (class-name &key where)
  (:method ((class-name symbol) &key where)
    (ensure-class-finalized class-name)
    (funcall #'select-das class-name :where where)))

(defun make-defmethod-exps (class)
  (let ((class-name (class-name class))
	(da-class-precedence-list (remove-if-not (lambda (c) (typep c 'da-class))
						 (class-precedence-list class)))
	(table-name (da-class-table-name class))
	(all-columns (mapcar #'da-slot-column-name (persistent-slots class)))
	(primary-keys (mapcar #'da-slot-column-name (primary-key-slots class))))
    (when all-columns
      (list* (make-insert-da-exp class table-name all-columns)
	     (make-select-das-exp class-name table-name all-columns)
	     (when primary-keys
	       (list
		(make-update-record-exp class table-name all-columns primary-keys)
		(make-refresh-da-exp class table-name all-columns primary-keys)
		(make-delete-da-exp class table-name primary-keys)
		(make-get-da-exp class-name table-name all-columns primary-keys)))))))

(defun make-insert-da-exp (class-name table-name all-columns)
  (let ((da (make-symbol "DA")))
    `(defmethod insert-da ((,da ,class-name) &optional deep)
       (with-slots ,all-columns ,da
	 (exec (:insert :into ,table-name ,all-columns
			:values ,(mapcar (lambda (cname)
					   `(:embed ,cname))
					 all-columns))))
       (when deep (call-next-method)))))

(defun make-update-record-exp (class-name table-name all-columns primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod update-record ((,da ,class-name) &optional deep)
       (with-slots ,all-columns ,da
	 (exec (:update ,table-name
			:set (:columns ,@(make-assign-values-exp all-columns))
			:where ,(where-exps primary-keys))))
       (when deep (call-next-method)))))

(defun make-assign-values-exp (columns)
  (mapcar (lambda (cname)
	    `(,cname := (:embed ,cname)))
	  columns))

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
		   :where ,(where-exps primary-keys)))))))
	 (destructuring-bind ,all-columns ,result
	   ,(make-set-slots-exp da all-columns)))
       ,da)))

(defun make-delete-da-exp (class-name table-name primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod delete-da ((,da ,class-name) &optional deep)
       (with-slots ,primary-keys ,da
	 (exec (:delete :from ,table-name
			:where ,(where-exps primary-keys))))
       (when deep (call-next-method)))))

(defun make-get-da-exp (class-name table-name all-columns primary-keys)
  (let ((data (make-symbol "DATA"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod get-da ((da-class (eql ',class-name)) &key ,@primary-keys)
       (assert (and ,@primary-keys) ,primary-keys
	       "Values for all primary keys must be provided: ~A" ',primary-keys)
       (let ((,data (car
		     (query
		      (:select (:columns ,@all-columns)
			       :from ,table-name
			       :where ,(where-exps primary-keys))))))
	 (when ,data
	   (let ((,new-da (make-instance ',class-name)))
	     (destructuring-bind ,all-columns ,data
	       ,(make-set-slots-exp new-da all-columns))
	     ,new-da))))))

#+nil(defun make-joined-query-exp-single (class-precedence-list all-columns
				     primary-keys)
  (if (not (cdr class-precedence-list))
      `(:select (:columns ,@all-columns)
		:from ,table-name
		:where ,(where-exps primary-keys))
      `(:select (:columns ,@(make-joined-query-columns class-precedence-list))
		:join )))

;; (:select (:columns (:dot table1 c1) (:dot table1 c2)
;;     (:dot table2 c3

#+nil(defun column-list (class exclude)
  (let ((direct-superclasses (class-direct-superclasses class))
	(column-names (column-names class))
	(class-name (class-name class)))
    (append (mapcar (lambda (c) `(:dot class-name c))
		    (set-difference column-names exclude))
	    (mapcan (lambda (class) (column-list class
						 ))))))

(defun where-exp (column)
  `(:= ,column (:embed ,column)))

(defun where-exps (columns)
  (if (cdr columns)
      `(:and ,@(mapcar #'where-exp columns))
      (where-exp (car columns))))

(defun make-select-das-exp (class-name table-name all-columns)
  (let ((result (make-symbol "RESULT"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod select-das ((da-class (eql ',class-name)) &key where)
       (let ((,result nil))
	 (if where
	     (do-query ,all-columns
		 (:select (:columns ,@all-columns) :from ,table-name
			  :where (:embed where))
	       (let ((,new-da (make-instance ',class-name)))
		 ,(make-set-slots-exp new-da all-columns)
		 (push ,new-da ,result)))
	     (do-query ,all-columns
		 (:select (:columns ,@all-columns) :from ,table-name)
	       (let ((,new-da (make-instance ',class-name)))
		 ,(make-set-slots-exp new-da all-columns)
		 (push ,new-da ,result))))
	 (nreverse ,result)))))

(defun make-set-slots-exp (da all-columns)
  (when all-columns
    `(setf ,@(mapcan (lambda (column) `((slot-value ,da ',column) ,column))
		     all-columns))))

;; TODO: Inheritance

(defun find-first-persistent-slot-class (slot-name reversed-cpl)
  (find-if (lambda (c) (contains-persistent-slot-p slot-name c))
	   reversed-cpl))

(defun contains-persistent-slot-p (slot-name class)
  (let ((slotd (find slot-name (class-direct-slots class)
		     :key #'slot-definition-name)))
    (when slotd
      (persistent-slot-p slotd))))

(defun persistent-slot-p (slotd)
  (or (da-slot-column-type slotd)
      (da-slot-primary-key slotd)
      (da-slot-not-null slotd)))

(defun slots-table-names (persistent-slots reversed-cpl)
  (let ((slot-names (mapcar #'slot-definition-name persistent-slots)))
    (values
     (mapcar (lambda (slot-name)
	       (da-class-table-name (find-first-persistent-slot-class
				     slot-name reversed-cpl)))
	     slot-names)
     slot-names)))

(defun da-class-slot-tables (class)
  (slots-table-names (persistent-slots class) (reverse (class-precedence-list class))))
