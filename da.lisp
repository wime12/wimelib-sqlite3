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

(defgeneric primary-keys (da-or-da-class)
  (:method ((da-name symbol))
    (when (not (class-finalized-p (find-class da-name)))
      (finalize-inheritance (find-class da-name)))
    (primary-keys (find-class da-name)))
  (:method ((da-class da-class))
    (mapcar #'slot-definition-name (primary-key-slots da-class)))
  (:method (da)
    (mapcar #'(lambda (key-name) (cons key-name (slot-value da key-name)))
	    (primary-keys (class-of da)))))

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

(defgeneric select-das (class-name &key where)
  (:method ((class-name symbol) &key where)
    (let ((class (find-class class-name)))
      (unless (class-finalized-p class)
	(finalize-inheritance class)))
    (funcall #'select-das class-name :where where)))

(defun make-defmethod-exps (class)
  (let ((class-name (class-name class))
	(table-name (table-name class))
	(all-columns (mapcar #'column-name (persistent-slots class)))
	(primary-keys (mapcar #'column-name (primary-key-slots class))))
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
			:set (:columns ,@(make-assign-values-exp all-columns))
			:where ,(where-exps primary-keys)))))))

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
    `(defmethod delete-da ((,da ,class-name))
       (with-slots ,primary-keys ,da
	 (exec (:delete :from ,table-name
			:where ,(where-exps primary-keys)))))))

(defun make-get-da-exp (class-name table-name all-columns primary-keys)
  (let ((data (make-symbol "DATA"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod get-da ((da-class (eql ',class-name)) &key ,@primary-keys)
       (assert (and ,@primary-keys) (,@primary-keys)
	       "Values for all primary keys must be provided.")
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
