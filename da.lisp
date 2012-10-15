(in-package #:wimelib-sqlite3)

(define-condition da-error (error)
  ((da :reader da-error-da)))

(define-condition da-does-not-exist-error (da-error)
  ())

(define-condition da-exists-error (da-error)
  ())

(defclass da-class (standard-class)
  ((table-name :initarg :table-name :initform nil)
   (primary-key :initarg :primary-key :initform nil :accessor da-class-primary-key))
  (:documentation "Metaclass for database access objects."))

(defmethod validate-superclass ((class da-class) (super-class standard-class))
  t)

(defclass da-object ()
  ()
  (:metaclass da-class))

(defmethod print-object ((da da-object) stream)
  (print-unreadable-object (da stream :type t :identity t)
    (primary-keys da)
    (format stream "~{~{:~A ~A~}~^ ~}"
	    (mapcar (lambda (p) `(,(car p) ,(cdr p)))
		    (primary-keys da)))))

(defmethod initialize-instance :around
    ((class da-class) &rest initargs &key direct-superclasses)
  (declare (dynamic-extent initargs))
  (let ((da-object-class (find-class 'da-object)))
    (if (some #'(lambda (c) (subtypep c da-object-class)) direct-superclasses)
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

(defgeneric da-class-table-name (da-class)
  (:method ((class da-class))
    (or (car (slot-value class 'table-name)) (class-name class))))

(defgeneric (setf da-class-table-name) (da-class new-value)
  (:method ((class da-class) new-value)
    (setf (slot-value class 'table-name) (list new-value))))

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
  ((column-name :initarg :column-name :reader da-slot-column-name)
   (column-type :initarg :column-type :reader da-slot-column-type)
   (unique :initarg :unique :reader da-slot-unique)
   (not-null :initarg :not-null :reader da-slot-not-null)))

(defclass da-standard-effective-slot-definition
    (standard-effective-slot-definition)
  ((column-name :initarg :column-name :reader da-slot-column-name)
   (column-type :initarg :column-type :reader da-slot-column-type)
   (unique :initarg :unique :reader da-slot-unique)
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
    (with-slots (column-type not-null unique column-name) slotd
      (let ((ct-slot (find-if (lambda (sd) (slot-boundp sd 'column-type))
			      direct-slot-definitions))
	    (nn-slot (find-if (lambda (sd) (slot-boundp sd 'not-null))
			      direct-slot-definitions))
	    (un-slot (find-if (lambda (sd) (slot-boundp sd 'unique))
			      direct-slot-definitions)))
	(let ((ct (if ct-slot (slot-value ct-slot 'column-type) t))
	      (nn (if nn-slot (slot-value nn-slot 'not-null) nil))
	      (un (if un-slot (slot-value un-slot 'not-null) nil)))
	  (setf column-type (or ct nn un))
	  (if column-type
	      (setf not-null nn
		    unique un
		    column-name (or (and (slot-boundp
                                          (car direct-slot-definitions)
                                          'column-name)
                                         (da-slot-column-name
				          (car direct-slot-definitions)))
				    slot-name))
	      (setf not-null nil
		    unique nil
		    column-name nil)))))
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
    (primary-keys (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-class-finalized da-class)
    (slot-names (primary-key-slots da-class)))
  (:method (da)
    (map-slot-names-to-values da (primary-keys (class-of da)))))

(defun slot-names (slotds)
  (mapcar #'slot-definition-name slotds))

(defgeneric persistent-columns (da-or-da-class)
  (:method ((da-name symbol))
    (persistent-columns (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-class-finalized da-class)
    (slot-column-names (persistent-slots da-class)))
  (:method ((da da-object))
    (map-slot-names-to-values da (persistent-columns (class-of da)))))

(defgeneric not-null-columns (da-or-da-class)
  (:method ((da-name symbol))
    (persistent-columns (find-class da-name)))
  (:method ((da-class da-class))
    (ensure-class-finalized da-class)
    (slot-column-names (not-null-slots da-class)))
  (:method ((da da-object))
    (map-slot-names-to-values da (not-null-columns (class-of da)))))

(defun slot-column-names (slots)
  (mapcar #'da-slot-column-name slots))

(defun map-slot-names-to-values (da slot-names)
  (mapcar #'(lambda (slot-name) (cons slot-name (slot-value da slot-name)))
	  slot-names))

(defmethod finalize-inheritance :after ((class da-class))
  (let ((lonely-primary-key
	 (set-difference (da-class-primary-key class)
			 (slot-names (persistent-slots class)))))
    (when lonely-primary-key
      (error "Primary key for class ~A: no slot~p ~{~A~^, ~}"
	     (class-name class) (length lonely-primary-key) lonely-primary-key)))
  (funcall (compile nil `(lambda () ,@(make-defmethod-exps class)))))

(defgeneric da-schema (class-designator)
  (:method ((class da-class))
    (ensure-finalized class)
    `(:create :table ,(da-class-table-name class)
	      ,(nconc (mapcar #'column-definition-from-slot-definition
		       (persistent-slots class))
		      (primary-key-declaration class))))
  (:method ((class-designator symbol))
    (da-schema (find-class class-designator))))

(defun column-definition-from-slot-definition (slot-definition)
  (when (da-slot-column-type slot-definition)
    `(,(slot-definition-name slot-definition)
       ,@(let ((column-type (da-slot-column-type slot-definition)))
	      (cond ((eql column-type t) nil)
		    (t (list column-type))))
       ,@(when (da-slot-unique slot-definition) '(:unique))
       ,@(when (da-slot-not-null slot-definition) '(:not :null)))))

(defun primary-key-declaration (class)
  (let ((primary-key (da-class-primary-key class)))
    (when primary-key
      `((:primary-key ,@primary-key)))))

(defgeneric insert-da (da))

(defgeneric update-record (da))

(defgeneric refresh-da (da))

(defgeneric delete-da (da))

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
  (let* ((class-name (class-name class))
	 (table-name (da-class-table-name class))
	 (persistent-slots (persistent-slots class))
	 (primary-key-slots (primary-key-slots class))
	 (all-column-slot-names (mapcar #'slot-definition-name persistent-slots))
	 (all-columns (mapcar #'da-slot-column-name persistent-slots))
	 (primary-key-slot-names (mapcar #'slot-definition-name primary-key-slots))
	 (primary-keys (mapcar #'da-slot-column-name primary-key-slots)))
    (when all-columns
      (list* (make-insert-da-exp class table-name all-column-slot-names
				 all-columns)
	     (make-select-das-exp class-name table-name all-column-slot-names
				  all-columns)
	     (when primary-keys
	       (list
		(make-update-record-exp class table-name
					all-column-slot-names all-columns
					primary-key-slot-names primary-keys)
		(make-refresh-da-exp class table-name
				     all-column-slot-names all-columns
				     primary-key-slot-names primary-keys)
		(make-delete-da-exp class table-name
				    primary-key-slot-names primary-keys)
		(make-get-da-exp class-name table-name
				 all-column-slot-names all-columns
				 primary-key-slot-names primary-keys)))))))

(defun make-insert-da-exp (class-name table-name all-column-slot-names all-columns)
  (let ((da (make-symbol "DA")))
    `(defmethod insert-da ((,da ,class-name))
       (with-slots ,all-column-slot-names ,da
	 (exec (:insert :into ,table-name ,all-columns
			:values ,(mapcar (lambda (cname)
					   `(:embed ,cname))
					 all-column-slot-names)))))))

(defun make-update-record-exp (class-name table-name
			       all-column-slot-names all-columns
			       primary-key-slot-names primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod update-record ((,da ,class-name))
       (with-slots ,all-column-slot-names ,da
	 (exec (:update ,table-name
			:set (:columns ,@(make-assign-values-exp
					  all-column-slot-names all-columns))
			:where ,(where-exps
				 primary-key-slot-names primary-keys)))))))

(defun make-assign-values-exp (column-slots column-names)
  (mapcar (lambda (cslot cname)
	    `(,cname := (:embed ,cslot)))
	  column-slots column-names))

(defun make-refresh-da-exp (class table-name
			    all-column-slot-names all-columns
			    primary-key-slot-names primary-keys)
  (let ((da (make-symbol "DA"))
	(result (make-symbol "RESULT")))
    `(defmethod refresh-da ((,da ,class))
       (let ((,result
	      (with-slots ,primary-key-slot-names ,da
		(car
		 (query
		  (:select
		   (:columns ,@all-columns)
		   :from ,table-name
		   :where ,(where-exps primary-key-slot-names primary-keys)))))))
	 (destructuring-bind ,all-column-slot-names ,result
	   ,(make-set-slots-exp da all-column-slot-names)))
       ,da)))

(defun make-delete-da-exp (class-name table-name
			   primary-key-slot-names primary-keys)
  (let ((da (make-symbol "DA")))
    `(defmethod delete-da ((,da ,class-name))
       (with-slots ,primary-key-slot-names ,da
	 (exec (:delete :from ,table-name
			:where ,(where-exps
				 primary-key-slot-names primary-keys)))))))

(defun make-get-da-exp (class-name table-name
			all-column-slot-names all-columns
			primary-key-slot-names primary-keys)
  (let ((data (make-symbol "DATA"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod get-da ((da-class (eql ',class-name))
			&key ,@primary-key-slot-names)
       (assert (and ,@primary-key-slot-names) ,primary-key-slot-names
	       "Values for all primary keys must be provided: ~A" ',primary-keys)
       (let ((,data (car
		     (query
		      (:select (:columns ,@all-columns)
			       :from ,table-name
			       :where ,(where-exps primary-key-slot-names
						   primary-keys))))))
	 (when ,data
	   (let ((,new-da (make-instance ',class-name)))
	     (destructuring-bind ,all-column-slot-names ,data
	       ,(make-set-slots-exp new-da all-column-slot-names))
	     ,new-da))))))

(defun where-exp (column-slot column-name)
  `(:= ,column-name (:embed ,column-slot)))

(defun where-exps (column-slots column-names)
  (if (cdr column-names)
      `(:and ,@(mapcar #'where-exp column-slots column-names))
      (where-exp (car column-slots) (car column-names))))

(defun make-select-das-exp (class-name table-name all-column-slot-names
			    all-columns)
  (let ((result (make-symbol "RESULT"))
	(new-da (make-symbol "NEW-DA")))
    `(defmethod select-das ((da-class (eql ',class-name)) &key where)
       (let ((,result nil))
	 (if where
	     (do-query ,all-column-slot-names
		 (:select (:columns ,@all-columns) :from ,table-name
			  :where (:embed where))
	       (let ((,new-da (make-instance ',class-name)))
		 ,(make-set-slots-exp new-da all-column-slot-names)
		 (push ,new-da ,result)))
	     (do-query ,all-column-slot-names
		 (:select (:columns ,@all-columns) :from ,table-name)
	       (let ((,new-da (make-instance ',class-name)))
		 ,(make-set-slots-exp new-da all-column-slot-names)
		 (push ,new-da ,result))))
	 (nreverse ,result)))))

(defun make-set-slots-exp (da all-columns)
  (when all-columns
    `(setf ,@(mapcan (lambda (column) `((slot-value ,da ',column) ,column))
		     all-columns))))
