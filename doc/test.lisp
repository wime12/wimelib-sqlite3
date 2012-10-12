(in-package #:wimelib-sqlite3)

(defparameter *a* (merge-pathnames "doc/amazonia.db"
				   wimelib-sqlite3-config:*BASE-DIRECTORY*))

(defclass tree-data ()
  ((plot-id :column-type t :accessor plot-id :initarg :plot-id)
   (tree-tag-number :primary-key t :reader tree-tag-number
		    :initarg :tree-tag-number)
   (species-id :column-type t :accessor species-id
	       :initarg :species-id)
   (dbh :column-type t :accessor dbh
	:initarg :dbh)
   (height :column-type t :accessor height
	   :initarg :height)
   (researcher :column-type t :accessor researcher
	       :initarg :researcher)
   (date :column-type t :accessor date
	 :initarg :date)
   (notes :column-type t :accessor notes
	  :initarg :notes))
  (:metaclass da-class)
  (:table-name |tree-data|))

(defclass plot-description ()
  ((sample-area-number :column-type t
		      :accessor sample-area-number
		      :initform nil
		      :initarg :sample-area-number)
   (plot-number :primary-key t
		:accessor plot-number
		:initform nil
		:initarg :plot-number)
   (plot-size :column-type t
	     :accessor plot-size
	     :initform nil
	     :initarg :plot-size)
   (land-tenure :column-type t
	       :accessor land-tenure
	       :initform nil
	       :initarg :land-tenure)
   (resource-tenure :column-type t
		    :accessor resource-tenur
		    :initform nil
		    :initarg :resource-tenure)
   (elevation :column-type t
	      :accessor elevation
	      :initform nil
	      :initarg :elevation)
   (aspect :column-type t
	  :accessor aspect
	  :initform nil
	  :initarg :aspect)
   (soil-type :column-type t
	     :accessor soil-type
	     :initform nil
	     :initarg :soil-type)
   (moisture-status :column-type t
		   :accessor moisture-status
		   :initform nil
		   :initarg :moisture-status)
   (date :column-type t
	:accessor date
	:initform nil
	:initarg :date)
   (researcher :column-type t
	      :accessor researcher
	      :initform nil
	      :initarg :researcher)
   (notes :column-type t
	 :accessor notes
	 :initform nil
	 :initarg :notes))
  (:metaclass da-class))

(defparameter *tree*
  (make-instance 'tree-data
		 :tree-tag-number 20000
		 :notes ""
		 :date "today"
		 :researcher "Wilfried"
		 :height 12.5
		 :dbh 5.4
		 :species-id 100
		 :plot-id 2))

(defclass a ()
  ((a-a :column-type t)
   (a-b :column-type t)
   (a-c :primary-key t))
  (:metaclass da-class))

(defclass b ()
  ((b-a :column-type t)
   (a-b)
   (b-c :primary-key t))
  (:metaclass da-class)
  (:table-name b-test))

(defclass c (a b) ()
  (:metaclass da-class))
