(in-package #:wimelib-sqlite3)

(defvar *a* "/Users/wilfried/amazonia.db")

(defclass tree-data ()
  ((plot-id :column-type t :accessor plot-id :initarg :plot-id)
   (tree-tag-number :primary-key t :reader tree-tag-number
		    :initarg :tree-tag-number)
   (species-id :column-type t :accessor species-id
	       :initarg :species-id)
   (dbh :column-type t :accessor dbh
	:initarg :dbh)
   (height :column-type t :accessor height
	   :initarg :heigth)
   (researcher :column-type t :accessor researcher
	       :initarg :researcher)
   (date :column-type t :accessor date
	 :initarg :date)
   (notes :column-type t :accessor notes
	  :initarg :notes))
  (:metaclass da-class))

(in-package #:postmodern)

(defclass country ()
  ((name :col-type string :initarg :name
         :reader country-name)
   (inhabitants :col-type integer :initarg :inhabitants
                :accessor country-inhabitants)
   (sovereign :col-type (or db-null string) :initarg :sovereign
              :accessor country-sovereign))
  (:metaclass dao-class)
  (:keys name))
