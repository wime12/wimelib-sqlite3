(in-package #:wimelib-sqlite3)

(open-db "amazonia.db")

;;; Queries

(query (:select [sample-area-location land-use-stage]
		:from sample-areas
		:where (:= sample-area-number 1)))

(query (:select (:count :*) :from species-list
		:where (:like scientific-name "%sp.")))

(query (:select [local-name scientific-name] :from species-list
		:where (:<= species-id 5)))

(defun julianday->universal-time (julian)
  (round (* 86400d0 (- julian 2415020.5d0))))

(defun universal-time->julianday (universal)
  (+ (/ universal 86400d0) 2415020.5d0))

(multiple-value-list
 (decode-universal-time
  (julianday->universal-time
   (caar (query (:select (:max (:julianday date)) :from tree-data))))))

;; Updates

(with-transaction
  (exec (:insert :into species-list (local-name) :values ("Aardvark")))
  (exec (:create :table foo ((bar :integer)))))

#+nil(exec (:drop :table foo))
#+nil(end-transaction)

(begin-transaction)

(query (:select :* :from species-list :where (:= local-name "Aardvark")))

(exec (:delete :from species-list :where (:= local-name "Aardvark")))

(query (:select :* :from species-list :where (:= local-name "Aardvark")))

(rollback)

(query (:select :* :from species-list :where (:= local-name "Aardvark")))

;; Meta Queries

(list-tables)

(table-exists-p 'species-list)

(list-attributes 'species-list)

(attribute-type 'scientific-name 'species-list)

(loop
   for attr in (list-attributes 'tree-data)
   collect (attribute-type attr 'tree-data))

(query (:select :* :from species-list
		:where (:between species-id 6 9)))

(ssql* `(:and
	   ,@(loop
		for table in '(thistime nexttime sometime never)
		for count from 42
		collect `(:between (:dot ,table bar) (:* hip hop) ,count)
		collect `(:like (:dot ,table baz) ,(symbol-name table)))))

(let ((sexp `(:and
		,@(loop
		     for table in '(thistime nexttime sometime never)
		     for count from 42
		     collect `(:between (:dot ,table bar) (:* hip hop) ,count)
		     collect `(:like (:dot ,table baz) ,(symbol-name table))))))
    `(ssql ,sexp))

(query (:select [field-type researcher] :from sample-areas))

;; Arithmetic operators

(query (:select (:+ plot-number (:* 1000 sample-area-number))
		:from plot-description)
       :flatp t)

(query (:select :* :from species-list))

;; Aggregates

(query (:select (:max (:+ plot-number (:* 1000 sample-area-number)))
		:from plot-description)
       :flatp t)

(query (:select (:avg (:+ plot-number (:* 1000 sample-area-number)))
		:from plot-description)
       :flatp t)

(loop
   :for table :in (list-tables) :repeat 5
   :collect (query (:select [@table (:count :*)] :from @table)))

(loop
   :for column in '(:* scientific-name)
   :collect (query (:select (:count @column) :from species-list)
		   :flatp t))

;; Comparisons

(query (:select land-use-stage :from sample-areas) :flatp t)

(query
 (:select land-use-stage :from sample-areas :where (:>= sample-area-number 37))
 :flatp t)

(query (:select :distinct land-use-stage :from sample-areas)
       :flatp t)

(query (:select [land-use-stage (:count :*)]
		:from sample-areas
		:group :by land-use-stage))

(query (:select [land-use-stage (:count :*)]
		:from sample-areas
		:group :by land-use-stage
		:having (:between (:count :*) 8 10)))

(query (:select (:max height) :from tree-data :where (:= researcher "Fernando"))
       :flatp t)

;; Strings: :LIKE

(query (:select scientific-name :from species-list
		:where (:like local-name "v%"))
       :flatp t)

;; :ISNULL

(query (:select local-name :from species-list :where scientific-name :isnull))

;; Removing duplicates: :DISTINCT

(query (:select :distinct researcher :from tree-data)
       :flatp t)

;; Logical: :AND :OR :NOT

(query (:select :distinct researcher :from [tree-data species-list]
		:where (:and (:= tree-data.species-id species-list.species-id)
			     (:like local-name "v%")))
       :flatp t)

(query (:select [table.local-name table.scientific-name]
		:from [(species-list :as table) (species-list :as join)]
		:where (:and (:= table.scientific-name join.scientific-name)
			     (:not (:= table.species-id join.species-id)))
		:order :by table.scientific-name))

;; Subselects

(query (:select scientific-name :from species-list
		:where local-name :in ((:select local-name
						:from species-list
						:where (:like local-name "v%"))))
       :flatp t)

(query (:select :distinct researcher :from [tree-data species-list]
		:where (:and (:= tree-data.species-id species-list.species-id)
			     ((local-name :in ((:select local-name
							:from species-list
							:where (:like local-name
								      "v%")))))))
       :flatp t)

(query (:select demo-site :from sample-areas
		:where (:<= date
			    ((:select :all date :from species-data)))
		:group :by demo-site)
       :flatp t)

(query (:select species-id :from species-data
		:where :not :exists ((:select :* :from sample-areas
					     :where (:= species-data.date
							sample-areas.date)))))

;; Updates

(defvar aardvark '(:= local-name "Aardvark"))

(values (query (:select :* :from species-list :where @aardvark)))

(with-transaction
  (exec (:insert :into species-list (local-name)
		 :values ("Aardvark"))))

(values (query (:select :* :from species-list :where @aardvark)))

(with-transaction
  (exec (:update species-list :set scientific-name := "Orycteropus afer"
		 :where @aardvark)))

(values (query (:select :* :from species-list :where @aardvark)))

(with-transaction
  (exec (:delete :from species-list :where @aardvark)))

(values (query (:select :* :from species-list :where @aardvark)))

;; Iteration

(map-query 'vector 'print (:select :* :from species-list :where @aardvark))

(do-query (local scientific id)
    (:select :* :from species-list :where @aardvark)
  (print (list local scientific id)))

