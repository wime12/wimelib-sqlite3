;;;; package.lisp

(defpackage #:wimelib-sqlite3
  (:use #:cl #:alexandria #:wimelib-sql #:closer-mop #:wimelib-sqlite3-ffi)
  (:shadowing-import-from #:closer-mop
			  #:standard-method
			  #:standard-generic-function
			  #:defmethod
			  #:defgeneric
			  #:standard-class)
  (:export
   ssql
   ssql*
   open-db
   close-db
   with-db
   with-open-db
   column-value
   column-values
   column-names
   exec
   do-rows
   do-query
   map-query
   query
   begin-transaction
   commit-transaction
   savepoint
   release-savepoint
   rollback
   with-transaction
   with-savepoint
   list-tables
   table-exists-p
   list-indices
   attribute-list
   attribute-name
   attribute-type
   defprepared
   prepare
   prepared-results
   enable-bind-reader-syntax
   disable-bind-reader-syntax
   da-class
   table-name
   insert-da
   update-record
   refresh-da
   delete-da
   get-da
   select-das))

