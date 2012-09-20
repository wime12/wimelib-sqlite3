;;;; wimelib-sqlite3.asd

(asdf:defsystem #:wimelib-sqlite3
  :serial t
  :description "Describe wimelib-sqlite3 here"
  :author "Wilfried Meindl <wilfried.meindl@gmail.com>"
  :license "Specify license here"
  :depends-on (#:alexandria
               #:cffi
	       #:wimelib-sql)
  :components ((:file "package")
               (:file "ffi")
	       (:file "sql")
	       (:file "sqlite3")))

