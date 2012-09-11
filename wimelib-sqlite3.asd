;;;; wimelib-sqlite3.asd

(asdf:defsystem #:wimelib-sqlite3
  :serial t
  :description "Describe wimelib-sqlite3 here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:alexandria
               #:cffi
	       #:wimelib-sql-sqlite)
  :components ((:file "package")
               (:file "wimelib-sqlite3-ffi")
	       (:file "wimelib-sqlite3")))

