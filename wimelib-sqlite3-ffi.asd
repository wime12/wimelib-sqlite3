;;;; wimelib-sqlite3.asd

(asdf:defsystem #:wimelib-sqlite3-ffi
  :serial t
  :description "Describe wimelib-sqlite3 here"
  :author "Wilfried Meindl <wilfried.meindl@gmail.com>"
  :license "Specify license here"
  :depends-on (#:cffi)
  :components ((:file "package-ffi")
               (:file "ffi")))
