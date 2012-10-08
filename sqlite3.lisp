(in-package #:wimelib-sqlite3)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (enable-embed-reader-syntax)
  (enable-column-reader-syntax)
  (enable-bind-reader-syntax))
