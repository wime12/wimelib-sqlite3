(in-package #:wimelib-sqlite3-ffi)

(define-foreign-library libsqlite3
  (:windows "sqlite3.dll")
  (:darwin "libsqlite3.dylib")
  (:unix "libsqlite3.so.0"))

(use-foreign-library libsqlite3)
