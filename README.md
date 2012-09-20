# wimelib-sqlite3

wimelib-sqlite3 is my minimal CFFI binding to the sqlite3 library.

# Example

A very simple session:

    > (defvar *db*)
    *db*

    > (setf *db* (sqlite3-open "test.db"))
    #.(SB-SYS:INT-SAP #X006007C0)

    > (sqlite3-exec *db* "create table foo(name text, value integer);")
    NIL

    > (sqlite3-exec *db* "insert into foo(name, value) values ('Linda', 1);")
    NIL

    > (sqlite3-exec *db* "insert into foo(name, value) values ('John', 53);")
    NIL

    > (sqlite3-exec *db* "insert into foo(name, value) values ('Tina', 42);")
    NIL

    > (sqlite3-exec *db* "select name, value from foo;"
        (lambda (stmt)
          (format t "~&~A: ~A~%"
                  (wimelib-sqlite3:sqlite3-column-text stmt 0)
                  (wimelib-sqlite3:sqlite3-column-text stmt 1))))
    Linda: 1
    John: 53
    Tina: 42
    NIL

    > (sqlite3-close *db*)
    NIL