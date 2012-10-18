(in-package #:wimelib-sqlite3)

(defmacro with-collectors ((&rest collectors) &body body)
  (flet ((gen-names (len &optional (string-or-integer ""))
	   (do ((i 0 (1+ i))
		(names nil (cons (gensym string-or-integer) names)))
	       ((>= i len) names))))
    (let* ((len (length collectors))
	   (heads (gen-names len "HEAD"))
	   (tails (gen-names len "TAIL")))
      `(let* (,@(mapcar (lambda (head) `(,head (list nil))) heads)
	      ,@(mapcar (lambda (head tail) `(,tail ,head)) heads tails))
	 (flet (,@(mapcar
		   (lambda (collector tail)
		     `(,collector (x)
				  (setf (cdr ,tail) (setf ,tail (list x)))))
		   collectors tails))
	   ,@body)
	 (values ,@(mapcar (lambda (head) `(cdr ,head)) heads))))))

(defmacro collecting (&body body)
  `(with-collectors (collect) ,@body))
