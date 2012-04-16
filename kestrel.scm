(module kestrel
 (k-connect k-disconnect k-set k-get k-begin k-commit k-rollback k-delete k-flush k-flush-all k-version k-stats k-shutdown k-reload k-dump-stats k-monitor)
  (import scheme tcp chicken extras srfi-1)
  (require-library tcp extras srfi-1)
;  (use extras tcp)

(define-record-type k-handle
  (make-k-handle host port in out)
  k-handle?
  (host k-handle-host)
  (port k-handle-port)
  (in k-handle-in)
  (out k-handle-out))

(define (k-send h command)
  (fprintf (k-handle-out h) "~A\r\n" command))

(define (k-rcv h)
  (let next-line ((line (read-line (k-handle-in h))) (result '()))
    (if (member line '("END" "STORED" "NOT_STORED" "EXISTS" "NOT_FOUND" "ERROR" "CLIENT_ERROR" "SERVER_ERROR" "DELETED"))
      (reverse (cons line result))
      (next-line (read-line (k-handle-in h)) (cons line result)))))
        
(define (k-connect host port)
  (let-values (((in out) (tcp-connect host port)))
    (make-k-handle host port in out)))

(define (k-disconnect h)
  (close-input-port (k-handle-in h))
  (close-output-port (k-handle-out h)))

(define (k-cmd0 h cmd)
  (let ((c (sprintf "~A" cmd)))
    (k-send h c)
    (k-rcv h)))
(define (k-cmd1 h cmd q)
  (let ((c (sprintf "~A ~A" cmd q)))
    (k-send h c)
    (k-rcv h)))

(define (k-set h q f x v)
  (let ((cmd (sprintf "set ~A ~A ~A ~A" q f x (string-length (sprintf "~A" v)))))
    (k-send h cmd)
    (k-send h v)
    (let ((rc (k-rcv h)))
      (if (string=? (last rc) "STORED")
           v
           #f))))


(define (k-get h q)
  (let ((rc (k-cmd1 h "get" q)))
    (if (= (length rc) 3)
         (second rc)
         #f)))

(define (k-begin h q)
  (k-get h (string-append q "/open")))

(define (k-commit h q)
  (k-get h (string-append q "/close")))

(define (k-rollback h q)
  (k-get h (string-append q "/abort")))

(define (k-delete h q)
  (let ((rc (k-cmd1 h "delete" q)))
    (if (string=? (last rc) "DELETED")
         #t
         #f)))

(define (k-flush h q)
  (let ((rc (k-cmd1 h "flush" q)))
    (if (string=? (last rc) "END")
         #t
         #f)))

(define (k-flush-all h)
  (k-send h "flush_all")
  (let ((rc (read-line (k-handle-in h))))
    (if (string=? rc "Flushed all queues.")
         #t
         #f)))

(define (k-version h)
    (k-send h "version")
    (read-line (k-handle-in h)))

(define (k-stats h)
  (k-cmd0 h "stats"))

(define (k-shutdown h)
  (k-cmd0 h "shutdown"))

(define (k-reload h)
  (k-send h "reload")
  (let ((rc (read-line (k-handle-in h))))
    (if (string=? rc "Reloaded config.")
         #t
         #f)))

(define (k-dump-stats h)
  (let ((rc (k-cmd0 h "dump_stats")))
  (if (string=? (last rc) "END")
    (drop-right rc 1)
    #f)))

(define (k-monitor h q sec callback)
  (k-send h (sprintf "monitor ~A ~A" q sec))
  (let next-line ((line (read-line (k-handle-in h))) (result '()))
    (printf "line:~A~N" line)
    (cond 
      ((and (> (string-length line) 5) (string=? (substring line 0 5) "VALUE"))
        (let* ((rc (k-rcv h)) (res (callback (drop-right rc 1))))
          (printf "next-line: ~A~N" rc)
          (next-line (read-line (k-handle-in h)) (cons res result))))
      ((string=? line "END")
        result))))

)
