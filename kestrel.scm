(module kestrel
 (k-connect k-disconnect k-set k-get k-begin k-commit k-rollback k-delete k-flush k-flush-all k-version k-stats k-shutdown k-reload k-dump-config k-dump-stats)
  (import scheme tcp chicken extras)
  (require-library tcp extras)
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
    (printf "~A~N" c)
    (k-send h c)
    (k-rcv h)))
(define (k-cmd1 h cmd q)
  (let ((c (sprintf "~A ~A" cmd q)))
    (printf "~A~N" c)
    (k-send h c)
    (k-rcv h)))

(define (k-set h q f x v)
  (let ((cmd (sprintf "set ~A ~A ~A ~A" q f x (string-length (sprintf "~A" v)))))
    (printf "~A~N" cmd)
    (k-send h cmd)
    (k-send h v)
    (k-rcv h)))

(define (k-get h q)
  (k-cmd1 h "get" q))

(define (k-begin h q)
  (k-cmd1 h "get" (string-append q "/open")))

(define (k-commit h q)
  (k-cmd1 h "get" (string-append q "/close")))

(define (k-rollback h q)
  (k-cmd1 h "get" (string-append q "/abort")))

(define (k-delete h q)
  (k-cmd1 h "delete" q))

(define (k-flush h q)
  (k-cmd1 h "flush" q))

(define (k-flush-all h)
  (k-send h "flush_all")
  (list (read-line (k-handle-in h))))

(define (k-version h)
    (k-send h "version")
    (list (read-line (k-handle-in h))))

(define (k-stats h)
  (k-cmd0 h "stats"))

(define (k-shutdown h)
  (k-cmd0 h "shutdown"))

(define (k-reload h)
  (k-send h "reload")
  (list (read-line (k-handle-in h))))

(define (k-dump-config h)
  (k-cmd0 h "dump_config"))

(define (k-dump-stats h)
  (k-cmd0 h "dump_stats"))

(define (k-monitor h q sec callback)
  (k-send h (sprintf "monitor ~A ~A" q sec))
  (let next-line ((line (read-line (k-handle-in h))) (result '()))
    (cond 
      ((string=? (substring line 0 4) "VALUE")
        (callback (read-line (k-handle-in h))) 
        (read-line (k-handle-in h))
        (next-line (read-line (k-handle-in h)) (cons line result)))
      ((string=? line "END")
        result))))

)
