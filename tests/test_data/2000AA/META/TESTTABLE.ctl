options (direct=true)
load data
characterset UTF8 length semantics char
infile 'TESTTABLE.RRF'
badfile 'TESTTABLE.bad'
discardfile 'TESTTABLE.dsc'
truncate
into table TESTTABLE
fields terminated by '|'
trailing nullcols
(TTY	char(10),
CODE	char(8)
)