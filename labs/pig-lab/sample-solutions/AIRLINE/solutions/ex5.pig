RAW_DATA5 = LOAD '/laboratory/airlines/2005.csv' USING PigStorage(',') AS
    (year: int, month: int, day: int, dow: int,
    dtime: int, sdtime: int, arrtime: int, satime: int,
    carrier: chararray, fn: int, tn: chararray,
    etime: int, setime: int, airtime: int,
    adelay: int, ddelay: int,
    scode: chararray, dcode: chararray, dist: int,
    tintime: int, touttime: int,
    cancel: chararray, cancelcode: chararray, diverted: int,
    cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

RAW_DATA6 = LOAD '/laboratory/airlines/2006.csv' USING PigStorage(',') AS
    (year: int, month: int, day: int, dow: int,
    dtime: int, sdtime: int, arrtime: int, satime: int,
    carrier: chararray, fn: int, tn: chararray,
    etime: int, setime: int, airtime: int,
    adelay: int, ddelay: int,
    scode: chararray, dcode: chararray, dist: int,
    tintime: int, touttime: int,
    cancel: chararray, cancelcode: chararray, diverted: int,
    cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

RAW_DATA7 = LOAD '/laboratory/airlines/2007.csv' USING PigStorage(',') AS
    (year: int, month: int, day: int, dow: int,
    dtime: int, sdtime: int, arrtime: int, satime: int,
    carrier: chararray, fn: int, tn: chararray,
    etime: int, setime: int, airtime: int,
    adelay: int, ddelay: int,
    scode: chararray, dcode: chararray, dist: int,
    tintime: int, touttime: int,
    cancel: chararray, cancelcode: chararray, diverted: int,
    cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);
RAW_DATA8 = LOAD '/laboratory/airlines/2008.csv' USING PigStorage(',') AS
    (year: int, month: int, day: int, dow: int,
    dtime: int, sdtime: int, arrtime: int, satime: int,
    carrier: chararray, fn: int, tn: chararray,
    etime: int, setime: int, airtime: int,
    adelay: int, ddelay: int,
    scode: chararray, dcode: chararray, dist: int,
    tintime: int, touttime: int,
    cancel: chararray, cancelcode: chararray, diverted: int,
    cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

RAW_DATA = UNION RAW_DATA8, RAW_DATA7, RAW_DATA6, RAW_DATA5;

DATA = FOREACH RAW_DATA GENERATE carrier, day AS d, dow AS dow, month AS m, (int)(arrtime-satime) AS delay;
A = FOREACH RAW_DATA GENERATE scode AS s, dcode AS d;
B = GROUP A by (s,d);
COUNT = FOREACH B GENERATE group, COUNT(A) as count_r;
sorted= ORDER COUNT by count_r ASC;
