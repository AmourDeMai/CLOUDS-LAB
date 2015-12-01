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
    scode: chararray, dcode: chararray, disct: int,
    tintime: int, touttime: int,
    cancel: chararray, cancelcode: chararray, diverted: int,
    cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

RAW_DATA = UNION RAW_DATA8, RAW_DATA7, RAW_DATA6, RAW_DATA5;

G_src = GROUP RAW_DATA BY scode;
G_src_count = FOREACH G_src GENERATE group as IATA, COUNT(RAW_DATA) AS count_flights;

G_dest = GROUP RAW_DATA BY dcode;
G_dest_count = FOREACH G_dest GENERATE group as IATA, COUNT(RAW_DATA) AS count_flights;

G_total = JOIN G_src_count BY $0 FULL OUTER, G_dest_count by $0;
G_total_count = FOREACH G_total GENERATE $0 AS IATA, $1+$3 AS total_numb_flights;
G_total_sort = ORDER G_total_count by total_numb_flights DESC;

STORE G_src_count INTO AIR_Ex1_src;
STORE G_dest_count INTO AIR_Ex1_dest;
STORE G_total_sort INTO AIR_Ex1_tot;
