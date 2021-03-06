-- TODO: load the input dataset, located in ./local-input/OSN/tw.txt
A = LOAD 'tw.txt' AS (id: long, fr: long);
B = LOAD 'tw.txt' AS (id: long, fr: long);

SPLIT A INTO good_A IF id is not null and fr is not null, bad_A OTHERWISE;
SPLIT B INTO good_B IF id is not null and fr is not null, bad_B OTHERWISE;

-- TODO: compute all the two-hop paths 
twohop = JOIN good_A by $1, good_B by $0;

-- TODO: project the twohop relation such that in output you display only the start and end nodes of the two hop path
p_result = FOREACH twohop GENERATE $0, $3;

-- TODO: make sure you avoid loops (e.g., if user 12 and 13 follow eachother) 
result = DISTINCT p_result;

STORE result INTO 'join.output';

