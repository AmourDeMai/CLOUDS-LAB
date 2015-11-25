dataset = LOAD '/laboratory/twitter-small.txt' AS (id: long, fr: long);

-- check if user IDs are valid (e.g. not null) and clean the dataset
SPLIT dataset INTO good_dataset IF id is not null and fr is not null, bad_dataset OTHERWISE;

-- organize data such that each node ID is associated to a list of neighbors
nodes = GROUP good_dataset BY id; 

-- foreach node ID generate an output relation consisting of the node ID and the number of "friends"
friends = FOREACH nodes GENERATE group, COUNT($1) AS followers;

-- count the following

nodes2 = GROUP good_dataset BY fr;
followings = FOREACH nodes2 GENERATE group, COUNT($1);

outliers = FILTER friends BY followers<3;

STORE friends INTO '/user/group001/pig/twc/pig-twitter-small-twc';
STORE followings INTO '/user/group001/pig/following/pig-twitter-small-following';
STORE outliers INTO '/user/group001/pig/outliers/pig-twitter-small-outliers';
