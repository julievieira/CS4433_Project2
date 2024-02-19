/* 
Task E
Load page access data hardcoded with fields: LogID, UserID, PageID */
accesses = LOAD 'access_logs.csv' USING PigStorage(',') AS (logID:int, userID:int, pageID:int);
/* Load user data with fields: UserID, UserName */
users = LOAD 'pages.csv' USING PigStorage(',') AS (userID:int, userName:chararray);
distinctAccesses = DISTINCT accesses;
groupedAccesses = GROUP accesses BY userID;
groupedDistinctAccesses = GROUP distinctAccesses BY userID;
totalCount = FOREACH groupedAccesses GENERATE group AS userID, COUNT(accesses) AS totalAccesses;
distinctCount = FOREACH groupedDistinctAccesses GENERATE group AS userID, COUNT(distinctAccesses) AS distinctAccesses;
countsJoined = JOIN totalCount BY userID, distinctCount BY userID;
finalOutput = JOIN countsJoined BY totalCount::userID, users BY userID;
/* Ensure that field aliases are used correctly after JOIN operations */
result = FOREACH finalOutput GENERATE users::userName AS userName, 
                                        countsJoined::totalCount::totalAccesses AS total, 
                                        countsJoined::distinctCount::distinctAccesses AS distinctAccess;

STORE result INTO 'output/userPageAccessCounts' USING PigStorage(',');
Dump result;