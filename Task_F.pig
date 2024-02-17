/* Task F */
/* Locations of input files are hardcoded in the below 3 lines */
access_logs = LOAD 'access_logs.csv' USING PigStorage(',') as (AccessID:int,ByWho:int,WhatPage:int,TypeOfAccount:chararray,AccessTime:chararray);
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);
friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel:int,PersonID:int,MyFriend:int,DateOfFriendship:chararray,Desc:chararray);

friends_join_access_logs = JOIN friends BY (PersonID, MyFriend) LEFT OUTER, access_logs BY (ByWho, WhatPage);
no_access = FILTER friends_join_access_logs BY access_logs::ByWho is null;
no_access_distinct = GROUP no_access BY PersonID;
no_access_IDs = FOREACH no_access_distinct GENERATE group;
name_ID = FOREACH pages GENERATE PersonID as PID, Name;
results_join = JOIN no_access_IDs BY group, name_ID BY PID;
results = FOREACH results_join GENERATE group, Name;
STORE results INTO 'output_file_TaskF' USING PigStorage(',');
Dump results;