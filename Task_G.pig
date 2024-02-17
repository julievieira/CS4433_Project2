/* Task G */
/* This task was tested using the date 2022-02-16 and the current date because the data set does not contain any recent accesses */
/* Locations of input files are hardcoded in the below 2 lines */
access_logs = LOAD 'access_logs.csv' USING PigStorage(',') as (AccessID:int,ByWho:int,WhatPage:int,TypeOfAccount:chararray,AccessTime:chararray);
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);
date_ID = FOREACH access_logs GENERATE ByWho, AccessTime;
recent_access = FILTER date_ID BY (AccessTime >= '2024-02-17 00:00:00');
recent_access_IDs = GROUP recent_access BY ByWho;
pages_access = JOIN pages BY PersonID LEFT OUTER, recent_access_IDs BY group;
no_access = FILTER pages_access BY recent_access_IDs::group is null;
results = FOREACH no_access GENERATE PersonID, Name;
STORE results INTO 'output_file_TaskG_2022-02-16' USING PigStorage(',');
Dump results;