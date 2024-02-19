/* Task B */
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);
access_logs = LOAD 'access_logs.csv' USING PigStorage(',') as (AccessID:int,ByWho:int,WhatPage:int,TypeOfAccount:chararray,AccessTime:chararray);

all_pages = GROUP access_logs BY WhatPage;
all_pages_count = FOREACH all_pages GENERATE group AS WhatPage, COUNT(access_logs) AS num_access_logs;
ordered_pages = ORDER all_pages_count BY num_access_logs DESC;
top_ten_pages = LIMIT ordered_pages 10;
top_ten_with_info = JOIN top_ten_pages BY WhatPage, pages BY PersonID;
results = FOREACH top_ten_with_info GENERATE WhatPage, Name, Nationality, num_access_logs;
Dump results;
