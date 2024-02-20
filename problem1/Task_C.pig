/* Task C */
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);

grouped_pages = GROUP pages BY CountryCode;
results = FOREACH grouped_pages GENERATE group, COUNT(pages);
Dump results;
