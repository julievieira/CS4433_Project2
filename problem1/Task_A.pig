/* Task A */
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);

americans = FILTER pages BY Nationality == 'United States';
results = FOREACH americans GENERATE Name,Hobby;
Dump results;