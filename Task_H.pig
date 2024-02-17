/* Task H */
/* Locations of input files are hardcoded in the below 2 lines */
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int,Name:chararray,Nationality:chararray,CountryCode:int,Hobby:chararray);
friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel:int,PersonID:int,MyFriend:int,DateOfFriendship:chararray,Desc:chararray);

person = GROUP friends BY PersonID;
friendship_count = FOREACH person GENERATE group, COUNT(friends) as count;
pages_people = GROUP pages ALL;
total_people = FOREACH pages_people GENERATE COUNT(pages) AS countP;
friendships = GROUP friends ALL;
total_friendships = FOREACH friendships GENERATE COUNT(friends) AS countF;
totals = CROSS total_people, total_friendships;
average = FOREACH totals GENERATE countF/countP as ave;
find_popular = CROSS friendship_count, average;
popular = FILTER find_popular BY (count > ave);
results = FOREACH popular GENERATE group;
STORE results INTO 'output_file_TaskH' USING PigStorage(',');
Dump results;