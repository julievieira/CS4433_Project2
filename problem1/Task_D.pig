/* 
Task D
 Load friendship data with fields: FriendRel, PersonID, MyFriend, DateofFriendship, Desc */
friendships = LOAD 'friends.csv' USING PigStorage(',') AS (friendRel:int, personID:int, myFriend:int, dateOfFriendship:int, desc:chararray);
/* Load user data with fields: UserID, UserName */
users = LOAD 'pages.csv' USING PigStorage(',') AS (userID:int, userName:chararray);
groupedFriends = GROUP friendships BY myFriend;
friendsCount = FOREACH groupedFriends GENERATE group AS userID, COUNT(friendships) AS friendCount;
/* Handling users with no friends :(, using a LEFT JOIN */
joinedData = JOIN users BY userID LEFT, friendsCount BY userID;
finalOutput = FOREACH joinedData GENERATE users::userName AS userName, 
                                        (friendsCount::friendCount IS NULL ? 0 : friendsCount::friendCount) AS friendCount;
STORE finalOutput INTO 'output/connectednessFactor' USING PigStorage(',');
Dump finalOutput