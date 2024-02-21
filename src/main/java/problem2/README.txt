Running tasks

Generate Points
hadoop/bin/hadoop jar shared_folder/Project2/part2/KMeans.jar /project2/input_data/ somedata.txt 1000

Task A
hadoop/bin/hadoop jar shared_folder/Project2/part2/KMeans.jar /project2/input_data/points.txt /project2/input_data/initialSeeds.txt /project2/output/taskA/

Task B
hadoop/bin/hadoop jar shared_folder/Project2/part2/KMeans.jar /project2/input_data/points.txt /project2/input_data/initialSeeds.txt /project2/output/taskB/ 3

Task C
hadoop/bin/hadoop jar shared_folder/Project2/part2/KMeans.jar /project2/input_data/points.txt /project2/input_data/initialSeeds.txt /project2/output/taskC/ 30

Task D
hadoop/bin/hadoop jar shared_folder/Project2/part2/KMeans.jar /project2/input_data/points.txt somedata.txt 5