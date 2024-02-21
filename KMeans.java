import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {
    public static class Map extends Mapper<Object, Text, Text, Text>{
        private java.util.Map<Integer, String> centroids = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            // read the record line by line
            String line;
            int index = 0;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                centroids.put(index, line);
                index++;
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] accessInfo = value.toString().split(",");
            int x = Integer.parseInt(accessInfo[0]);
            int y = Integer.parseInt(accessInfo[1]);

            double minDistance = Double.MAX_VALUE; // searching for minimum distance between point and centroid
            String[] centroidStringArrInitial = centroids.get(0).split(",");
            // parse the centroid integer values and remove any whitespace from the string
            int closestCentroidX = Integer.parseInt(centroidStringArrInitial[0].replaceAll("\\s", ""));
            int closestCentroidY = Integer.parseInt(centroidStringArrInitial[1].replaceAll("\\s", ""));
            for(int i = 0; i < centroids.size(); i++){
                String[] centroidStringArr = centroids.get(i).split(",");
                int centroidX = Integer.parseInt(centroidStringArr[0].replaceAll("\\s", ""));
                int centroidY = Integer.parseInt(centroidStringArr[1].replaceAll("\\s", ""));
                int distanceX = x - centroidX;
                int distanceY = y - centroidY;
                double distance = Math.pow((Math.pow(distanceX, 2) + Math.pow(distanceY, 2)), 0.5);
                if (distance < minDistance){
                    closestCentroidX = centroidX;
                    closestCentroidY = centroidY;
                    minDistance = distance;
                }
            }
            context.write(new Text( closestCentroidX + ", " + closestCentroidY), new Text(x + ", " + y + "," + 1));
        }
    }

    public static class MapReportClusters extends Mapper<Object, Text, Text, Text>{
        private java.util.Map<Integer, String> centroids = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            // read the record line by line
            String line;
            int index = 0;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                centroids.put(index, line);
                index++;
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] accessInfo = value.toString().split(",");
            int x = Integer.parseInt(accessInfo[0]);
            int y = Integer.parseInt(accessInfo[1]);

            double minDistance = Double.MAX_VALUE; // searching for minimum distance between point and centroid
            String[] centroidStringArrInitial = centroids.get(0).split(",");
            // parse the centroid integer values and remove any whitespace from the string
            int closestCentroidX = Integer.parseInt(centroidStringArrInitial[0].replaceAll("\\s", ""));
            int closestCentroidY = Integer.parseInt(centroidStringArrInitial[1].replaceAll("\\s", ""));
            for(int i = 0; i < centroids.size(); i++){
                String[] centroidStringArr = centroids.get(i).split(",");
                int centroidX = Integer.parseInt(centroidStringArr[0].replaceAll("\\s", ""));
                int centroidY = Integer.parseInt(centroidStringArr[1].replaceAll("\\s", ""));
                int distanceX = x - centroidX;
                int distanceY = y - centroidY;
                double distance = Math.pow((Math.pow(distanceX, 2) + Math.pow(distanceY, 2)), 0.5);
                if (distance < minDistance){
                    closestCentroidX = centroidX;
                    closestCentroidY = centroidY;
                    minDistance = distance;
                }
            }
            context.write(new Text( "Centroid: " + closestCentroidX + ", " + closestCentroidY), new Text("Point: " + x + ", " + y));
        }
    }

    public static class Combiner extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;
            String[] keyString = key.toString().split(",");
            int oldCentroidX = Integer.parseInt(keyString[0].replaceAll("\\s", ""));
            int oldCentroidY = Integer.parseInt(keyString[1].replaceAll("\\s", ""));

            for (Text val : values) {
                String[] valString = val.toString().split(",");
                int pointX = Integer.parseInt(valString[0].replaceAll("\\s", ""));
                int pointY = Integer.parseInt(valString[1].replaceAll("\\s", ""));
                sumX += pointX;
                sumY += pointY;
                count++;
            }
            context.write(new Text( oldCentroidX + ", " + oldCentroidY), new Text(sumX + ", " + sumY + "," + count));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;
            String[] keyString = key.toString().split(",");
            int oldCentroidX = Integer.parseInt(keyString[0].replaceAll("\\s", ""));
            int oldCentroidY = Integer.parseInt(keyString[1].replaceAll("\\s", ""));

            for (Text val : values) {
                String[] valString = val.toString().split(",");
                int pointX = Integer.parseInt(valString[0].replaceAll("\\s", ""));
                int pointY = Integer.parseInt(valString[1].replaceAll("\\s", ""));
                sumX += pointX;
                sumY += pointY;
                count += Integer.parseInt(valString[2].replaceAll("\\s", ""));
            }
            int aveX = sumX / count;
            int aveY = sumY / count;
            System.out.println("New centroid: " + aveX + ", " + aveY + ", Old centroid: " + oldCentroidX + ", " + oldCentroidY);
            // write new and old centroids to output file
            context.write(new Text(aveX + ", " + aveY), new Text(", Old centroid: " + oldCentroidX + ", " + oldCentroidY));
        }
    }

    // new Reducer class used to produce report of all points and corresponding clusters
//    public static class ReduceReportClusters extends Reducer<Text,Text,Text,Text> {
//
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            int sumX = 0;
//            int sumY = 0;
//            int count = 0;
//            for (Text val : values) {
//                System.out.println(val);
//                String[] valString = val.toString().split(",");
//                int pointX = Integer.parseInt(valString[0].replaceAll("\\s", ""));
//                int pointY = Integer.parseInt(valString[1].replaceAll("\\s", ""));
//                sumX += pointX;
//                sumY += pointY;
//                count += Integer.parseInt(valString[2].replaceAll("\\s", ""));
//            }
//            int aveX = sumX / count;
//            int aveY = sumY / count;
//            // write new centroids to output file and all corresponding points
//            while (valuesCopy.hasNext()){
//                Text val = valuesCopy.next();
//                System.out.println("Val 2: " + val);
//                String[] valString = val.toString().split(",");
//                int pointX = Integer.parseInt(valString[0].replaceAll("\\s", ""));
//                int pointY = Integer.parseInt(valString[1].replaceAll("\\s", ""));
//                System.out.println("Final Cluster: " + aveX + ", " + aveY + "; " + "Corresponding Point: " + pointX + ", " + pointY);
//                context.write(new Text("Final Cluster: " + aveX + ", " + aveY), new Text(", Corresponding Point: " + pointX + ", " + pointY));
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {
        // hardcoded variables
        int numCentroids = 3; // assuming we are given number of centroids
        int minPoint = 0; // assuming points cannot have X or Y < 0 (for centroid creation)
        int maxPoint = 5000; // assuming points cannot have X or Y > 5000 (for centroid creation)
        int r = 3; // R value for the number of iterations we are using
        int threshold = 10; // used to determine if difference b/w old & new centroid X & Y is close enough to terminate process
        int k = 1000; // value to generate k points and upload to HDFS
        // if generated = true, then a new file of k points will be created; otherwise points.txt will be used
        boolean generated = true;
        boolean reportCentersAndConvergence = true; // true to report final centroids and whether convergence was achieved
        boolean reportClusters = true; // true to report the final clusters (points and which centroid they belong to)
        // inputPath reads in the points.txt file we created
        Path inputPathPredetermined = new Path("hdfs://localhost:9000/project2/points.txt");
        Path inputPathGenerated = new Path("hdfs://localhost:9000/project2/pointsGenerated.txt");
        // outputPathString is the location of the final output of the centroids
        String outputPathString = "hdfs://localhost:9000/project2/output";
        // outputPathFile is the part-r-00000 file generated for final output
        String outputPathFileString = outputPathString + "/part-r-00000";
        // intermediatePath creates a new centroids.csv file that acts to store the updated
        // centroids in each iteration to be stored in the distributed cache
        String intermediatePathString = "hdfs://localhost:9000/project2/centroids.csv";
        // file to store which points belong to which centroid if reportCluster is true
        String clusterString = "hdfs://localhost:9000/project2/clusters";

        // Paths
        Path intermediatePath = new Path(intermediatePathString);
        Path outputPath = new Path(outputPathString);
        Path outputPathFile = new Path(outputPathFileString);
        Path inputPath = inputPathPredetermined;
        Path clusterPath = new Path(clusterString);
        if(generated){
            inputPath = inputPathGenerated;
        }

        long startTime = System.currentTimeMillis();
        Configuration confIP = new Configuration();

        FileSystem fs = outputPath.getFileSystem(confIP);

        // if input file need to generate points, do so here
        if(generated){
            if(fs.exists(inputPath)){
                fs.delete(inputPath, true);
            }
            FSDataOutputStream inputStream = fs.create(inputPath);
            for(int i = 0; i < k; i++){
                int randX = (int) (Math.random() * maxPoint) + minPoint;
                int randY = (int) (Math.random() * maxPoint) + minPoint;
                inputStream.write(new String(randX + "," + randY + "\n").getBytes(StandardCharsets.UTF_8));
            }
            // close the stream so we can use the new centroids we just created
            inputStream.close();
        }

        // Delete the output directory if it exists
        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true);
        }
        FSDataOutputStream intermediateStream = fs.create(intermediatePath);
        for(int i = 0; i < numCentroids; i++){
            int randX = (int) (Math.random() * maxPoint) + minPoint;
            int randY = (int) (Math.random() * maxPoint) + minPoint;
            intermediateStream.write(new String(randX + "," + randY + "\n").getBytes(StandardCharsets.UTF_8));
        }
        // close the stream so we can use the new centroids we just created
        intermediateStream.close();

        boolean ret = false;
        HashMap newCentroids = new HashMap();
        HashMap oldCentroids = new HashMap();
        boolean terminate = false;
        for(int i = 0; i < r; i++) {
            // use to compare the old and new centroids to the threshold
            oldCentroids = new HashMap<>();
            newCentroids = new HashMap();

            System.out.println("Iteration: " + i);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(Map.class);
            job.setCombinerClass(Combiner.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Configure the DistributedCache
            DistributedCache.addCacheFile(intermediatePath.toUri(), job.getConfiguration());
            DistributedCache.setLocalFiles(job.getConfiguration(), intermediatePathString);

            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            ret = job.waitForCompletion(true);

            if (fs.exists(intermediatePath)) {
                fs.delete(intermediatePath, true);
            }
            intermediateStream = fs.create(intermediatePath);
            FSDataInputStream fis = fs.open(outputPathFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            // read the record line by line
            String line;
            int index = 0;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] lineArr = line.split(", ");
                String newCentroidX = lineArr[0];
                String newCentroidY = lineArr[1];
                String oldCentroidX = lineArr[2].replaceAll("Old centroid:", "").replaceAll("\\s", "");
                String oldCentroidY = lineArr[3].replaceAll("\\s", "");
                intermediateStream.write(new String(newCentroidX + "," + newCentroidY + "\n").getBytes(StandardCharsets.UTF_8));

                // this newly calculated centroid will become the old centroid in the next iteration
                // used to determine if threshold has been met
                oldCentroids.put(index, oldCentroidX + "," + oldCentroidY);
                newCentroids.put(index, newCentroidX + "," + newCentroidY);
                index++;
            }

            // we only calculate centroid values after the first iteration so it is assumed that we are
            // only using the threshold to compare the difference between the values we are calculating
            // assume we are going to terminate unless one of the centroid differences is still greater
            // than threshold; otherwise set i = r (current iteration = max iterations) to terminate
            terminate = false;
            if(i > 0){
                terminate = true;
                for(int j = 0; j < oldCentroids.size(); j++){
                    String[] oldCentroidStringArr = oldCentroids.get(j).toString().split(",");
                    int oldX = Integer.parseInt(oldCentroidStringArr[0].replaceAll("\\s", ""));
                    int oldY = Integer.parseInt(oldCentroidStringArr[1].replaceAll("\\s", ""));
                    String[] newCentroidStringArr = newCentroids.get(j).toString().split(",");
                    int newX = Integer.parseInt(newCentroidStringArr[0].replaceAll("\\s", ""));
                    int newY = Integer.parseInt(newCentroidStringArr[1].replaceAll("\\s", ""));
                    int distanceX = oldX - newX;
                    int distanceY = oldY - newY;
                    double distance = Math.pow((Math.pow(distanceX, 2) + Math.pow(distanceY, 2)), 0.5);
                    if(distance > threshold){
                        terminate = false;
                    }
                }
            }
            if (terminate){
                i = r;
            }

            // close the stream
            intermediateStream.close();
            IOUtils.closeStream(reader);
        }

        if(reportCentersAndConvergence){
            System.out.println();
            System.out.println("Final Report:");
            if(terminate){
                System.out.println("Convergence was achieved");
            }
            else {
                System.out.println("Convergence was not achieved");
            }
            for(int i = 0; i < newCentroids.size(); i++){
                System.out.println(newCentroids.get(i));
            }
        }
        System.out.println();

        // report all final clusters and corresponding points by running map reduce using the last set
        // of centroids found (therefore we are using the values that MapReduce terminated at)
        // and writing them to a newly defined output file, defined as clusters.txt above
        if(reportClusters){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(MapReportClusters.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Configure the DistributedCache
            DistributedCache.addCacheFile(intermediatePath.toUri(), job.getConfiguration());
            DistributedCache.setLocalFiles(job.getConfiguration(), intermediatePathString);

            // if the output file exists, clear it
            if (fs.exists(clusterPath)) {
                fs.delete(clusterPath, true);
            }
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, clusterPath);

            ret = job.waitForCompletion(true);

            // if the centroids file exists, clear it
            if (fs.exists(intermediatePath)) {
                fs.delete(intermediatePath, true);
            }
            intermediateStream = fs.create(intermediatePath);
            FSDataInputStream fis = fs.open(outputPathFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            // close the stream
            intermediateStream.close();
            IOUtils.closeStream(reader);
        }
        fs.close();

        System.out.println();
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }
}
