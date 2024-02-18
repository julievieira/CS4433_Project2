import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

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
            int closestCentroidX = Integer.parseInt(centroidStringArrInitial[0]);
            int closestCentroidY = Integer.parseInt(centroidStringArrInitial[1]);
            for(int i = 0; i < centroids.size(); i++){
                String[] centroidStringArr = centroids.get(i).split(",");
                int centroidX = Integer.parseInt(centroidStringArr[0]);
                int centroidY = Integer.parseInt(centroidStringArr[1]);
                int distanceX = x - centroidX;
                int distanceY = y - centroidY;
                double distance = Math.pow((Math.pow(distanceX, 2) + Math.pow(distanceY, 2)), 0.5);
                if (distance < minDistance){
                    closestCentroidX = centroidX;
                    closestCentroidY = centroidY;
                    minDistance = distance;
                }
            }
            context.write(new Text( closestCentroidX + ", " + closestCentroidY), new Text(x + ", " + y));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        private java.util.Map<Integer, String> newCentroids = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;
            String[] keyString = key.toString().split(", ");
            int oldCentroidX = Integer.parseInt(keyString[0]);
            int oldCentroidY = Integer.parseInt(keyString[1]);

            for (Text val : values) {
                String[] valString = val.toString().split(", ");
                int pointX = Integer.parseInt(valString[0]);
                int pointY = Integer.parseInt(valString[1]);
                sumX += pointX;
                sumY += pointY;
                count++;
            }
            int aveX = sumX / count;
            int aveY = sumY / count;
            newCentroids.put(0, aveX + ", " + aveY);

            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(path)) {
                fs.delete(path, true); // true will delete recursively
            }
            FSDataOutputStream intermediateStream = fs.create(path);
            intermediateStream.write(new String(newCentroids.get(0) + "\n").getBytes(StandardCharsets.UTF_8));
            intermediateStream.close();

            // write new and old centroids to output file
            context.write(new Text(aveX + ", " + aveY), new Text(", Old centroid: " + oldCentroidX + ", " + oldCentroidY));
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Delete the output directory if it exists
        Path outputPath = new Path("hdfs://localhost:9000/project2/output");
        Path intermediatePath = new Path("hdfs://localhost:9000/project2/centroids.csv");
        FileSystem fs = outputPath.getFileSystem(conf);

        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true); // true will delete recursively
        }
        FSDataOutputStream intermediateStream = fs.create(intermediatePath);
        int numCentroids = 3;
        for(int i = 0; i < numCentroids; i++){
            int randX = (int) (Math.random() * 5000);
            int randY = (int) (Math.random() * 5000);
            intermediateStream.write(new String(randX + "," + randY + "\n").getBytes(StandardCharsets.UTF_8));
        }
        // close the stream so we can use the new centroids we just created
        intermediateStream.close();

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path("hdfs://localhost:9000/project2/centroids.csv").toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), "hdfs://localhost:9000/project2/centroids.csv");

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project2/points.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/output"));

        boolean ret = job.waitForCompletion(true);

        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true); // true will delete recursively
        }
        intermediateStream = fs.create(intermediatePath);
        FSDataInputStream fis = fs.open(new Path("hdfs://localhost:9000/project2/output/part-r-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        // read the record line by line
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] lineArr = line.split(", ");
            String newCentroidX = lineArr[0];
            String newCentroidY = lineArr[1];
            intermediateStream.write(new String(newCentroidX + "," + newCentroidY + "\n").getBytes(StandardCharsets.UTF_8));
        }
        // close the stream
        intermediateStream.close();
        IOUtils.closeStream(reader);
        fs.close();


        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }
}
