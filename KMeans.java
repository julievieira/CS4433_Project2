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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
//        private java.util.Map<Integer, String> centroids = new HashMap<>();
        private int total = 0;
        private int num = 0;
        private java.util.Map<Integer, String> centroids = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            URI[] cacheFiles = context.getCacheFiles();
//            Path path = new Path(cacheFiles[0]);
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            // wrap it into a BufferedReader object which is easy to read a record
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
//            context.write(new Text(x + ", " + y), new Text(centroids.get(0)));
        }
    }

//    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
//        private Map<String, IntWritable> accessPairs = new HashMap<>();
//        private Map<String, IntWritable> distinct = new HashMap<>();
//        private Map<String, IntWritable> total = new HashMap<>();
//        private IntWritable resultPairs = new IntWritable();
//        private HashMap visited = new HashMap<>();
//
//        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int distinct = 0;
//            int sum = 0;
//            String[] keyString = key.toString().split(",");
//            String byWho = keyString[0];
//            for (IntWritable val : values) {
//                sum += val.get();
//                if (!accessPairs.containsKey(key)) {
//                    distinct += val.get();
//                    accessPairs.put(key.toString(), new IntWritable(distinct));
//                }
//                total.put(key.toString(), new IntWritable(sum));
//            }
////            resultPairs.set(distinct);
////            context.write(new Text(byWho), resultPairs);
//        }
//
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            for(Map.Entry<String, IntWritable> itr : accessPairs.entrySet()) {
//                // account number
//                String key = itr.getKey().toString().split(",")[0];
//                // handle the total number of accesses
//                if (!total.containsKey(key)) {
//                    total.put(key, itr.getValue());
//                } else {
//                    IntWritable currentVal = total.get(key);
//                    IntWritable addVal = itr.getValue();
//                    IntWritable newVal = new IntWritable(currentVal.get() + addVal.get());
//                    total.replace(key, newVal);
//                }
//                // handle the number of distinct accesses
//                if (!distinct.containsKey(key)) {
//                    distinct.put(key, itr.getValue());
//                } else {
//                    IntWritable currentVal = distinct.get(key);
//                    IntWritable addVal = new IntWritable(1);
//                    IntWritable newVal = new IntWritable(currentVal.get() + addVal.get());
//                    distinct.replace(key, newVal);
//                }
//            }
//            for(Map.Entry<String, IntWritable> itr : accessPairs.entrySet()){
//                String key = itr.getKey().toString().split(",")[0];
//                if(!visited.containsKey(key)){
//                    context.write(new Text("Person ID: " + key + ", Total Accesses: " + total.get(key) + ", # Distinct Accesses: "), distinct.get(key));
//                    visited.put(key, 1);
//                }
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReducer.class);
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
        int r = 3;
        for(int i = 0; i < r; i++){
            int randX = (int) (Math.random() * 5000);
            int randY = (int) (Math.random() * 5000);
            intermediateStream.write(new String(randX + "," + randY + "\n").getBytes(StandardCharsets.UTF_8));
        }
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

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }
}
