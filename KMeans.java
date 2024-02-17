import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class KMeans {

    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] accessInfo = value.toString().split(",");
            String x = accessInfo[1];
            String y = accessInfo[2];

            /*
            Compare the points to the centroids and determine which one it's closest to
             */

            if(!x.contains("X")) {
                context.write(new Text(), new Text(x + ", " + y));
            }
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                /*
                Find the average between all of the points sent to this reducer,
                meaning they all are connected to the same centroid
                 */
            }
            context.write(new Text(newCentroidX + ", " + newCentroidY), new Text(oldCentroidX + ", " + oldCentroidY));
        }
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }
}
