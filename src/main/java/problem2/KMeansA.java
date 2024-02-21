package problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class KMeansA {

    public static void main(String[] args) throws Exception {
        String points_inputPath = args[0];
        String seeds = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        FileInputFormat.addInputPath(job, new Path(points_inputPath));
        job.addCacheFile(new URI(seeds));
        job.setMapperClass(KMeans.KMeansMapper.class);
        job.setReducerClass(KMeans.KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);

        // final output job
        Job job1 = Job.getInstance(conf, "KMeansFinal");
        FileInputFormat.addInputPath(job1, new Path(points_inputPath));
        System.out.println(args[2] + "part-r-00000");
        job1.addCacheFile(new URI(args[2] + "part-r-00000"));
        job1.setMapperClass(KMeans.KMeansMapper.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        System.out.println(outputPath + "final_output");
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "final_output"));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
