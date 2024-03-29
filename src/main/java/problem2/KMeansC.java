package problem2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class KMeansC {

    private static double distanceBetween(String x_1, String y_1, String x_2, String y_2) {
        double x1 = Double.parseDouble(x_1);
        double y1 = Double.parseDouble(y_1);
        double x2 = Double.parseDouble(x_2);
        double y2 = Double.parseDouble(y_2);
        return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }

    private static boolean convergence(String currentCentroidsFile, int threshold, Configuration conf) throws IOException {
        Path path = new Path(currentCentroidsFile);

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis = fs.open(path);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        ArrayList<Integer> allConverge = new ArrayList<>();
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {

            String[] generalSplit = line.split("\t");
            if (generalSplit.length < 2) {
                return false;
            }

            String[] currentSplit = generalSplit[0].split(",");
            String current_x_val = currentSplit[0];
            String current_y_val = currentSplit[1];

            String[] oldSplit = generalSplit[1].split(",");
            String old_x_val = oldSplit[0];
            String old_y_val = oldSplit[1];

            if (distanceBetween(current_x_val, current_y_val, old_x_val, old_y_val) <= threshold) {
                allConverge.add(1);
            }
            else {
                allConverge.add(0);
            }
        }
        IOUtils.closeStream(reader);

        return !allConverge.contains(0);
    }

    public static void main(String[] args) throws Exception {
        String points_inputPath = args[0];
        String seeds = args[1];
        String outputPath = args[2] + "iteration0";
        int threshold = Integer.parseInt(args[3]);
        int R = 20;
        int latestInt = 0;

        Configuration conf = new Configuration();
        for (int i = 0; i < R; i++) {
            if (i != 0) {
                seeds = args[2] + "iteration" + (i - 1) + "/part-r-00000";
                outputPath = args[2] + "iteration" + i;
            }

            if (convergence(seeds, threshold, conf)) {
                latestInt = i - 1;
                break;
            }

            Job job = Job.getInstance(conf, "KMeans " + i);
            FileInputFormat.addInputPath(job, new Path(points_inputPath));
            job.addCacheFile(new URI(seeds));
            job.setMapperClass(KMeans.KMeansMapper.class);
            job.setReducerClass(KMeans.KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }
        Job job1 = Job.getInstance(conf, "KMeansFinal");
        FileInputFormat.addInputPath(job1, new Path(points_inputPath));
        System.out.println(args[2] + "iteration" + latestInt + "/part-r-00000");
        job1.addCacheFile(new URI(args[2] + "iteration" + latestInt + "/part-r-00000"));
        job1.setMapperClass(KMeans.KMeansMapper.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        System.out.println(args[2] + "final_output");
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "final_output"));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
