package problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class randomPoints {

    public static void generateRandom(int numPoints, String fileName) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

        Random random = new Random();
        for (int i = 0; i < numPoints; i++) {
            int x = random.nextInt(5001); // Generate random x-coordinate between 0 and 5000
            int y = random.nextInt(5001); // Generate random y-coordinate between 0 and 5000
            writer.write(x + "," + y); // Write the point to the file
            writer.newLine(); // Move to the next line
        }

        writer.close();
    }

    public static void main(String[] args) throws IOException {

        String outputPath = args[0];
        String fileName = args[1];
        String numOfPoints = args[2];

        generateRandom(Integer.parseInt(numOfPoints), fileName);
        System.out.println(fileName + " generated in " + outputPath);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(fileName), new Path(outputPath));
    }
}