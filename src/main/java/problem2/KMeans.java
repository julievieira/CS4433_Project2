package problem2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class KMeans {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        private ArrayList<ArrayList<String>> centroidList = new ArrayList<>();

        private double distanceBetween(String x_1, String y_1, String x_2, String y_2) {
            double x1 = Double.parseDouble(x_1);
            double y1 = Double.parseDouble(y_1);
            double x2 = Double.parseDouble(x_2);
            double y2 = Double.parseDouble(y_2);
            return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] generalSplit = line.split("\t");
                String[] split = generalSplit[0].split(",");
                String x_val = split[0];
                String y_val = split[1];
                ArrayList<String> aCentroid = new ArrayList<>();
                aCentroid.add(x_val);
                aCentroid.add(y_val);
                centroidList.add(aCentroid);
            }

            IOUtils.closeStream(reader);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // parse fields
            String[] allPoints = value.toString().split(",");
            String x_val = allPoints[0];
            String y_val = allPoints[1];

            double minDistance = 100000;
            String closestCentroid = null;
            for (int i = 0; i < centroidList.size(); i++) {
                String xCentroid = centroidList.get(i).get(0);
                String yCentroid = centroidList.get(i).get(1);
                double distance = distanceBetween(x_val, y_val, xCentroid, yCentroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = String.format("%s,%s", xCentroid, yCentroid);
                }
            }

            outputKey.set(closestCentroid);
            outputValue.set(String.format("%s,%s", x_val, y_val));
            context.write(outputKey, outputValue);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sumX = 0;
            double numX = 0;
            double sumY = 0;
            double numY = 0;
            for (Text value : values) {
                String[] point = value.toString().split(",");
                String x_val = point[0];
                String y_val = point[1];

                sumX += Double.parseDouble(x_val);
                numX += 1;
                sumY += Double.parseDouble(y_val);
                numY += 1;
            }

            int averageX = (int) Math.round(sumX / numX);
            int averageY = (int) Math.round(sumY / numY);

            outputKey.set(String.format("%d,%d", averageX, averageY));
            outputValue.set(key);
            context.write(outputKey, outputValue);
        }
    }
}
