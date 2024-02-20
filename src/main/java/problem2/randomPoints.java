package problem2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
        System.out.println("Points generated");
    }

    public static void main(String[] args) throws IOException {

        int numOfPoints = 5;
        String fileName = "points.txt";

        generateRandom(5, fileName);
    }
}