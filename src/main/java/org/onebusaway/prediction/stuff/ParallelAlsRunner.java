package org.onebusaway.prediction.stuff;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.cf.taste.hadoop.als.ParallelALSFactorizationJob;
import org.apache.mahout.driver.MahoutDriver;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ParallelAlsRunner {
    public static void main(String[] args) throws Throwable {
        /*createMatrix();
        createSequenceFile();*/
        createRecommendations();
    }

    private static void createMatrix() throws Exception {
        File f = new File("/Users/dbenoff/java/mahout_test/als/als_output");
        FileUtils.deleteDirectory(f);
        f = new File("/Users/dbenoff/java/mahout_test/als/tmp");
        FileUtils.deleteDirectory(f);

        String argz = "--input /Users/dbenoff/java/mahout_test/als/all_ratings.csv --output /Users/dbenoff/java/mahout_test/als/als_output --lambda 0.1 --implicitFeedback true --alpha 0.8 --numFeatures 2 --numIterations 5 --numThreadsPerSolver 1 --tempDir /Users/dbenoff/java/mahout_test/als/tmp";
        String[] args = argz.split(" ");
        ParallelALSFactorizationJob job = new ParallelALSFactorizationJob();
        job.run(args);
    }

    private static void createSequenceFile() throws Exception {
        String filename = "/Users/dbenoff/java/mahout_test/als/sequence.csv";
        String outputfilename =  "/Users/dbenoff/java/mahout_test/als/sequencesfiles/part-0000";
        new File(outputfilename).delete();
        Path path = new Path(outputfilename);
        //opening file
        BufferedReader br = new BufferedReader(new FileReader(filename));
        //creating Sequence Writer
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer (fs,conf,path,IntWritable.class,VectorWritable.class);
        String line = br.readLine();

        String delimiter = "\t";

        line = br.readLine();
        while (line != null) {
            String[] temp = line.split(delimiter);
            IntWritable key = new IntWritable(Integer.parseInt(temp[0]));
            RandomAccessSparseVector vector = new RandomAccessSparseVector(temp.length - 1);
            for (int i=1; i< temp.length;i++) {
                vector.set(i -1, Long.parseLong(temp[i]));
            }
            VectorWritable value = new VectorWritable(vector);
            writer.append(key,value);
            line = br.readLine();
        }
        writer.close();
        br.close();
    }

    public static void createRecommendations() throws Throwable {
        FileUtils.deleteDirectory(new File("/Users/dbenoff/java/mahout_test/als/recommendations"));
        String argz = "recommendfactorized --input /Users/dbenoff/java/mahout_test/als/sequencefiles/part-0000 --numRecommendations 1 --output /Users/dbenoff/java/mahout_test/als/recommendations --maxRating 1 --userFeatures /Users/dbenoff/java/mahout_test/als/als_output/U/ --itemFeatures /Users/dbenoff/java/mahout_test/als/als_output/M/ ";
        String[] args = argz.split(" ");
        MahoutDriver.main(args);
    }
}
