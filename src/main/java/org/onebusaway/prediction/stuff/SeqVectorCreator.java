package org.onebusaway.prediction.stuff;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by dbenoff on 1/2/15.
 */
public class SeqVectorCreator {
    public static void main(String[] argvs) throws IOException {

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

}
