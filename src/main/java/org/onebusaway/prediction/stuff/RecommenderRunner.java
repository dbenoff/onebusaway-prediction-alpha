package org.onebusaway.prediction.stuff;

import org.apache.commons.io.FileUtils;
import org.apache.mahout.driver.MahoutDriver;

import java.io.File;

public class RecommenderRunner {
    public static void main(String[] args) throws Throwable {
        FileUtils.deleteDirectory(new File("/Users/dbenoff/java/mahout_test/als/recommendations"));
        String argz = "recommendfactorized --input /Users/dbenoff/java/mahout_test/als/sequencefiles/part-0000 --numRecommendations 1 --output /Users/dbenoff/java/mahout_test/als/recommendations --maxRating 1 --userFeatures /Users/dbenoff/java/mahout_test/als/als_output/U/ --itemFeatures /Users/dbenoff/java/mahout_test/als/als_output/M/ ";
        args = argz.split(" ");
        MahoutDriver.main(args);
    }
}
