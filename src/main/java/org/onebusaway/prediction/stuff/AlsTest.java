package org.onebusaway.prediction.stuff;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.io.File;
import java.util.List;

/**
 * Created by dbenoff on 12/29/14.
 */
public class AlsTest {
    public static void main(String[] args) throws Exception {

        String filename = "/Users/dbenoff/java/mahout_test/als/all_ratings.csv";
        DataModel dataModel = new FileDataModel(new File(filename));

        ALSWRFactorizer factorizer = new ALSWRFactorizer(dataModel, 50, 0.065, 15);

        SVDRecommender recommender = new SVDRecommender(dataModel, factorizer);


        List<RecommendedItem> items = recommender.recommend(2, 1);

        System.out.println(items);
    }
}
