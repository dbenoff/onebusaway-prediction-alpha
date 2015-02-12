package org.onebusaway.prediction.recommend;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.PlusAnonymousConcurrentUserDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.onebusaway.prediction.service.TripDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

public class RecommenderService {

    private Logger log = Logger.getLogger(RecommenderService.class);
    private UserNeighborhood neighborhood;


        /*

        To get 'anonymous' recommendations:

        Long userId = plusModel.takeAvailableUser();
        PreferenceArray anonymousPrefs = new GenericUserPreferenceArray(3);
        anonymousPrefs.setUserID(0, userId);
        anonymousPrefs.setItemID(0, 100L);
        anonymousPrefs.setValue(0, 578.2811441389305f);
        anonymousPrefs.setItemID(1, 200L);
        anonymousPrefs.setValue(1, 628.1081578707681f);
        anonymousPrefs.setItemID(2, 300L);
        anonymousPrefs.setValue(2, 0.37037037037037035f);
        plusModel.setTempPrefs(anonymousPrefs, userId);
        long[] userIds = neighborhood.getUserNeighborhood(userId);
        plusModel.clearTempPrefs(userId);
        */

    public long[] recommend(long inputId) {
        try {
            return  neighborhood.getUserNeighborhood(inputId);
        } catch (TasteException e) {
            throw new RuntimeException("Mahout exception");
        }
    }

    public RecommenderService(File dataFile){

        DataModel fileDataModel = null;
        try {
            fileDataModel = new FileDataModel(dataFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't open data file");
        }
        PlusAnonymousConcurrentUserDataModel plusModel = new PlusAnonymousConcurrentUserDataModel(fileDataModel, 100);
        UserSimilarity userSimilarity = null;
        try {
            userSimilarity = new EuclideanDistanceSimilarity(plusModel);
            neighborhood = new NearestNUserNeighborhood(100, userSimilarity, plusModel);
        } catch (TasteException e) {
            throw new RuntimeException("Mahout exception");
        }


    }

}
