package org.onebusaway.prediction.util;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.onebusaway.prediction.entities.Segment;
import org.onebusaway.prediction.recommend.RecommenderService;
import org.onebusaway.prediction.service.TripDataService;
import org.onebusaway.prediction.stuff.NormUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

@Component
public class SegmentLevelMahoutPredictionsGenerator {

    private RecommenderService recommenderService;
    private TripDataService tripDataService;
    private Logger log = Logger.getLogger(SegmentLevelMahoutPredictionsGenerator.class);
    private String csvpath = "src/main/resources/csv/mahout_segment_data.csv";
    private String testOutputPath = "src/main/resources/csv/mahout_test_results";

    public void testMahoutRecommendations(String gtfsTripId, Double threshold, Boolean useTravelTimeDimension, Boolean useTimeOfDayDimension) throws Throwable {

        Map<String, Map<String, Date>> allArrivalsMap = tripDataService.getArrivalsForEntireRouteByGtfsTripId(gtfsTripId, true);

        Map<String, List<Long[]>> stopPairTravelTimeMap = new HashMap<>();
        List<String> stopList = tripDataService.getStopListByGtfsTripId(gtfsTripId);

        List<Segment> segments = new ArrayList<>();

        for(int i = 0; i < stopList.size(); i++){
            for(int j = i + 1; j < stopList.size(); j++ ){

                String fromStopId = stopList.get(i);
                String toStopId = stopList.get(j);

                for(String tripKey : allArrivalsMap.keySet()){
                    Map<String, Date> arrivals = allArrivalsMap.get(tripKey);
                    if(arrivals.keySet().contains(fromStopId) && arrivals.keySet().contains(toStopId)){
                        Date start = arrivals.get(fromStopId);
                        Date end = arrivals.get(toStopId);
                        Long interval = (end.getTime() - start.getTime()) / 1000;

                        if(interval < 0){
                            throw new RuntimeException("data problem");
                        }

                        String stopPairKey = fromStopId +  "|" + toStopId;

                        Long previousInterval = null;
                        String previousStopId = null;
                        if(stopList.indexOf(fromStopId) > 1){
                            previousStopId = stopList.get(stopList.indexOf(fromStopId) - 1);
                            if(arrivals.keySet().contains(previousStopId)){
                                Date previousArrival = arrivals.get(previousStopId);
                                previousInterval = (start.getTime() - previousArrival.getTime()) / 1000;
                            }
                        }

                        if(previousInterval ==  null){
                            //only include cases where we know the travel time for the prior segment
                            continue;
                        }

                        if(!stopPairTravelTimeMap.containsKey(stopPairKey)){
                            stopPairTravelTimeMap.put(stopPairKey, new ArrayList<>());
                        }

                        Calendar cal = Calendar.getInstance();
                        cal.setTime(end);
                        cal.set(Calendar.HOUR_OF_DAY, 0);
                        cal.set(Calendar.MINUTE, 0);
                        cal.set(Calendar.SECOND, 0);
                        cal.set(Calendar.MILLISECOND, 0);
                        long secondsSinceMidnight = (end.getTime() - cal.getTimeInMillis()) / 1000;
                        stopPairTravelTimeMap.get(stopPairKey).add(new Long[]{interval, secondsSinceMidnight, previousInterval});

                        Segment s = new Segment();
                        s.setOriginStopId(fromStopId);
                        s.setDestinationStopId(toStopId);
                        s.setTripKey(tripKey);
                        s.setGtfsTripId(gtfsTripId);
                        s.setStartTime(start);
                        s.setEndTime(end);
                        s.setTravelTimeInSeconds(interval.intValue());
                        s.setPriorSegmentTravelTimeInSeconds(previousInterval.intValue());
                        s.setSecondsSinceMidnightUntilStartTime((int) secondsSinceMidnight);
                        s.setPreviousStopId(previousStopId);
                        segments.add(s);
                    }
                }
            }
        }

        Map<String, Double[]> stopPairScalingValuesMap = new HashMap<>();
        DescriptiveStatistics timeOfDayDS = new DescriptiveStatistics();
        DescriptiveStatistics priorSegmentTravelTimeDS = new DescriptiveStatistics();
        for(String stopPairId : stopPairTravelTimeMap.keySet()){
            List<Long[]> travelTimes = stopPairTravelTimeMap.get(stopPairId);
            for(Long[] metrics : travelTimes){
                timeOfDayDS.addValue(metrics[1]);
                priorSegmentTravelTimeDS.addValue(metrics[2]);
            }
            stopPairScalingValuesMap.put(stopPairId, new Double[]{
                    timeOfDayDS.getMin(),
                    timeOfDayDS.getMax(),
                    priorSegmentTravelTimeDS.getMin(),
                    priorSegmentTravelTimeDS.getMax()
                }
            );
            timeOfDayDS.clear();
            priorSegmentTravelTimeDS.clear();
        }

        Map<String, Integer> tripKeyToIdMap = new HashMap<>();
        int i = 0;
        for(String tripKey : allArrivalsMap.keySet()){
            tripKeyToIdMap.put(tripKey, i);
            i++;
        }

        Map<Integer, Segment> segmentIdToSegmentMap = new HashMap<>();
        for(Segment s : segments){
            segmentIdToSegmentMap.put(i, s);
            i++;
        }



        Map<String, Integer> stopPairToIdMap = new HashMap<>();
        for(String stopPair : stopPairTravelTimeMap.keySet()){
            stopPairToIdMap.put(stopPair, i);
            i++;
        }

        Map<Integer, Double[]> scaledValuesMap = new HashMap<>();
        List<String[]> rows = new ArrayList<>();
        for(Integer segmentId : segmentIdToSegmentMap.keySet()){
            Segment s = segmentIdToSegmentMap.get(segmentId);
            String stopPairId = s.getOriginStopId() + "|" + s.getDestinationStopId();
            Double[] scalingValues = stopPairScalingValuesMap.get(stopPairId);
            NormUtil priorSegmentTravelTimeNormUtil = new NormUtil(scalingValues[3], scalingValues[2], 1000, 0);
            NormUtil timeOfDayNormUtil = new NormUtil(scalingValues[1], scalingValues[0], 1000, 0);

            Double normalizedPriorSegmentTravelTime = priorSegmentTravelTimeNormUtil.normalize(s.getPriorSegmentTravelTimeInSeconds());
            Double normalizedTimeOfDay = timeOfDayNormUtil.normalize(s.getSecondsSinceMidnightUntilStartTime());

            String[] row = new String[3];
            if(useTravelTimeDimension){
                row[0] = segmentId.toString();
                row[1] = "100"; //mins from midnight
                row[2] = normalizedPriorSegmentTravelTime.toString();
                rows.add(row);
            }

            if(useTimeOfDayDimension){
                row = new String[3];
                row[0] = segmentId.toString();
                row[1] = "200"; //travel time for segment
                row[2] = normalizedTimeOfDay.toString();
                rows.add(row);
            }

            row = new String[3];
            row[0] = segmentId.toString();
            row[1] = "300"; //stop pair ID
            row[2] = String.valueOf(stopPairToIdMap.get(stopPairId) * 100000);
            rows.add(row);
            scaledValuesMap.put(segmentId, new Double[]{normalizedTimeOfDay, normalizedPriorSegmentTravelTime});
        }



        File f = new File(csvpath);
        f.delete();
        FileWriter fileWriter = new FileWriter(csvpath);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);
        csvWriter.writeAll(rows);
        csvWriter.flush();
        csvWriter.close();

        recommenderService = new RecommenderService(f);
        Map<String, List<Double[]>> predictionErrorMap = new HashMap<>();
        int processedSegmentCount = 0;
        for(Integer segmentId : segmentIdToSegmentMap.keySet()){

            Segment inputSegment = segmentIdToSegmentMap.get(segmentId);
            String fromStopId = inputSegment.getOriginStopId();
            String toStopId = inputSegment.getDestinationStopId();
            String stopPairId = fromStopId + "|" + toStopId;

            List<String> relevantHistoricalTripKeys = new ArrayList<>();

            long[] recommendations = recommenderService.recommend(segmentId);

            for(i = 0; i < recommendations.length; i++){
                Segment compareSegment = segmentIdToSegmentMap.get((int)recommendations[i]);
                if(!inputSegment.getOriginStopId().equals(compareSegment.getOriginStopId())
                        || !inputSegment.getDestinationStopId().equals(compareSegment.getDestinationStopId())){
                    continue;
                }

                long travelTimeDiff = Math.abs(inputSegment.getPriorSegmentTravelTimeInSeconds() - compareSegment.getPriorSegmentTravelTimeInSeconds());
                long timeOfDayDiff = Math.abs(inputSegment.getSecondsSinceMidnightUntilStartTime() - compareSegment.getSecondsSinceMidnightUntilStartTime());

                Double[] scaledValues = scaledValuesMap.get(segmentId);
                Double[] compareScaledValues = scaledValuesMap.get((int)recommendations[i]);

                double scaledTravelTimeDiff = Math.abs(scaledValues[0] - compareScaledValues[0]);
                double scaledTimeOfDayDiff = Math.abs(scaledValues[1] - compareScaledValues[1]);
                double totalDiff = (scaledTravelTimeDiff + scaledTimeOfDayDiff);

                if(totalDiff <= threshold){
                    //log.warn("scaled travel time / time of day / sum: " + scaledTravelTimeDiff + " " + scaledTimeOfDayDiff + " " + (scaledTravelTimeDiff + scaledTimeOfDayDiff));
                    relevantHistoricalTripKeys.add(compareSegment.getTripKey());
                }
            }

            Map<String, Date> arrivalsToTest = allArrivalsMap.get(inputSegment.getTripKey());
            double actualTravelTime = (arrivalsToTest.get(toStopId).getTime() - arrivalsToTest.get(fromStopId).getTime()) / 1000;

            Double totalTravelTime = 0d;
            int travelTimeN = 0;
            for(String tripKey : relevantHistoricalTripKeys){
                Map<String, Date> arrivals = allArrivalsMap.get(tripKey);
                if(arrivals.keySet().contains(fromStopId) && arrivals.keySet().contains(toStopId)){
                    double travelTime = (arrivals.get(toStopId).getTime() - arrivals.get(fromStopId).getTime()) / 1000;
                    totalTravelTime += travelTime;
                    travelTimeN++;
                }
            }

            if(travelTimeN > 4){
                Double meanTravelTime = totalTravelTime / travelTimeN;
                Double error = Math.abs(actualTravelTime - meanTravelTime);
                if(error.isNaN()){
                    throw new RuntimeException();
                }

                if(!predictionErrorMap.keySet().contains(stopPairId)){
                    predictionErrorMap.put(stopPairId, new ArrayList<>());
                }
                predictionErrorMap.get(stopPairId).add(new Double[]{actualTravelTime, meanTravelTime, error, (double)travelTimeN});
                log.warn("Created prediction for stop pair " + stopPairId + " with error pct " + (error / meanTravelTime));
            }
            processedSegmentCount++;
            if(processedSegmentCount % 1000 == 0){
                log.warn("processed " + processedSegmentCount + " segments out of " + segmentIdToSegmentMap.keySet().size());
            }
            /*if(processedSegmentCount > 500)
                break;*/
        }

        rows = new ArrayList();

        String filePath = testOutputPath + "_" + gtfsTripId + "_" + threshold.toString() + "_" + useTimeOfDayDimension.toString() + "_" + useTravelTimeDimension.toString() + ".csv";
        f = new File(filePath);

        f.delete();
        rows.add(new String[]{"GTFS Trip ID", "From Stop ID", "To Stop ID", "# of Predictions", "Mean Travel Time", "Mean Error in Secs", "Mean Squared Error", "<= 10% error", "<= 20% error", "<= 30% error","> 30% error"});


        for(String stopPairKey : predictionErrorMap.keySet()){
            String[] stops = stopPairKey.split("\\|");
            List<Double[]> metrics = predictionErrorMap.get(stopPairKey);
            Double cumulativeSquaredError = 0d;
            Double cumulativeError = 0d;
            Double cumulativeTravelTime = 0d;
            int tenPercentErrorCount = 0;
            int twentyPercentErrorCount = 0;
            int thirtyPercentErrorCount = 0;
            int moreThanThirtyPercentErrorCount = 0;
            for(Double[] metric : metrics){
                Double actualTravelTime = metric[0];
                Double meanTravelTime = metric[1];
                Double error = metric[2];
                Double sampleSize = metric[3];

                cumulativeTravelTime += actualTravelTime;
                cumulativeError += error;
                cumulativeSquaredError += (error * error);
                Double errorPct = error / actualTravelTime;
                if(errorPct <= .1d){
                    tenPercentErrorCount++;
                }else if(errorPct <= .2d){
                    twentyPercentErrorCount++;
                }else if(errorPct <= .3d){
                    thirtyPercentErrorCount++;
                }else if(errorPct > .3d){
                    moreThanThirtyPercentErrorCount++;
                }
            }
            double meanSquaredError = cumulativeSquaredError / metrics.size();
            double meanTravelTime = cumulativeTravelTime / metrics.size();
            double meanError = cumulativeError / metrics.size();
            //"", "<= 20% error", "<= 30% error","> 30% error"
            rows.add(new String[]{
                            gtfsTripId, //"GTFS Trip ID"
                            stops[0], //"From Stop ID"
                            stops[1], //"To Stop ID"
                            String.valueOf(metrics.size()), //# of observations
                            String.format("%.2f", meanTravelTime), //Mean Travel Time
                            String.format("%.2f", meanError), //Mean Error
                            String.format("%.2f", meanSquaredError), //Mean Squared Error
                            String.valueOf(tenPercentErrorCount), //<= 10% error
                            String.valueOf(twentyPercentErrorCount), //<= 20% error
                            String.valueOf(thirtyPercentErrorCount), //<= 30% error
                            String.valueOf(moreThanThirtyPercentErrorCount), //> 30% error
                    }
            );
        }

        fileWriter = new FileWriter(filePath, false);
        csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.DEFAULT_QUOTE_CHARACTER);
        csvWriter.writeAll(rows);
        csvWriter.flush();
        csvWriter.close();
    }

    private String getMinutesFromSeconds(Double totalSecs){
        Double hours = totalSecs / 3600;
        Double minutes = (totalSecs % 3600) / 60;
        Double seconds = totalSecs % 60;

        String timeString = String.format("%02d:%02d:%02d", hours.intValue(), minutes.intValue(), seconds.intValue());
        return timeString;
    }

    @Autowired
    public SegmentLevelMahoutPredictionsGenerator(TripDataService tripDataService) {
        this.tripDataService = tripDataService;
    }

}
