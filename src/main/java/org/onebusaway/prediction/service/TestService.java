package org.onebusaway.prediction.service;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@Component
public class TestService {

    Logger log = Logger.getLogger(TestService.class);
    private TripDataService tripDataService;

    public void computeStatsForGtfsTripId(String gtfsTripId, String csvpath, Boolean append) throws IOException, ParseException {



        Map<String, Map<String, Date>> allArrivalsMap = tripDataService.getArrivalsForEntireRouteByGtfsTripId(gtfsTripId, true);

        Map<String, List<Long[]>> stopPairTravelTimeMap = new HashMap<>();
        List<String> stopList = tripDataService.getStopListByGtfsTripId(gtfsTripId);
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
                        if(stopList.indexOf(fromStopId) > 1){
                            String previousStopId = stopList.get(stopList.indexOf(fromStopId) - 1);
                            if(arrivals.keySet().contains(previousStopId)){
                                Date previousArrival = arrivals.get(previousStopId);
                                previousInterval = (start.getTime() - previousArrival.getTime()) / 1000;
                            }
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
                    }
                }
            }
        }

        Map<String, Double> meansMap = new HashMap<>();
        Map<String, Double> meanSquaredErrorMap = new HashMap<>();
        Map<String, Double> meanErrorMap = new HashMap<>();

        Map<String, Integer> nMap = new HashMap<>();
        DescriptiveStatistics overallMeanDS = new DescriptiveStatistics();
        DescriptiveStatistics absoluteErrorDS = new DescriptiveStatistics();
        DescriptiveStatistics absoluteErrorSquaredDS = new DescriptiveStatistics();
        for(String key : stopPairTravelTimeMap.keySet()){

            List<Long[]> intervals = stopPairTravelTimeMap.get(key);

            //remove intervals where we don't have a travel time for the previous segment
            Iterator<Long[]> intervalIter = intervals.iterator();
            while(intervalIter.hasNext()){
                Long[] interval = intervalIter.next();
                if(interval[2] == null){
                    intervalIter.remove();
                }
            }


            if(intervals.size() < 5)
                continue;

            for(Long[] interval : intervals){
                overallMeanDS.addValue(interval[0]);
            }
            Double mean = overallMeanDS.getMean();
            overallMeanDS.getStandardDeviation();
            overallMeanDS.clear();
            meansMap.put(key, mean);

            nMap.put(key, intervals.size());
            DescriptiveStatistics relativeMeanDS = new DescriptiveStatistics();
            for(int i = 0; i < intervals.size(); i++){
                for(int j = 0; j < intervals.size(); j++){
                    if(j != i){
                        relativeMeanDS.addValue(intervals.get(j)[0]);
                    }
                }
                double relativeMean = relativeMeanDS.getMean();
                relativeMeanDS.clear();
                double error = Math.abs(intervals.get(i)[0] - relativeMean);
                double errorSquared = error * error;
                absoluteErrorDS.addValue(error);
                absoluteErrorSquaredDS.addValue(errorSquared);
            }
            meanSquaredErrorMap.put(key, absoluteErrorSquaredDS.getMean());
            meanErrorMap.put(key, absoluteErrorDS.getMean());
            absoluteErrorDS.clear();
            absoluteErrorSquaredDS.clear();
        }

        List<String[]> rows = new ArrayList();

        File f = new File(csvpath);

        if(!append){
            f.delete();
            rows.add(new String[]{"GTFS Trip ID", "From Stop ID", "To Stop ID", "# of observations", "Mean Travel Time", "Mean Error", "Mean Squared Error"});
        }


        for(String stopPairKey : meanSquaredErrorMap.keySet()){
            String[] stops = stopPairKey.split("\\|");
            rows.add(new String[]{
                            gtfsTripId,
                            stops[0],
                            stops[1],
                            nMap.get(stopPairKey).toString(),
                            String.format("%.2f", meansMap.get(stopPairKey)),
                            String.format("%.2f", meanErrorMap.get(stopPairKey)),
                            String.format("%.2f", meanSquaredErrorMap.get(stopPairKey)),
                }
            );
        }

        FileWriter fileWriter = new FileWriter(csvpath, append);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.DEFAULT_QUOTE_CHARACTER);
        csvWriter.writeAll(rows);
        csvWriter.flush();

    }

    @Autowired
    public TestService(TripDataService tripDataService) {
        this.tripDataService = tripDataService;
    }

}
