package org.onebusaway.prediction.util;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;
import org.onebusaway.prediction.entities.Segment;
import org.onebusaway.prediction.service.TripDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

@Component
public class SegmentLevelDataExporter {

    private TripDataService tripDataService;
    private Logger log = Logger.getLogger(SegmentLevelDataExporter.class);
    private String testOutputPath = "src/main/resources/csv/raw_data_";

    public void generateExtract(String gtfsTripId) throws Throwable {

        Map<String, Map<String, Date>> allArrivalsMap = tripDataService.getArrivalsForEntireRouteByGtfsTripId(gtfsTripId, true);

        Map<String, List<Long[]>> stopPairTravelTimeMap = new HashMap<>();
        List<String> stopList = tripDataService.getStopListByGtfsTripId(gtfsTripId);
        List<Double> distanceList = tripDataService.getDistancesByGtfsTripId(gtfsTripId);

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

        List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"Segment ID", "Segment Index", "Seconds Since Midnight", "Prior Segment Rate", "Current Segment Rate", "Prior Segment Distance", "Current Segment Distance"});
        for(Integer segmentId : segmentIdToSegmentMap.keySet()){
            Segment s = segmentIdToSegmentMap.get(segmentId);
            if(stopList.indexOf(s.getDestinationStopId()) - stopList.indexOf(s.getOriginStopId()) > 1)
                continue;

            Double distance = distanceList.get(stopList.indexOf(s.getDestinationStopId())) - distanceList.get(stopList.indexOf(s.getOriginStopId()));
            Double rate = distance / s.getTravelTimeInSeconds();

            Double priorDistance = distanceList.get(stopList.indexOf(s.getOriginStopId())) - distanceList.get(stopList.indexOf(s.getOriginStopId()) - 1);
            Double priorRate = priorDistance / s.getPriorSegmentTravelTimeInSeconds();

            String[] row = new String[7];

            row[0] = s.getTripKey();
            row[1] = String.valueOf(stopList.indexOf(s.getOriginStopId()));
            row[2] = s.getSecondsSinceMidnightUntilStartTime().toString();
            row[3] = priorRate.toString();
            row[4] = rate.toString();
            row[5] = priorDistance.toString();
            row[6] = distance.toString();
            rows.add(row);
        }


        String filePath = this.testOutputPath + gtfsTripId + ".csv";
        File f = new File(filePath);
        f.delete();
        FileWriter fileWriter = new FileWriter(filePath);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);
        csvWriter.writeAll(rows);
        csvWriter.flush();
        csvWriter.close();

    }

    @Autowired
    public SegmentLevelDataExporter(TripDataService tripDataService) {
        this.tripDataService = tripDataService;
    }

}
