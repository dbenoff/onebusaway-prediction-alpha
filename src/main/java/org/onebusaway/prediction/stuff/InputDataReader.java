package org.onebusaway.prediction.stuff;

import org.apache.log4j.Logger;
import org.onebusaway.prediction.entities.Observation;
import org.onebusaway.prediction.entities.Trip;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class InputDataReader {
    private Logger log = Logger.getLogger(this.getClass());
    private Map<String, Trip> tripMap = new HashMap();

    public void prepareData() throws Throwable {

        SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
        File[] files = new File("src/main/resources/csv/").listFiles();
        for (File file : files) {
            if (file.isFile()) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                while (line != null) {
                    String[] elements = getElements(line);
                    Date timeReported = format.parse(elements[29]);

                    /*vehicle_id	 inferred_trip_id	 assigned_run_id    date
                    31	20	33  29*/
                    String key =  elements[31] + "|" + elements[20] + "|" + elements[33] + "|" + elements[29].split(" ")[0];
                    Trip trip;
                    if(tripMap.keySet().contains(key)){
                        trip = tripMap.get(key);
                    }else{
                        trip = new Trip();
                        trip.setTripKey(key);
                        tripMap.put(key,trip);
                    }

                    Observation o = new Observation();
                    o.setTrip(trip);
                    trip.getObservations().add(o);

                    String originStopId = elements[26];
                    Double distanceFromOriginStop = Double.parseDouble(elements[25]);
                    String destinationStopId = elements[24];
                    Double distanceFromDestinationStop = Double.parseDouble(elements[23]);
                    Double latitude = Double.parseDouble(elements[13]);
                    Double longitude = Double.parseDouble(elements[14]);

                    o.setTimeReported(timeReported);
                    o.setOriginStopId(originStopId);
                    o.setDistanceFromOriginStop(distanceFromOriginStop);
                    o.setDestinationStopId(destinationStopId);
                    o.setDistanceToDestinationStop(distanceFromDestinationStop);
                    o.setLatitude(latitude);
                    o.setLongitude(longitude);
                    o.setId(elements[0]);
                    o.setVehicleId(elements[31]);
                    o.setAssignedRunId(elements[33]);


                    line = br.readLine();
                }
                for(String key : tripMap.keySet()){
                    Trip trip = tripMap.get(key);

                    List<Observation> observations = trip.getObservations();
                    observations.sort(new Comparator<Observation>() {
                        @Override
                        public int compare(Observation o1, Observation o2) {
                            return o1.getTimeReported().compareTo(o2.getTimeReported());
                        }
                    });

                    String nextStop = null;
                    List<String> visitedStops = new ArrayList<>();
                    Iterator<Observation> observationIterator = trip.getObservations().iterator();
                    while(observationIterator.hasNext()){
                        Observation o = observationIterator.next();
                        if(nextStop == null){
                            nextStop = o.getDestinationStopId();
                        }
                        log.warn(nextStop);
                        if(!o.getDestinationStopId().equals(nextStop)){
                            log.warn(o.getDestinationStopId() + " " + nextStop);
                            if(visitedStops.contains(o.getDestinationStopId())){
                                log.warn(o.getDestinationStopId() + " " + nextStop);
                            }else{
                                visitedStops.add(nextStop);
                                nextStop = o.getDestinationStopId();
                            }
                        }
                    }
                    log.warn(visitedStops);
                }
            }
        }
    }

    private String[] getElements(String input){
        String[] elements = input.split("\t");
        for(int i = 0; i < elements.length; i++){
            elements[i] = elements[i].replaceAll("\"", "");
        }
        return elements;
    }
}
