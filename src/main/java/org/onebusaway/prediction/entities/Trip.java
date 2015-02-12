package org.onebusaway.prediction.entities;

import java.util.*;

/**
 * Represents a bus going from the beginning of a route to the end one time
 */
public class Trip {

    private String tripKey;
    private String gtfsTripId;
    private List<Observation> observations = new ArrayList<>();
    private Map<String, Date> arrivalMap = new HashMap<>();

    public String getTripKey() {
        return tripKey;
    }

    public void setTripKey(String tripKey) {
        this.tripKey = tripKey;
    }

    public List<Observation> getObservations() {
        return observations;
    }

    public Map<String, Date> getArrivalMap() {
        return arrivalMap;
    }

    public String getGtfsTripId() {
        return gtfsTripId;
    }

    public void setGtfsTripId(String gtfsTripId) {
        this.gtfsTripId = gtfsTripId;
    }
}
