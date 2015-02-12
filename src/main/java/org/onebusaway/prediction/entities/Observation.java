package org.onebusaway.prediction.entities;

import java.util.Date;

/**
 * Represents a data 'ping' from a bus
 */
public class Observation {

    private String id;
    private Trip trip;
    private String tripKey;
    private String gtfsTripId;
    private Date timeReported;
    private String originStopId;
    private Double distanceFromOriginStop;
    private String destinationStopId;
    private Double distanceToDestinationStop;
    private Double latitude;
    private Double longitude;
    private String vehicleId;
    private String assignedRunId;

    public Trip getTrip() {
        return trip;
    }

    public void setTrip(Trip trip) {
        this.trip = trip;
    }

    public String getTripKey() {
        return tripKey;
    }

    public void setTripKey(String tripKey) {
        this.tripKey = tripKey;
    }

    public Date getTimeReported() {
        return timeReported;
    }

    public void setTimeReported(Date timeReported) {
        this.timeReported = timeReported;
    }

    public String getOriginStopId() {
        return originStopId;
    }

    public void setOriginStopId(String originStopId) {
        this.originStopId = originStopId;
    }

    public Double getDistanceFromOriginStop() {
        return distanceFromOriginStop;
    }

    public void setDistanceFromOriginStop(Double distanceFromOriginStop) {
        this.distanceFromOriginStop = distanceFromOriginStop;
    }

    public String getDestinationStopId() {
        return destinationStopId;
    }

    public void setDestinationStopId(String destinationStopId) {
        this.destinationStopId = destinationStopId;
    }

    public Double getDistanceToDestinationStop() {
        return distanceToDestinationStop;
    }

    public void setDistanceToDestinationStop(Double distanceToDestinationStop) {
        this.distanceToDestinationStop = distanceToDestinationStop;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getAssignedRunId() {
        return assignedRunId;
    }

    public void setAssignedRunId(String assignedRunId) {
        this.assignedRunId = assignedRunId;
    }

    public String getGtfsTripId() {
        return gtfsTripId;
    }

    public void setGtfsTripId(String gtfsTripId) {
        this.gtfsTripId = gtfsTripId;
    }
}
