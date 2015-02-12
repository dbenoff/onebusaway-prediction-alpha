package org.onebusaway.prediction.entities;

import java.util.Date;

/**
 * Represents a bus going from one stop to another one time.
 */
public class Segment {
    String originStopId;
    String destinationStopId;
    String previousStopId;
    Date startTime;
    Date endTime;
    Integer travelTimeInSeconds;
    String gtfsTripId;
    Integer routeId;
    String tripKey;
    Integer priorSegmentTravelTimeInSeconds;
    Integer secondsSinceMidnightUntilStartTime;
    Integer scaledPriorSegmentTravelTimeInSeconds;
    Integer scaledSecondsSinceMidnightUntilStartTime;

    public String getOriginStopId() {
        return originStopId;
    }

    public void setOriginStopId(String originStopId) {
        this.originStopId = originStopId;
    }

    public String getDestinationStopId() {
        return destinationStopId;
    }

    public void setDestinationStopId(String destinationStopId) {
        this.destinationStopId = destinationStopId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Integer getTravelTimeInSeconds() {
        return travelTimeInSeconds;
    }

    public void setTravelTimeInSeconds(Integer travelTimeInSeconds) {
        this.travelTimeInSeconds = travelTimeInSeconds;
    }

    public Integer getPriorSegmentTravelTimeInSeconds() {
        return priorSegmentTravelTimeInSeconds;
    }

    public void setPriorSegmentTravelTimeInSeconds(Integer priorSegmentTravelTimeInSeconds) {
        this.priorSegmentTravelTimeInSeconds = priorSegmentTravelTimeInSeconds;
    }

    public String getGtfsTripId() {
        return gtfsTripId;
    }

    public void setGtfsTripId(String gtfsTripId) {
        this.gtfsTripId = gtfsTripId;
    }

    public Integer getRouteId() {
        return routeId;
    }

    public void setRouteId(Integer routeId) {
        this.routeId = routeId;
    }

    public String getTripKey() {
        return tripKey;
    }

    public void setTripKey(String tripKey) {
        this.tripKey = tripKey;
    }

    public Integer getSecondsSinceMidnightUntilStartTime() {
        return secondsSinceMidnightUntilStartTime;
    }

    public void setSecondsSinceMidnightUntilStartTime(Integer secondsSinceMidnightUntilStartTime) {
        this.secondsSinceMidnightUntilStartTime = secondsSinceMidnightUntilStartTime;
    }

    public Integer getScaledPriorSegmentTravelTimeInSeconds() {
        return scaledPriorSegmentTravelTimeInSeconds;
    }

    public void setScaledPriorSegmentTravelTimeInSeconds(Integer scaledPriorSegmentTravelTimeInSeconds) {
        this.scaledPriorSegmentTravelTimeInSeconds = scaledPriorSegmentTravelTimeInSeconds;
    }

    public Integer getScaledSecondsSinceMidnightUntilStartTime() {
        return scaledSecondsSinceMidnightUntilStartTime;
    }

    public void setScaledSecondsSinceMidnightUntilStartTime(Integer scaledSecondsSinceMidnightUntilStartTime) {
        this.scaledSecondsSinceMidnightUntilStartTime = scaledSecondsSinceMidnightUntilStartTime;
    }

    public String getPreviousStopId() {
        return previousStopId;
    }

    public void setPreviousStopId(String previousStopId) {
        this.previousStopId = previousStopId;
    }
}
