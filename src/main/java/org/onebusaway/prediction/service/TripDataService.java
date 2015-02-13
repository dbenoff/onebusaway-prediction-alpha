package org.onebusaway.prediction.service;

import org.apache.log4j.Logger;
import org.onebusaway.prediction.entities.Observation;
import org.onebusaway.prediction.entities.Trip;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class TripDataService {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(TripDataService.class);

    public Collection<String> getTripIds() throws Throwable {

        List<String> tripIds = jdbcTemplate.query("select tripid from routes_trips", new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString(1);
            }
        });


        return tripIds;
    }

    public Collection<Trip> getTrips(String tripId) throws Throwable {

        Map<String, String> errorMap = new HashMap<>();
        final Map<String, Trip> tripMap = new HashMap();
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String gtfsTripId = getGtfsTripIdForObaTripId(tripId);


        String stopSql = "select r.* from routes_trips rt\n" +
                "join routes r on rt.routeid = r.id\n" +
                "where rt.tripid = '" + gtfsTripId + "'";

        String[] stopString = jdbcTemplate.queryForObject(stopSql, new RowMapper<String[]>() {
            @Override
            public String[] mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
                return new String[]{resultSet.getString(2), resultSet.getString(3)};
            }
        });

        List<String> stopList = Arrays.asList(stopString[0].split("\\,"));
        List<String> stopDistanceList = Arrays.asList(stopString[1].split("\\,"));

        String tripSql = "select distinct * from inferredlocation " +
                "where next_scheduled_stop_id is not null " +
                "and inferred_phase = 'IN_PROGRESS' " +
                "and inferred_trip_id = '" + tripId + "' " +
                "order by time_reported";

        List<Observation> observations = jdbcTemplate.query(tripSql, new ObservationRowMapper());

        for(Observation o : observations){
            if(!tripMap.keySet().contains(o.getTripKey())){
                Trip trip = new Trip();
                trip.setTripKey(o.getTripKey());
                trip.setGtfsTripId(gtfsTripId);
                tripMap.put(trip.getTripKey(), trip);
            }
            Trip trip = tripMap.get(o.getTripKey());
            trip.getObservations().add(o);
            o.setTrip(trip);
        }

        Set<String> badTrips = new HashSet<>();

        for(String key : tripMap.keySet()){
            Trip trip = tripMap.get(key);
            String priorDestinationStopId = null;
            List<String> visitedStops = new ArrayList<>();
            for(int i = 0; i < trip.getObservations().size(); i++){
                Observation o = trip.getObservations().get(i);

                String destinationStopId = o.getDestinationStopId();

                if(destinationStopId == null){
                    log.warn("No destination for trip id: " + key);
                    continue;
                }

                destinationStopId = destinationStopId.substring(destinationStopId.lastIndexOf("_") + 1);
                if(!stopList.contains(destinationStopId)){
                        log.warn("Found invalid stop for trip " + key);
                        continue;
                    }

                    if(destinationStopId == stopList.get(stopList.size() - 1))
                        log.warn("end of journey for trip: " + key);

                    if(priorDestinationStopId == null){
                        priorDestinationStopId = destinationStopId;
                    } else if (!priorDestinationStopId.equals(destinationStopId)){

                        if(trip.getArrivalMap().containsKey(destinationStopId)){
                        log.warn("Already visited stop id " + destinationStopId + " for trip " + key);
                        continue;
                    }

                    //we've passed a stop
                    Observation priorObservation = trip.getObservations().get(i - 1);

                    int fromIndex = stopList.indexOf(priorDestinationStopId);
                    int toIndex = stopList.indexOf(destinationStopId);

                    if(fromIndex > toIndex){
                        log.warn("Headed in wrong direction from stop " + priorDestinationStopId  + " index " + fromIndex + " to " + destinationStopId + " index " + toIndex);
                        continue;
                    }

                    //assume constant speed and zero dwell time to infer stop arrival time
                    Double distanceCoveredBetweenPings = priorObservation.getDistanceToDestinationStop() + (o.getDistanceFromOriginStop()* -1);
                    Long timeInMillis = o.getTimeReported().getTime() - priorObservation.getTimeReported().getTime();

                    if(toIndex - fromIndex > 1){
                        //the bus passed more than one stop between pings.  Infer each arrival proportionally
                        for(int j = fromIndex + 1; j < toIndex; j++){
                            //add the missing distance
                            distanceCoveredBetweenPings += (Double.parseDouble(stopDistanceList.get(j + 1)) - Double.parseDouble(stopDistanceList.get(j)));
                        }
                    }

                    //Infer the arrival time
                    Double rate = distanceCoveredBetweenPings/timeInMillis * 1000;
                    Double arrivalTimeOffset = o.getDistanceFromOriginStop() / rate;
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(o.getTimeReported());
                    cal.add(Calendar.SECOND, arrivalTimeOffset.intValue());
                    Date arrival = cal.getTime();
                    log.warn("Calculated arrival for " + key + " stopid " + priorDestinationStopId + " at " + arrival);
                    trip.getArrivalMap().put(priorDestinationStopId, arrival);
                    visitedStops.add(priorDestinationStopId);

                    if(toIndex - fromIndex > 1) {
                        if(toIndex - fromIndex > 5){
                            badTrips.add(key);
                            errorMap.put(key, "missing more than 5 stops in trip, skipping " + key);
                            continue;
                        }
                        Date priorArrival = arrival;
                        Double missingDistance = 0d;
                        for(int j = toIndex - 1; j > fromIndex; j--){
                            missingDistance += Double.parseDouble(stopDistanceList.get(j + 1)) - Double.parseDouble(stopDistanceList.get(j));

                            arrivalTimeOffset = (missingDistance / rate) * -1;
                            cal.add(Calendar.SECOND, arrivalTimeOffset.intValue());
                            arrival = cal.getTime();

                            if(priorObservation.getTimeReported().getTime() > arrival.getTime() || arrival.getTime() > priorArrival.getTime()){
                                log.warn("inferred arrival " + arrival + " is BEFORE last ping from bus, discrepancy in linear distance of route between OBA and GTFS.  Re-setting arrival to " + priorObservation.getTimeReported());
                                arrival = priorObservation.getTimeReported();
                            }

                            log.warn(key + " inferring arrival timestamp for stop " + stopList.get(j) + " " + arrival + " at distance "
                                    + (Double.parseDouble(stopDistanceList.get(stopList.indexOf(destinationStopId))) + missingDistance));
                            trip.getArrivalMap().put(stopList.get(j), arrival);
                            priorArrival = arrival;
                        }
                    }
                    priorDestinationStopId = destinationStopId;
                }
            }
        }

        for(String key : tripMap.keySet()){
            Trip trip = tripMap.get(key);
            Date priorDate = null;
            String priorStop = null;
            for(String stopId : stopList){
                if(trip.getArrivalMap().keySet().contains(stopId)){
                    if(priorDate == null){
                        priorDate = trip.getArrivalMap().get(stopId);
                        priorStop = stopId;
                    }else{
                        Date arrival = trip.getArrivalMap().get(stopId);
                        if(arrival.getTime() < priorDate.getTime()){
                            badTrips.add(key);
                        }
                        priorDate = arrival;
                        priorStop = stopId;
                    }
                }
            }
        }

        for(String key : badTrips){
            tripMap.remove(key);
            if(errorMap.keySet().contains(key)){
                log.warn(errorMap.get(key));
            }else{
                log.warn("Removing invalid trip " + key);
            }
        }

        return tripMap.values();
    }


    public void truncateArrivals(){
        jdbcTemplate.update("truncate table observations_trips;");
        jdbcTemplate.update("truncate table arrivals;");
    }


    public void persistArrivals(Collection<Trip> trips){

        String arrivalInsertSql = "INSERT INTO arrivals (" +
                "	trip_key, " +
                "	trip_id, " +
                "	stop_id, " +
                "	arrival) " +
                "VALUES (?, ?, ?, ?)";
        int[] arrivalTypes = new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP};

        String observationInsertSql = "INSERT INTO observations_trips (observationid, trip_id, trip_key) VALUES (?,?,?);";
        int[] observationTypes = new int[] { Types.BIGINT, Types.VARCHAR, Types.VARCHAR};

        int i = 1;
        for(Trip trip : trips) {
            if(trip.getArrivalMap().keySet().size() < 1)
                continue;
            for(String stopId : trip.getArrivalMap().keySet()){
                Date arrival = trip.getArrivalMap().get(stopId);
                Object[] params = new Object[] { trip.getTripKey(), trip.getGtfsTripId(), stopId, new java.sql.Date(arrival.getTime()) };
                jdbcTemplate.update(arrivalInsertSql, params, arrivalTypes);
            }
            for(Observation o : trip.getObservations()){
                Object[] params = new Object[] { o.getId(), trip.getGtfsTripId(), trip.getTripKey() };
                jdbcTemplate.update(observationInsertSql, params, observationTypes);
            }
            log.warn("persisted arrivals for " + i + " trips");
            i++;
        }


    }

    public List<String> getStopListByGtfsTripId(String tripId){
        String query = " select r.stops from routes_trips rt\n" +
                " join routes r on r.id = rt.routeid\n" +
                " where rt.tripid = '" + tripId + "'";

        String stops = jdbcTemplate.queryForObject(query, (rs, rowNum) -> {
            return rs.getString(1);
        });
        List<String> stopList = new ArrayList<>();
        String[] elements = stops.split(",");
        for(int i = 0; i < elements.length; i++){
            stopList.add(elements[i]);
        }
        return stopList;
    }

    public List<Double> getDistancesByGtfsTripId(String tripId){
        String query = " select r.distances from routes_trips rt\n" +
                " join routes r on r.id = rt.routeid\n" +
                " where rt.tripid = '" + tripId + "'";

        String stops = jdbcTemplate.queryForObject(query, (rs, rowNum) -> {
            return rs.getString(1);
        });
        List<Double> stopList = new ArrayList<>();
        String[] elements = stops.split(",");
        for(int i = 0; i < elements.length; i++){
            stopList.add(Double.parseDouble(elements[i]));
        }
        return stopList;
    }

    public Observation getObservationById(Long id){
        String query = "select * from inferredlocation_test " +
                "where id = " + id;

        return jdbcTemplate.queryForObject(query, new ObservationRowMapper());
    }

    public List<String> getGtfsTripsWithSameRouteByGtfsTripId(String gtfsTripId){
        String topNSql = "select rt2.tripid from routes_trips rt1\n" +
                "join routes_trips rt2 on rt1.routeid = rt2.routeid\n" +
                " where rt1.tripid = '" + gtfsTripId + "'";

        List<String> tripIds = jdbcTemplate.query(topNSql, (rs, rowNum) -> {
            return rs.getString(1);
        });
        return tripIds;
    }

    public List<String> getObaTripsWithSameRouteByObaTripId(String obaTripId){
        String topNSql = "select distinct ti2.oba_trip_id from trip_ids ti1\n" +
                "join routes_trips rt1 on ti1.gtfs_trip_id = rt1.tripid\n" +
                "join routes_trips rt2 on rt1.routeid = rt2.routeid\n" +
                "join trip_ids ti2 on ti2.gtfs_trip_id = rt2.tripid\n" +
                "where ti1.oba_trip_id = '" + obaTripId + "'";

        List<String> tripIds = jdbcTemplate.query(topNSql, (rs, rowNum) -> {
            return rs.getString(1);
        });
        return tripIds;
    }

    public Map<String, Map<String, Date>> getArrivalsForGtfsTripId(String tripId, boolean weekdaysOnly){
        String arrivalSql = "select * from arrivals where trip_id = '" + tripId + "' ";
        if(weekdaysOnly){
            arrivalSql += "and dayofweek(arrival) > 1 and dayofweek(arrival) < 7";
        }
        arrivalSql += " order by arrival";
        Map<String, Map<String, Date>> tripMap = new HashMap<>();
        jdbcTemplate.query(arrivalSql, (rs, rowNum) -> {
            String tripKey = rs.getString(1);
            String stopId = rs.getString(3);
            Date date = rs.getTimestamp(4);
            if(!tripMap.keySet().contains(tripKey)){
                tripMap.put(tripKey, new HashMap<>());
            }
            Map<String, Date> arrivalMap = tripMap.get(tripKey);
            arrivalMap.put(stopId, date);
            return null;
        });
        return tripMap;
    }

    public Map<String, Map<String, Date>> getArrivalsForEntireRouteByGtfsTripId(String tripId, boolean weekdaysOnly){
        String arrivalSql = "select a.* from routes_trips rt1 " +
                "join routes_trips rt2 on rt1.routeid = rt2.routeid " +
                "join arrivals a on a.trip_id = rt2.tripid " +
                "where rt1.tripid = '" + tripId + "' ";
        if(weekdaysOnly){
            arrivalSql += "and dayofweek(arrival) > 1 and dayofweek(arrival) < 7";
        }
        arrivalSql += " order by arrival";
        Map<String, Map<String, Date>> tripMap = new HashMap<>();
        jdbcTemplate.query(arrivalSql, (rs, rowNum) -> {
            String tripKey = rs.getString(1);
            String stopId = rs.getString(3);
            Date date = rs.getTimestamp(4);
            if(!tripMap.keySet().contains(tripKey)){
                tripMap.put(tripKey, new HashMap<>());
            }
            Map<String, Date> arrivalMap = tripMap.get(tripKey);
            arrivalMap.put(stopId, date);
            return null;
        });
        return tripMap;
    }



    public List<String> getDistinctArrivalTripIds(){
        String topNSql = "select distinct trip_id from arrivals";

        List<String> tripIds = jdbcTemplate.query(topNSql, (rs, rowNum) -> {
            return rs.getString(1);
        });
        return tripIds;
    }

    public String getGtfsTripIdForObaTripId(String obaTripId){
        String gtfsTripIdSql = "select SUBSTR(replace(inferred_trip_id, agency_id, ''), 2) " +
                "from inferredlocation " +
                "where inferred_trip_id = '" + obaTripId + "' " +
                "limit 1";

        return jdbcTemplate.queryForObject(gtfsTripIdSql, String.class);
    }

    public class ObservationRowMapper implements RowMapper<Observation>{

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Observation mapRow(ResultSet resultSet, int rowIndex) throws SQLException {

            String[] elements = new String[34];
            for(int i = 0; i < elements.length; i++){
                elements[i] = resultSet.getString(i + 1);
            }

            Date timeReported = null;
            try {
                timeReported = format.parse(elements[29].replace(".0", ""));
            } catch (ParseException e) {
                throw new RuntimeException("unparseable date " + elements[29]);
            }

                /*vehicle_id	 inferred_trip_id	 assigned_run_id    calendar date
                31	20	33  29*/
            String key =  elements[31] + "|" + elements[20] + "|" + elements[33] + "|" + elements[29].split(" ")[0];

            Observation o = new Observation();

            String originStopId = elements[26];
            Double distanceFromOriginStop = Double.parseDouble(elements[25]);
            String destinationStopId = elements[24];
            Double distanceFromDestinationStop = Double.parseDouble(elements[23]);
            Double latitude = Double.parseDouble(elements[13]);
            Double longitude = Double.parseDouble(elements[14]);
            String gtfsTripId = elements[20];

            o.setTripKey(key);
            o.setGtfsTripId(gtfsTripId);
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

            return o;
        }
    }

    @Autowired
    public TripDataService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
