package org.onebusaway.prediction.stuff;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

@Component
public class StopListGenerator {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(StopListGenerator.class);

        public void prepareGtfsStopLists() throws Throwable {

            showStats();
            Map<String, Collection<String>> stopMap = new HashMap<>();
            String stopsSql = "SELECT * FROM dbenoff_gtfs_stop_times ";

            List<String[]> stopsData = jdbcTemplate.query(stopsSql, new RowMapper<String[]>() {
                @Override
                public String[] mapRow(ResultSet resultSet, int rowIndex) throws SQLException {

                    String[] row = new String[7];
                    for(int i = 0; i < row.length; i++){
                        row[i] = resultSet.getString(i + 1);
                    }

                    if(rowIndex % 10000 == 0){
                        log.warn("processed " + rowIndex + " rows");
                        showStats();
                    }

                    return row;
                }
            });

            for(String[] row : stopsData){

                String tripId = row[0];
                String stopId = row[1];
                if(!stopMap.keySet().contains(tripId)){
                    stopMap.put(tripId, new ArrayList<String>());
                }
                Collection<String> stopList = stopMap.get(tripId);
                stopList.add(stopId);
            }

            int i = 0;
            for(String tripId : stopMap.keySet()){

                Collection<String> stops = stopMap.get(tripId);
                Set<String> stopSet = new HashSet<>(stops);
                if(stopSet.size() != stops.size()){
                    //will need to figure out a strategy for handling these trips
                    log.warn("Skipping trip " + tripId + " because it visits the same stop twice");
                }

                StringJoiner join = new StringJoiner(",");
                for(String stopId : stops){
                    join.add(stopId);
                }
                String insertSql = "INSERT INTO stop_lists (" +
                                "	trip_id, " +
                                "	stops) " +
                                "VALUES (?, ?)";

                Object[] params = new Object[] { tripId, join.toString()};
                int[] types = new int[] { Types.VARCHAR, Types.VARCHAR};
                int row = jdbcTemplate.update(insertSql, params, types);
                i++;
                if(i % 10000 == 0){
                    log.warn("processed " + i + " trips");
                }
            }
        }

    private void showStats(){

            int mb = 1024*1024;

            //Getting the runtime reference from system
            Runtime runtime = Runtime.getRuntime();

            System.out.println("##### Heap utilization statistics [MB] #####");

            //Print used memory
            System.out.println("Used Memory:"
                    + (runtime.totalMemory() - runtime.freeMemory()) / mb);

            //Print free memory
            System.out.println("Free Memory:"
                    + runtime.freeMemory() / mb);

            //Print total available memory
            System.out.println("Total Memory:" + runtime.totalMemory() / mb);

            //Print Maximum available memory
            System.out.println("Max Memory:" + runtime.maxMemory() / mb);
    }

    @Autowired
    public StopListGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
