package org.onebusaway.prediction.stuff;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class TestTableGenerator {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(TestTableGenerator.class);

    public void populateTable(int daysOfHistory, String tripId) throws Throwable {

        //jdbcTemplate.execute("truncate table dbenoff_inferredlocation");


        String partitionSql = "SELECT partition_name FROM information_schema.PARTITIONS " +
                "WHERE TABLE_SCHEMA = 'onebusaway_nyc' " +
                "AND TABLE_NAME = 'obanyc_inferredlocation'";

        List<String> partitionNames = jdbcTemplate.query(partitionSql, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSet.getString(1);
            }
        });
        Collections.sort(partitionNames ,Collections.reverseOrder());

        List<String> resultSets = new ArrayList<>();

        for(String partitionName : partitionNames) {

            if (resultSets.size() >= daysOfHistory) {
                break;
            }

            if (partitionName.length() < 9) {
                continue;
            }

            String year = partitionName.substring(1, 5);
            String month = partitionName.substring(5, 7);
            String day = partitionName.substring(7, 9);
            String timestamp = year + "-" + month + "-" + day + " 00:00:00";
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date partitionDate = format.parse(timestamp);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, -1);
            if (partitionDate.getTime() < cal.getTime().getTime()) {

                    for(int i = 0; i < 24; i++){
                        String timeIndex = year + "-" + month + "-" + day + " ";

                        if(i < 10){
                            timeIndex += 0;
                        }

                        timeIndex += String.valueOf(i) + ":00:00";
                        log.warn(partitionName + " " + timeIndex);
                        getResultsForDayAndTripId(timeIndex, tripId);

                        if(i==23){
                            resultSets.add(timeIndex);
                        }
                    }
            }
        }
    }

    private void getResultsForDayAndTripId(String timestamp, String tripId){
        String locationsByTripIdAndDaySql = "insert into dbenoff_inferredlocation " +
                "select * \n" +
                "from obanyc_inferredlocation\n" +
                "where time_reported_index = '" + timestamp + "'\n" +
                "and inferred_phase = 'IN_PROGRESS'\n" +
                "and inferred_trip_id = '" + tripId + "'\n" +
                "order by time_reported";

        log.info("writing service for trip id " + tripId + " and date " + timestamp);
        jdbcTemplate.execute(locationsByTripIdAndDaySql);
    }

    @Autowired
    public TestTableGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
