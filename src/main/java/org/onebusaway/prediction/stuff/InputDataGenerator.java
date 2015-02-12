package org.onebusaway.prediction.stuff;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class InputDataGenerator {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(InputDataGenerator.class);
    private CSVWriter csvWriter = null;

    public void createCsv(int daysOfHistory, String tripId) throws Throwable {

        FileWriter fileWriter = new FileWriter("src/main/resources/csv/fake.csv");
        csvWriter = new CSVWriter(fileWriter, '\t');

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
                        getResultsForDayAndTripId(timeIndex, tripId);
                        if(i==23){
                            resultSets.add(timeIndex);
                        }

                    }
            }
        }
        csvWriter.flush();
    }

    private ResultSet getResultsForDayAndTripId(String timestamp, String tripId){
        String locationsByTripIdAndDaySql = "select * \n" +
                "from obanyc_inferredlocation\n" +
                "where time_reported_index = '" + timestamp + "'\n" +
                "and inferred_phase = 'IN_PROGRESS'\n" +
                "and inferred_trip_id = '" + tripId + "'\n" +
                "order by time_reported";

        log.info("querying for trip id " + tripId + " and date " + timestamp);

        return jdbcTemplate.query(locationsByTripIdAndDaySql, new ResultSetExtractor<ResultSet>() {
            @Override
            public ResultSet extractData(ResultSet rs) throws SQLException, DataAccessException {
                try {
                    csvWriter.writeAll(rs, false);
                } catch (IOException e) {
                    throw new SQLException(e.getMessage());
                }
                return rs;
            }
        });
    }

    @Autowired
    public InputDataGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
