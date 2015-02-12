package org.onebusaway.prediction.util;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;
import org.onebusaway.prediction.entities.Observation;
import org.onebusaway.prediction.entities.Trip;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Component
public class ObservationLevelMahoutDataGenerator {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(ObservationLevelMahoutDataGenerator.class);
    private String csvpath = "src/main/resources/csv/mahout.csv";

    /*
    min(inferred_latitude)	max(inferred_latitude)	min(inferred_longitude)	max(inferred_longitude)
    38.288654	42.443569	-75.506264	-71.422012
    */

    public String[][] generateMahoutData(Collection<Trip> trips) throws Throwable {

        Calendar cal = Calendar.getInstance();
        List<String[]> rows = new ArrayList<>();
        Double[] minMaxLatLon = getMinMaxLatLon();

        for(Trip trip : trips){
            for(Observation o : trip.getObservations()){

                Double ceil = 100d;
                Double floor = 0d;
                Double minLat = minMaxLatLon[0];
                Double maxLat = minMaxLatLon[1];
                Double minLon = minMaxLatLon[2];
                Double maxLon = minMaxLatLon[3];
                Double minSecs = 0d;
                Double maxSecs = 60d * 60d * 24d;

                Double absLat = Math.abs(o.getLatitude());
                Double absLon = Math.abs(o.getLongitude());

                Double normalizedLatitude = ((ceil - floor) * (absLat - minLat))/(maxLat - minLat) + floor;
                Double normalizedLongitude = ((ceil - floor) * (absLon - minLon))/(maxLon - minLon) + floor;

                cal.setTime(o.getTimeReported());
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                long passed = o.getTimeReported().getTime() - cal.getTimeInMillis();
                long secondsSinceMidnight = passed / 1000;

                Double normalizedSecondsSinceMidnight = ((ceil - floor) * (secondsSinceMidnight - minSecs))/(maxSecs - minSecs) + floor;
                String[] row = new String[3];
                row[0] = o.getId();
                row[1] = "100"; //latitude type
                row[2] = normalizedLatitude.toString();
                rows.add(row);

                row = new String[3];
                row[0] = o.getId();
                row[1] = "200"; //longitude type
                row[2] = normalizedLongitude.toString();
                rows.add(row);

                row = new String[3];
                row[0] = o.getId();
                row[1] = "300"; //time type
                row[2] = normalizedSecondsSinceMidnight.toString();
                rows.add(row);
            }
        }

        File f = new File(csvpath);
        f.delete();
        FileWriter fileWriter = new FileWriter(csvpath);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);
        csvWriter.writeAll(rows);
        csvWriter.flush();

        return rows.toArray(new String[][]{});
    }

    private Double[] getMinMaxLatLon(){
        String topNSql =
                "select min(inferred_latitude),\t\n" +
                        "max(inferred_latitude),\n" +
                        "min(inferred_longitude),\n" +
                        "max(inferred_longitude) \n" +
                        "from inferredlocation ";

        List<Double[]> minMaxLatLon = jdbcTemplate.query(topNSql, new RowMapper<Double[]>() {
            @Override
            public Double[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new Double[]{
                        rs.getDouble(1),
                        rs.getDouble(2),
                        rs.getDouble(3),
                        rs.getDouble(4)
                    };
                };

        });
        return minMaxLatLon.get(0);
    }

    @Autowired
    public ObservationLevelMahoutDataGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
