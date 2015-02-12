package org.onebusaway.prediction.util;

import org.apache.log4j.Logger;
import org.geotools.referencing.GeodeticCalculator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.awt.geom.Point2D;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

@Component
public class GTFSImporter {
    private final JdbcTemplate jdbcTemplate;

    private Logger log = Logger.getLogger(GTFSImporter.class);

        private Map<String, List<String>> shapes;
        private Map<String, String> tripsToShapes;
        private Map<String, String> stops;
        private Map<String, List<String>> tripsToStops;
        private Map<String, List<Double>> tripsToStopDistances;
        private GeodeticCalculator calc;

    public void importGtfs(String root) throws Throwable {
        shapes = new HashMap<>();
        tripsToShapes = new HashMap<>();
        stops = new HashMap<>();
        tripsToStops = new HashMap<>();
        tripsToStopDistances = new HashMap<>();
        calc = new GeodeticCalculator();


        File dir = new File(root);
        List<File> gtfsZipFiles = new ArrayList<>();
        getSubdirs(dir, gtfsZipFiles);
        for(File f : gtfsZipFiles){
            handleFile(f);
        }

        //all the data from the files is loaded, construct the routes and persist them
        calculateRouteVersions();
    }

    private void calculateRouteVersions(){
        Map<String, List<String>> stopListsToTripIds = new HashMap<>();
        Map<String, String> stopListsToDistances = new HashMap<>();

        int i = 0;
        for(String tripId : tripsToStops.keySet()) {
            String shapeId = tripsToShapes.get(tripId);
            Collection<String> stops = tripsToStops.get(tripId);

            StringJoiner stopJoiner = new StringJoiner(",");
            for(String stopId : stops){
                stopJoiner.add(stopId);
            }

            String stopListKey = shapeId + "|" + stopJoiner.toString();

            if(!stopListsToTripIds.keySet().contains(stopListKey)){
                stopListsToTripIds.put(stopListKey, new ArrayList<>());
            }
            stopListsToTripIds.get(stopListKey).add(tripId);

            if(!stopListsToDistances.keySet().contains(stopListKey)){

                Collection<Double> distanceList = getDistancesForTripId(tripId);

                StringJoiner distanceJoiner = new StringJoiner(",");
                for(Double distance : distanceList){
                    distanceJoiner.add(distance.toString());
                }
                String distanceListString = distanceJoiner.toString();

                stopListsToDistances.put(stopListKey, distanceListString);
            }
            i++;
            if(i % 1000 == 0){
                log.warn("processed " + i + " trips out of " + tripsToStops.keySet().size());
            }
        }

        for(String stopListKey : stopListsToDistances.keySet()){
            String[] elements = stopListKey.split("\\|");
            final String shapeId = elements[0];
            final String stops = elements[1];
            final String distances = stopListsToDistances.get(stopListKey);
            final String INSERT_SQL =  "INSERT INTO routes (" +
                    "	stops," +
                    "   distances, " +
                    "   shapeid)" +
                    "VALUES (?, ?, ?)";

            final String name = "Rob";
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(
                    new PreparedStatementCreator() {
                        public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                            PreparedStatement ps =
                                    connection.prepareStatement(INSERT_SQL, new String[] {"id"});
                            ps.setString(1, stops);
                            ps.setString(2, distances);
                            ps.setString(3, shapeId);
                            return ps;
                        }
                    },
                    keyHolder);
            Number id = keyHolder.getKey();
            for(String tripId : stopListsToTripIds.get(stopListKey)){
                    String insertSql = "INSERT INTO routes_trips (" +
                    "	tripid, " +
                    "	routeid) " +
                    "VALUES (?, ?)";

                Object[] params = new Object[] { tripId, id};
                int[] types = new int[] { Types.VARCHAR, Types.INTEGER};
                jdbcTemplate.update(insertSql, params, types);
            }
        }
    }

    private Collection<Double> getDistancesForTripId(String tripId){

        String shapeId = tripsToShapes.get(tripId);
        List<String> points = shapes.get(shapeId);
        Collection<Double> distanceList = new ArrayList<>();
        for(String stopId : tripsToStops.get(tripId)){
            String[] stopCoords = stops.get(stopId).split(",");
            Point2D stopPoint = new Point2D.Double(Double.parseDouble(stopCoords[0]), Double.parseDouble(stopCoords[1]));
            String prevPoint = null;
            Double minDist = Double.MAX_VALUE;
            Point2D minDistPoint = null;
            int minDistIndex = -1;
            int i = 0;
            for(String point : points){
                if(prevPoint != null && !prevPoint.equals(point)){  //duplicate points in gtfs file for some reason, skip them;
                    //on the current line segment, what's the closest point to the stop?
                    String[] segmentStartCoords = prevPoint.split(",");
                    String[] segmentEndCoords = point.split(",");
                    Point2D segmentStartPoint = new Point2D.Double(Double.parseDouble(segmentStartCoords[0]), Double.parseDouble(segmentStartCoords[1]));
                    Point2D segmentEndPoint = new Point2D.Double(Double.parseDouble(segmentEndCoords[0]), Double.parseDouble(segmentEndCoords[1]));
                    Point2D closestPoint = getClosestPointOnSegment(segmentStartPoint, segmentEndPoint, stopPoint);
                    Double distance = closestPoint.distance(stopPoint);
                    if(distance < minDist){
                        minDist = distance;
                        minDistPoint = closestPoint;
                        minDistIndex = i;
                    }
                }
                prevPoint = point;
                i++;
            }

            //we now have the closest point on the shape to the stop, get that point's cumulative distance along the shape.
            prevPoint = null;
            Double cumulativeDistance = 0d;
            for(i =0; i < minDistIndex; i++){
                String point = points.get(i);
                if(prevPoint != null && !prevPoint.equals(point)){
                    String[] segmentStartCoords = prevPoint.split(",");
                    String[] segmentEndCoords = point.split(",");
                    Point2D segmentStartPoint = new Point2D.Double(Double.parseDouble(segmentStartCoords[0]), Double.parseDouble(segmentStartCoords[1]));
                    Point2D segmentEndPoint = new Point2D.Double(Double.parseDouble(segmentEndCoords[0]), Double.parseDouble(segmentEndCoords[1]));
                    calc.setStartingGeographicPoint(segmentStartPoint);
                    calc.setDestinationGeographicPoint(segmentEndPoint);
                    cumulativeDistance += calc.getOrthodromicDistance();
                }
                prevPoint = point;
            }

            String[] segmentStartCoords = prevPoint.split(",");
            calc.setStartingGeographicPoint(new Point2D.Double(Double.parseDouble(segmentStartCoords[0]), Double.parseDouble(segmentStartCoords[1])));
            calc.setDestinationGeographicPoint(minDistPoint);

            //now add on the last bit of distance from the end point of the prior segment to the tangent point of the stop
            cumulativeDistance += calc.getOrthodromicDistance();
            distanceList.add(cumulativeDistance);
        }
        return distanceList;
    }

    private void handleFile(File file) throws Throwable {
        ZipInputStream zis = null;
        InputStream is = null;
        try {
            ZipFile zipFile = new ZipFile(file);
            for (Enumeration e = zipFile.entries(); e.hasMoreElements(); ) {
                ZipEntry ze = (ZipEntry) e.nextElement();
                String filename = ze.getName();
                BufferedReader br = new BufferedReader(new InputStreamReader(zipFile.getInputStream(ze)));
                String line;
                int i = 0;
                while ((line = br.readLine()) != null) {
                    if (i == 0) {
                        i++;
                        continue;
                    }
                    switch (filename) {
                        case "shapes.txt":
                            handleShape(line);
                            break;
                        case "stop_times.txt":
                            handleTime(line);
                            break;
                        case "stops.txt":
                            handleStop(line);
                            break;
                        case "trips.txt":
                            handleTrip(line);
                            break;
                        default:
                            continue;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null)
                is.close();
            if (zis != null)
                zis.close();
        }

    }


    private void handleShape(String route){
        String[] elements = route.split(",");
        String shapeId = elements[0];
        if(!shapes.keySet().contains(shapeId)){
            shapes.put(shapeId, new ArrayList<>());
        }
        String lat = elements[2];
        String lon = elements[3];
        shapes.get(shapeId).add(lat + "," + lon);
    }

    private void handleTrip(String route){
        String[] elements = route.split(",");
        String tripId = elements[1];
        String shapeId = elements[5];
        tripsToShapes.put(tripId, shapeId);
    }

    private void handleStop(String route){
        String[] elements = route.split(",");
        String stopId = elements[0];
        String lat = elements[2];
        String lon = elements[3];
        stops.put(stopId, lat +","+lon);
    }

    private void handleTime(String route){
        String[] elements = route.split(",");
        String tripId = elements[0];
        if(!tripsToStops.keySet().contains(tripId)){
            tripsToStops.put(tripId, new ArrayList<>());
            tripsToStopDistances.put(tripId, new ArrayList<>());
        }
        String stopId = elements[1];
        tripsToStops.get(tripId).add(stopId);
    }

    List<File> getSubdirs(File file, List<File> files) {

        List<File> filesToAdd = Arrays.asList(file.listFiles(new FileFilter() {
            public boolean accept(File f) {
                if(f.getName().endsWith(".zip") && f.getName().startsWith("google_transit_")){
                    return true;
                }
                return false;
            }
        }));

        files.addAll(filesToAdd);

        List<File> subdirs = Arrays.asList(file.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory();
            }
        }));
        subdirs = new ArrayList<>(subdirs);

        List<File> deepSubdirs = new ArrayList<>();
        for(File subdir : subdirs) {
            deepSubdirs.addAll(getSubdirs(subdir, files));
        }
        subdirs.addAll(deepSubdirs);
        return subdirs;
    }

    private Point2D getClosestPointOnSegment(Point2D ss, Point2D se, Point2D p)
    {
        return getClosestPointOnSegment(ss.getX(), ss.getY(), se.getX(), se.getY(), p.getX(), p.getY());
    }

    private Point2D getClosestPointOnSegment(double sx1, double sy1, double sx2, double sy2, double px, double py)
    {
        double xDelta = sx2 - sx1;
        double yDelta = sy2 - sy1;

        if ((xDelta == 0) && (yDelta == 0))
        {
            throw new IllegalArgumentException("Segment start equals segment end");
        }

        double u = ((px - sx1) * xDelta + (py - sy1) * yDelta) / (xDelta * xDelta + yDelta * yDelta);

        final Point2D closestPoint;
        if (u < 0)
        {
            closestPoint = new Point2D.Double(sx1, sy1);
        }
        else if (u > 1)
        {
            closestPoint = new Point2D.Double(sx2, sy2);
        }
        else
        {
            closestPoint = new Point2D.Double(sx1 + u * xDelta, sy1 + u * yDelta);
        }

        return closestPoint;
    }

    @Autowired
    public GTFSImporter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


}
