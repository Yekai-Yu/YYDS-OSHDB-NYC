package com.yyds.ccc.osmtlc.jts;

import com.yyds.ccc.osmtlc.oshdb.G;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinateLocator {
    Logger logger = LoggerFactory.getLogger(CoordinateLocator.class);

    protected static String DIR = "/home/ec2-user";
//    private static String DIR = "/Users/yekaiyu/Desktop/CS 598 Cloud Computing Capstone/Research Project/Main";
    protected static String GEOJSON_PATH = DIR + "/NYC Taxi Zones.geojson";

    static String PICK_UP_LONG = "pickup_longitude";
    static String PICK_UP_LAT = "pickup_latitude";
    static String DROP_OFF_LONG = "dropoff_longitude";
    static String DROP_OFF_LAT = "dropoff_latitude";

    Map<String, Object> geometryMap = new HashMap<>();
    Map<Polygon, Integer> zoneMap = new HashMap<>();

    public CoordinateLocator() throws Exception {
        logger.info("Parsing GEOJSON...");
        String content = Files.readString(Paths.get(GEOJSON_PATH));
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(content);
        geometryMap = (Map<String, Object>) obj;
        JSONArray features = (JSONArray) geometryMap.get("features");

        features.parallelStream().forEach(featureObj -> {
            JSONObject feature = (JSONObject) featureObj;
            JSONObject properties = (JSONObject) feature.get("properties");
            JSONObject geometry = (JSONObject) feature.get("geometry");
            JSONArray coord = (JSONArray) geometry.get("coordinates");
            JSONArray co = (JSONArray) ((JSONArray) coord.get(0)).get(0);
            List<Coordinate> coList = new ArrayList<>();
            for (Object c : co) {
                JSONArray cc = (JSONArray) c;
                coList.add(new Coordinate((Double) cc.get(0), (Double) cc.get(1)));
            }
            Coordinate[] cArr = new Coordinate[coList.size()];
            G g = new G((String) properties.get("zone"),
                    (String) properties.get("borough"),
                    Integer.parseInt((String) properties.get("location_id")),
                    (String) geometry.get("type"),
                    coList.toArray(cArr));
            logger.info("Processing zone: {}", g.getZone());
            GeometryFactory geoFactory = new GeometryFactory();
            Polygon geometryJTS = geoFactory.createPolygon(g.getCoordinates());
            zoneMap.put(geometryJTS, g.getLocationId());
        });
    }

    public void process(File csvFile) throws Exception {
        Reader in = new FileReader(csvFile);
        Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withHeader(
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "pickup_longitude",
                "pickup_latitude",
                "dropoff_longitude",
                "dropoff_latitude",
                "fare_amount",
                "surcharge",
                "tolls_amount",
                "total_amount").parse(in);
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(csvFile.getAbsolutePath().substring(0, csvFile.getAbsolutePath().length() - ".csv".length()) + "Converted.csv"), CSVFormat.DEFAULT)) {
            printer.printRecord(
                    "pickup_datetime",
                    "dropoff_datetime",
                    "passenger_count",
                    "trip_distance",
                    "PULocationID",
                    "DOLocationID",
                    "fare_amount",
                    "surcharge",
                    "tolls_amount",
                    "total_amount");
            boolean skipHeader = true;
            int totalCounter = 0;
            int processedCounter = 0;
            for (CSVRecord record : csvRecords) {
                if (skipHeader) {
                    skipHeader = false;
                    continue;
                }
                ++totalCounter;
                try {
                    Double pickUpLong = Double.parseDouble(record.get(PICK_UP_LONG));
                    Double pickUpLat = Double.parseDouble(record.get(PICK_UP_LAT));
                    Double dropOffLong = Double.parseDouble(record.get(DROP_OFF_LONG));
                    Double dropOffLat = Double.parseDouble(record.get(DROP_OFF_LAT));


                    Coordinate pickUp = new Coordinate(pickUpLong, pickUpLat);
                    Coordinate dropOff = new Coordinate(dropOffLong, dropOffLat);

                    GeometryFactory geometryFactory1 = new GeometryFactory();
                    Point pickUpPoint = geometryFactory1.createPoint(pickUp);
                    GeometryFactory geometryFactory2 = new GeometryFactory();
                    Point dropOffPoint = geometryFactory2.createPoint(dropOff);

                    int pickUpZone = -1;
                    int dropOffZone = -1;
                    boolean foundP = false;
                    boolean foundD = false;
                    for (Polygon zone : zoneMap.keySet()) {
                        if (!foundP && pickUpPoint.within(zone)) {
                            pickUpZone = zoneMap.get(zone);
                            foundP = true;
                        }
                        if (!foundD && dropOffPoint.within(zone)) {
                            dropOffZone = zoneMap.get(zone);
                            foundD = true;
                        }
                        if (foundP && foundD) {
                            break;
                        }
                    }
                    if (pickUpZone == -1 || dropOffZone == -1) {
                        continue;
                    }
                    printer.printRecord(
                            record.get("pickup_datetime"),
                            record.get("dropoff_datetime"),
                            record.get("passenger_count"),
                            record.get("trip_distance"),
                            pickUpZone,
                            dropOffZone,
                            record.get("fare_amount"),
                            record.get("surcharge"),
                            record.get("tolls_amount"),
                            record.get("total_amount"));
                    ++processedCounter;
                } catch (Exception e) {
//                    logger.info("Error record, moving on...", e);
                }
            }
            logger.info("Total records: {}, processed records: {}", totalCounter, processedCounter);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
