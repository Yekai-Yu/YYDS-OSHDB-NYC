package com.yyds.ccc.osmtlc.oshdb;

import com.google.gson.Gson;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class POCController {

    Logger logger = LoggerFactory.getLogger(POCController.class);

    private static String DIR = "/home/ec2-user";
//    private static String DIR = "/Users/yekaiyu/Desktop/CS 598 Cloud Computing Capstone/Research Project/Main";
    private static String DB_PATH = DIR + "/us_2021-01-06.oshdb.mv.db";
    private static String GEOJSON_PATH = DIR + "/NYC Taxi Zones.geojson";

    private static Set<String> PRIMARY_FEATURES = new HashSet<>() {{
        add("amenity");
        add("aeroway");
        add("building");
        add("craft");
        add("historic");
        add("landuse");
        add("leisure");
        add("office");
        add("public_transport");
        add("shop");
        add("tourism");
    }};

    private static Set<String> EXCLUDE_FEATURES = new HashSet<>() {{
        add("yes");
        add("no");
        add("roof");
        add("ce");
        add("1ye");
        add("displaced_threshold");
        add("stable");
    }};

    private static String TAG_VAL_PATTERN = "[a-zA-Z_\\-]*";

    OSHDBH2 oshdb;

    /**
     * One timestamp, a type of tag
     */
    @RequestMapping(value = "test", method = RequestMethod.GET)
    public void poc() {
        logger.info("Hit");
        Map<String, Object> geometryMap = null;
        MapReducer<OSMEntitySnapshot> view = null;
        TagTranslator tagTranslator = null;
        List<ZoneTagData> allTimeZoneTagData = new ArrayList<>();
        try {
            logger.info("Init DB");
            oshdb = new OSHDBH2(DB_PATH);
            tagTranslator = new TagTranslator(oshdb.getConnection());
            view = OSMEntitySnapshotView.on(oshdb);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            logger.info("Parsing GEOJSON");
            String content = Files.readString(Paths.get(GEOJSON_PATH));
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(content);
            geometryMap = (Map<String, Object>) obj;
            JSONArray features = (JSONArray) geometryMap.get("features");

            MapReducer<OSMEntitySnapshot> finalView1 = view;
            TagTranslator finalTagTranslator1 = tagTranslator;
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

                // one zone, at one time
                LocalDate start = LocalDate.of(2014, 1, 1);
                LocalDate end = LocalDate.of(2020, 12, 31);
//                LocalDate end = LocalDate.of(2014, 1, 15);

                Stream<LocalDate> dates = start.datesUntil(end.plusDays(1), Period.ofMonths(1));
                List<String> dateList = dates.map(LocalDate::toString).collect(Collectors.toList());

                MapReducer<OSMEntitySnapshot> finalView = finalView1;
                TagTranslator finalTagTranslator = finalTagTranslator1;
                List<ZoneTagData> currentZone = new ArrayList<>();
                dateList.parallelStream().forEach(time -> {
                            logger.info("Processing time: {}", time);
                            ZoneTagData zoneTagData = new ZoneTagData(g.getZone(), g.getBorough(), g.getLocationId());
                            zoneTagData.setTimestamp(time);
                            try {
                                finalView.areaOfInterest(geometryJTS)
                                        .timestamps(time)
                                        .osmType(OSMType.WAY, OSMType.NODE)
                                        .forEach(snapshot -> {
                                            Iterable<OSHDBTag> tagList = snapshot.getEntity().getTags();
                                            tagList.forEach(tag -> {
                                                aggTag(tag, finalTagTranslator, zoneTagData, snapshot);
                                            });
                                        });
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            currentZone.add(zoneTagData);
                        }
                );
                allTimeZoneTagData.addAll(currentZone);
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(allTimeZoneTagData.size());
        Gson gson = new Gson();
        String outputJson = gson.toJson(allTimeZoneTagData);
        Set<OSMTag> allTimeTags = allTimeZoneTagData.stream().flatMap(zone -> zone.getTagSet().stream()).collect(Collectors.toSet());
        String tagJson = gson.toJson(allTimeTags);
        try {
            FileWriter fileWriter = new FileWriter("2014-2020-allTimeZoneTagData.json");
            fileWriter.write(outputJson);
            fileWriter.close();
        } catch (Exception e) {

        }

        try {
            FileWriter fileWriter = new FileWriter("allTimeTags.json");
            fileWriter.write(tagJson);
            fileWriter.close();
        } catch (Exception e) {

        }
    }

    private void aggTag(OSHDBTag tag, TagTranslator finalTagTranslator, ZoneTagData zoneTagData, OSMEntitySnapshot snapshot) {
        OSMTag osmTag = finalTagTranslator.getOSMTagOf(tag.getKey(), tag.getValue());
        // check tag - if char
        // many tags
        // filter excluded tag val
        boolean matcher = Pattern.matches(TAG_VAL_PATTERN, osmTag.getValue());
        if (PRIMARY_FEATURES.contains(osmTag.getKey()) && matcher && !EXCLUDE_FEATURES.contains(osmTag.getValue())) {
            zoneTagData.getTagSet().add(osmTag);

            Double area = snapshot.getGeometry().getArea();
            List<Double> areaList = zoneTagData.getFeatureMap().getOrDefault(osmTag.getValue(), new ArrayList<>());
            // ignore area=0
            if (area != 0) {
                if (areaList.isEmpty()) {
                    areaList.add(1.0);
                    areaList.add(area);
                } else {
                    areaList.set(0, areaList.get(0) + 1.0);
                    areaList.set(1, areaList.get(1) + area);
                }
                zoneTagData.getFeatureMap().put(osmTag.getValue(), areaList);
            }
        }
    }
}
