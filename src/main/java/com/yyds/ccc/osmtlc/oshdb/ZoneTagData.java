package com.yyds.ccc.osmtlc.oshdb;

import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ZoneTagData {
    String timestamp;
    String zoneId;
    String borough;
    int locationId;
    // each amenity as key
    // value: total number, total area of the tag
    Map<String, List<Double>> featureMap = new HashMap<>();
    // overall tags
    Set<OSMTag> tagSet = new HashSet<>();

    public ZoneTagData() {
    }

    public ZoneTagData(String zoneId, String borough, int locationId) {
        this.zoneId = zoneId;
        this.borough = borough;
        this.locationId = locationId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public String getBorough() {
        return borough;
    }

    public void setBorough(String borough) {
        this.borough = borough;
    }

    public int getLocationId() {
        return locationId;
    }

    public void setLocationId(int locationId) {
        this.locationId = locationId;
    }

    public Map<String, List<Double>> getFeatureMap() {
        return featureMap;
    }

    public void setFeatureMap(Map<String, List<Double>> featureMap) {
        this.featureMap = featureMap;
    }

    public Set<OSMTag> getTagSet() {
        return tagSet;
    }

    public void setTagSet(Set<OSMTag> tagSet) {
        this.tagSet = tagSet;
    }
}
