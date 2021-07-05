package com.yyds.ccc.osmtlc.oshdb;

import org.locationtech.jts.geom.Coordinate;

public class G {
    String zone;
    String borough;
    int locationId;
    String type;
    Coordinate[] coordinates;

    public G() {
    }

    public G(String zone, String borough, int locationId, String type, Coordinate[] coordinates) {
        this.zone = zone;
        this.borough = borough;
        this.locationId = locationId;
        this.type = type;
        this.coordinates = coordinates;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Coordinate[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Coordinate[] coordinates) {
        this.coordinates = coordinates;
    }
}
