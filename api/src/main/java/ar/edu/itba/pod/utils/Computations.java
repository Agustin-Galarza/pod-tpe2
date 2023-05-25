package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.Coordinate;

public class Computations {
    public static double haversineDistance(Coordinate location1, Coordinate location2){
        double lat1 = location1.latitude(), lon1 = location1.longitude();
        double lat2 = location2.latitude(), lon2 = location2.longitude();

        // distance between latitudes and longitudes
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        // convert to radians
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        // apply formulae
        double a = Math.pow(Math.sin(dLat / 2), 2) +
                Math.pow(Math.sin(dLon / 2), 2) *
                        Math.cos(lat1) *
                        Math.cos(lat2);
        double rad = 6371;
        double c = 2 * Math.asin(Math.sqrt(a));
        return rad * c;
    }
}
