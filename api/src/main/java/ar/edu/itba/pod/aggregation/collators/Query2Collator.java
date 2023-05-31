package ar.edu.itba.pod.aggregation.collators;

import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

@SuppressWarnings("deprecation")
public class Query2Collator implements
        Collator<Map.Entry<Integer, Pair<Double, Integer>>, List<Map.Entry<String, Pair<Double, BikeRental>>>> {
    private final String stationsMapName;
    private final String rentalsMapName;
    private final int maxStations;
    private final HazelcastInstance hazelcastInstance;
    private final IMap<Integer, Station> stationsMap;
    private final IMap<Integer, BikeRental> rentalsMap;

    public Query2Collator(HazelcastInstance hazelcastInstance, String stationsMapName, String rentalsMapName, int maxStations) {
        this.stationsMapName = stationsMapName;
        this.rentalsMapName = rentalsMapName;
        this.maxStations = maxStations;
        // Since the Collator is executed on the job emitting member (the client in this case) its of no use to
        // implement HazelcastInstanceAware and the instance needs to be passed here.
        this.hazelcastInstance = hazelcastInstance;
        this.stationsMap = hazelcastInstance.getMap(stationsMapName);
        this.rentalsMap = hazelcastInstance.getMap(rentalsMapName);
    }

    private String stationsNameMapper(int id) {
        return stationsMap.get(id).name();
    }

    private boolean stationIsPresent(Map.Entry<Integer, Pair<Double, Integer>> tripRecord) {
        int statStationId = tripRecord.getKey();
        int rentalId = tripRecord.getValue().right();
        int endStationId = rentalsMap.get(rentalId).endStationId();
        return stationsMap.containsKey(statStationId) && stationsMap.containsKey(endStationId);
    }

    // Searches in stations map and replaces stationId for its name
    private Map.Entry<String, Pair<Double, Integer>> getStationName(Map.Entry<Integer, Pair<Double, Integer>> tripRecord) {
        return Map.entry(stationsNameMapper(tripRecord.getKey()), tripRecord.getValue());
    }

    private double tripSpeed(Map.Entry<String, Pair<Double, Integer>> tripRecord) {
        return tripRecord.getValue().left();
    }

    private String alphabetical(Map.Entry<String, Pair<Double, Integer>> entry) {
        return entry.getKey();
    }

    // Searches in rentals map and replaces tripId for its BikeRental
    private Map.Entry<String, Pair<Double, BikeRental>> getRental(Map.Entry<String, Pair<Double, Integer>> tripRecord) {
        return Map.entry(tripRecord.getKey(), new Pair<>(tripRecord.getValue().left(), rentalsMap.get(tripRecord.getValue().right())));
    }

    @Override
    public List<Map.Entry<String, Pair<Double, BikeRental>>> collate(
            Iterable<Map.Entry<Integer, Pair<Double, Integer>>> values) {
        return StreamSupport.stream(values.spliterator(), true)
                .filter(this::stationIsPresent)
                .map(this::getStationName)
                .sorted(Comparator.comparingDouble(this::tripSpeed).reversed().thenComparing(this::alphabetical))
                .limit(maxStations)
                .map(this::getRental)
                .toList();
    }

}
