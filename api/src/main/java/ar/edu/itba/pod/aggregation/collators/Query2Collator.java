package ar.edu.itba.pod.aggregation.collators;

import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.StreamSupport;

@SuppressWarnings("deprecation")
public class Query2Collator implements
        Collator<Map.Entry<Integer, Pair<Double,Integer>>, List<Map.Entry<String,Pair<Double, BikeRental>>>> {
    private final String stationsMapName;
    private final String rentalsMapName;
    private final int maxStations;
    private final HazelcastInstance hazelcastInstance;


    public Query2Collator(HazelcastInstance hazelcastInstance, String stationsMapName, String rentalsMapName, int maxStations) {
        this.stationsMapName = stationsMapName;
        this.rentalsMapName = rentalsMapName;
        this.maxStations = maxStations;
        // Since the Collator is executed on the job emitting member (the client in this case) its of no use to
        // implement HazelcastInstanceAware and the instance needs to be passed here.
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public List<Map.Entry<String, Pair<Double, BikeRental>>> collate(Iterable<Map.Entry<Integer, Pair<Double, Integer>>> values) {
        final IMap<Integer, Station> stationsMap = hazelcastInstance.getMap(stationsMapName);
        final IMap<Integer, BikeRental> rentalsMap = hazelcastInstance.getMap(rentalsMapName);

        //////////////// Functions ////////////////
        final Function<Integer, String> stationsNameMapper = id -> stationsMap.get(id).name();

        final Predicate<Map.Entry<Integer, Pair<Double, Integer>>> stationIsPresent =
                tripRecord -> {
                    int statStationId = tripRecord.getKey();
                    int rentalId = tripRecord.getValue().right();
                    int endStationId = rentalsMap.get(rentalId).endStationId();
                    return stationsMap.containsKey(statStationId) && stationsMap.containsKey(endStationId);
                };
        // Searches in stations map and replaces stationId for its name
        final Function<Map.Entry<Integer, Pair<Double,Integer>>, Map.Entry<String, Pair<Double,Integer>>>
                getStationName = tripRecord -> Map.entry(stationsNameMapper.apply(tripRecord.getKey()), tripRecord.getValue());
        // Searches in rentals map and replaces tripId for its BikeRental
        final Function<Map.Entry<String, Pair<Double,Integer>> ,Map.Entry<String, Pair<Double,BikeRental>>>
                getRental = tripRecord -> Map.entry(tripRecord.getKey(), new Pair<>(tripRecord.getValue().left(), rentalsMap.get(tripRecord.getValue().right())));
        final ToDoubleFunction<Map.Entry<String, Pair<Double,Integer>>> tripSpeed =
                tripRecord -> tripRecord.getValue().left();
        final Function<Map.Entry<String, Pair<Double,Integer>>, String> alphabetical =
                Map.Entry::getKey;
        ////////////////////////////////////////////////

        // Sort values, fill missing data and limit values to the given amount
        return StreamSupport.stream(values.spliterator(), true)
                .filter(stationIsPresent)
                .map(getStationName)
                .sorted(Comparator.comparingDouble(tripSpeed).reversed().thenComparing(alphabetical))
                .limit(maxStations)
                .map(getRental)
                .toList();
    }

}
