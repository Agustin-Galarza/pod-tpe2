package ar.edu.itba.pod.aggregation.collators;

import ar.edu.itba.pod.model.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings("deprecation")
public class Query1Collator implements Collator<Map.Entry<Integer,Integer>, List<Map.Entry<String,Integer>>> {
    private final String stationsMapName;
    private HazelcastInstance hazelcastInstance;
    public Query1Collator(HazelcastInstance hazelcastInstance, String stationsMapName){
        this.stationsMapName = stationsMapName;
        // Since the Collator is executed on the job emitting member (the client in this case) its of no use to
        // implement HazelcastInstanceAware and the instance needs to be passed here.
        this.hazelcastInstance = hazelcastInstance;
    }
    @Override
    public List<Map.Entry<String,Integer>> collate(Iterable<Map.Entry<Integer,Integer>> values) {
        final IMap<Integer, Station> stationsMap = hazelcastInstance.getMap(stationsMapName);
        final Function<Integer, String> stationNameMapper = id -> stationsMap.get(id).name();

        final ToIntFunction<Map.Entry<String,Integer>> tripsAmount = Map.Entry::getValue;
        final Function<Map.Entry<String,Integer>, String> alphabetical = Map.Entry::getKey;
        // ordenar los valores, primero por cant de viajes luego alfabético por nombre de estación
        return StreamSupport.stream(values.spliterator(), true)
                .filter(entry -> stationsMap.containsKey(entry.getKey()))
                .map(entry -> Map.entry(stationNameMapper.apply(entry.getKey()), entry.getValue()))
                .sorted(Comparator.comparingInt(tripsAmount).thenComparing(alphabetical))
                .toList();
    }

}
