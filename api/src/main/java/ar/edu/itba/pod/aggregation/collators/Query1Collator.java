package ar.edu.itba.pod.aggregation.collators;

import ar.edu.itba.pod.model.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

@SuppressWarnings("deprecation")
public class Query1Collator implements Collator<Map.Entry<Integer,Integer>, List<Map.Entry<String,Integer>>> {
    private final IMap<Integer, Station> stationsMap;
    public Query1Collator(HazelcastInstance hazelcastInstance, String stationsMapName){
        // Since the Collator is executed on the job emitting member (the client in this case) its of no use to
        // implement HazelcastInstanceAware and the instance needs to be passed here.
        this.stationsMap = hazelcastInstance.getMap(stationsMapName);
    }

    private String stationNameMapper(int id){
        return stationsMap.get(id).name();
    }
    private int tripsAmount(Map.Entry<String,Integer> entry){
        return entry.getValue();
    }

    private String alphabetical(Map.Entry<String,Integer> entry){
        return entry.getKey().toLowerCase();
    }

    @Override
    public List<Map.Entry<String,Integer>> collate(Iterable<Map.Entry<Integer,Integer>> values) {
        // ordenar los valores, primero por cant de viajes luego alfabético por nombre de estación
        return StreamSupport.stream(values.spliterator(), true)
                .filter(entry -> stationsMap.containsKey(entry.getKey()))
                .map(entry -> Map.entry(stationNameMapper(entry.getKey()), entry.getValue()))
                .sorted(Comparator.comparingInt(this::tripsAmount).reversed().thenComparing(this::alphabetical))
                .toList();
    }

}
