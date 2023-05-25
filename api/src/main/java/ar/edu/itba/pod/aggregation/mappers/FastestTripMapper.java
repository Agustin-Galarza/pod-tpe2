package ar.edu.itba.pod.aggregation.mappers;

import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Coordinate;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.Computations;
import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

import static java.time.temporal.ChronoUnit.*;

@SuppressWarnings("deprecation")

public class FastestTripMapper implements Mapper<Integer, BikeRental, Integer, Pair<Double,Integer>>, HazelcastInstanceAware {

    private HazelcastInstance hazelcastInstance;
    private final String stationsMapName;

    public FastestTripMapper(String stationsMapName){
        this.stationsMapName = stationsMapName;
    }

    private static double timeDiff(LocalDateTime startTime, LocalDateTime endTime){
        return SECONDS.between(startTime, endTime) / 3600.0;
    }

    private static double computeSpeed(Coordinate startLocation, Coordinate endLocation, LocalDateTime startTime, LocalDateTime endTime){
        return Computations.haversineDistance(startLocation, endLocation) / timeDiff(startTime, endTime);
    }

    private Coordinate getStationLocation(int id){
        return ((Station)hazelcastInstance.getMap(stationsMapName).get(id)).coordinate();
    }

    @Override
    public void map(Integer tripId, BikeRental bikeRental, Context<Integer, Pair<Double, Integer>> context) {
        if(
                bikeRental.startStationId() == bikeRental.endStationId() ||
                !hazelcastInstance.getMap(stationsMapName).containsKey(bikeRental.startStationId()) ||
                !hazelcastInstance.getMap(stationsMapName).containsKey(bikeRental.endStationId())
        ){
            return;
        }
        var tripSpeed = computeSpeed(
                getStationLocation(bikeRental.startStationId()),
                getStationLocation(bikeRental.endStationId()),
                bikeRental.startDate(),
                bikeRental.endDate()
        );
        context.emit(bikeRental.startStationId(), new Pair<>(tripSpeed, tripId));
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

}
