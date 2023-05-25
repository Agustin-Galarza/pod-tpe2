package ar.edu.itba.pod.aggregation.mappers;

import ar.edu.itba.pod.model.BikeRental;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class MemberTripCounterMapper implements Mapper<Integer, BikeRental, Integer, Integer> {
    private static final Integer ONE = 1;
    @Override
    public void map(Integer s, BikeRental bikeRental, Context<Integer, Integer> context) {
        if(bikeRental.isMember())
            context.emit(bikeRental.startStationId(), ONE);
    }
}
