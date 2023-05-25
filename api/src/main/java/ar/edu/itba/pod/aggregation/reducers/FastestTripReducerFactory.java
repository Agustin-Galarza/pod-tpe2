package ar.edu.itba.pod.aggregation.reducers;

import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class FastestTripReducerFactory implements ReducerFactory<Integer, Pair<Double, Integer>, Pair<Double, Integer>> {

    private static class FastestTripReducer extends Reducer<Pair<Double, Integer>, Pair<Double, Integer>>{
        private Pair<Double, Integer> fastestTrip;

        private boolean isFaster(Pair<Double, Integer> trip){
            return Double.compare(trip.left(), fastestTrip.left()) > 0;
        }

        @Override
        public void beginReduce(){
            fastestTrip = new Pair<>(-1.0, -1);
        }
        @Override
        public void reduce(Pair<Double, Integer> trip) {
            if(isFaster(trip)){
                fastestTrip = new Pair<>(trip.left(), trip.right());
            }
        }

        @Override
        public Pair<Double, Integer> finalizeReduce() {
            return fastestTrip;
        }
    }
    @Override
    public Reducer<Pair<Double, Integer>, Pair<Double, Integer>> newReducer(Integer integer) {
        return new FastestTripReducer();
    }
}
