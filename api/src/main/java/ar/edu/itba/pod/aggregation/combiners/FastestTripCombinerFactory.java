package ar.edu.itba.pod.aggregation.combiners;

import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

@SuppressWarnings("deprecation")
public class FastestTripCombinerFactory implements CombinerFactory<Integer, Pair<Double, Integer>, Pair<Double, Integer>> {

    private static class FastestTripCombiner extends Combiner<Pair<Double, Integer>, Pair<Double, Integer>>{

        private Pair<Double, Integer> fastestTrip;

        private boolean isFaster(Pair<Double, Integer> trip){
            return Double.compare(trip.left(), fastestTrip.left()) > 0;
        }

        @Override
        public void combine(Pair<Double, Integer> trip) {
            if(fastestTrip == null){
                fastestTrip = trip;
            } else if(isFaster(trip)){
                fastestTrip = trip;
            }
        }

        @Override
        public Pair<Double, Integer> finalizeChunk() {
            return fastestTrip;
        }

        @Override
        public void reset(){
            fastestTrip = null;
        }
    }
    @Override
    public Combiner<Pair<Double, Integer>, Pair<Double, Integer>> newCombiner(Integer integer) {
        return new FastestTripCombiner();
    }
}
