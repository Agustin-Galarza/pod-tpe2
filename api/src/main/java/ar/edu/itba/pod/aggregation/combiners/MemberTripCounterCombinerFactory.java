package ar.edu.itba.pod.aggregation.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

@SuppressWarnings("deprecation")
public class MemberTripCounterCombinerFactory implements CombinerFactory<Integer,Integer,Integer> {
    private static class MemberTripCounterCombiner extends Combiner<Integer, Integer>{
        private int sum;
        @Override
        public void combine(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeChunk() {
            return sum;
        }

        @Override
        public void reset(){
            sum = 0;
        }
    }
    @Override
    public Combiner<Integer, Integer> newCombiner(Integer integer) {
        return null;
    }
}
