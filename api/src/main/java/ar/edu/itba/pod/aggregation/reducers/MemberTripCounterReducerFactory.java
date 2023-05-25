package ar.edu.itba.pod.aggregation.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@SuppressWarnings("deprecation")
public class MemberTripCounterReducerFactory implements ReducerFactory<Integer, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(Integer integer) {
        return new MemberTripCounterReducer();
    }

    private static class MemberTripCounterReducer extends Reducer<Integer, Integer> {
        private int sum;

        @Override
        public void beginReduce(){
            sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }


}
