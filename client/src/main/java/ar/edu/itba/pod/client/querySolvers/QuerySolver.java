package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.utils.FnResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

public interface QuerySolver {
    @SuppressWarnings("deprecation")
    FnResult solveQuery(HazelcastInstance hazelcastInstance,
                        KeyValueSource<Integer, BikeRental> rentalsSource,
                        String stationsMapName,
                        String filePath);

    String getName();
}
