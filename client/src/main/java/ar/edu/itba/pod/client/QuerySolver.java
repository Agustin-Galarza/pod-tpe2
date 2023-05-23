package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.BikeRental;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

public interface QuerySolver {
    @SuppressWarnings("deprecation")
    void solveQuery(JobTracker jobTracker, KeyValueSource<String, BikeRental> rentalsSource, String stationsMapName);
}
