package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.BikeRental;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.concurrent.ExecutionException;

public class Query1Solver implements QuerySolver{
    @Override
    @SuppressWarnings("deprecation")
    public void solveQuery(JobTracker jobTracker, KeyValueSource<String, BikeRental> rentalsSource, String stationsMapName){
        //TODO
        var future = jobTracker.newJob(rentalsSource).mapper(null).reducer(null).submit();

        try {
            future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
