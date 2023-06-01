package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.aggregation.collators.Query1Collator;
import ar.edu.itba.pod.aggregation.mappers.MemberTripCounterMapper;
import ar.edu.itba.pod.aggregation.reducers.MemberTripCounterReducerFactory;
import ar.edu.itba.pod.client.exceptions.FileWriteException;
import ar.edu.itba.pod.client.exceptions.MapReduceExecutionException;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query1Solver implements QuerySolver {
    private static final Logger logger = LoggerFactory.getLogger(Query1Solver.class);
    private final String name;
    public Query1Solver(String name){
        this.name = name;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void solveQuery(
            HazelcastInstance hazelcastInstance,
            Function<HazelcastInstance, IMap<Integer,BikeRental>> getRentalsMap,
            Function<HazelcastInstance, IMap<Integer, Station>> getStationsMap,
            String filePath
    ){
        //TODO: change this to an abstract class
        var stationsMap = getStationsMap.apply(hazelcastInstance);
        var rentalsMap = getRentalsMap.apply(hazelcastInstance);

        JobCompletableFuture<List<Map.Entry<String,Integer>>> jobFuture = hazelcastInstance.getJobTracker(name)
                .newJob(KeyValueSource.fromMap(rentalsMap))
                .mapper(new MemberTripCounterMapper())
                .reducer(new MemberTripCounterReducerFactory())
                .submit(new Query1Collator(hazelcastInstance, stationsMap.getName()));

        // Sort values and write to file
        try {
            List<Map.Entry<String,Integer>> computedValues = jobFuture.get();

            writeValues(filePath, computedValues);
        } catch (InterruptedException e) {
            throw new MapReduceExecutionException("Job was interrupted (" + e.getCause() + ")");
        } catch (ExecutionException e) {
            throw new MapReduceExecutionException("MapReduce computation was aborted (" + e.getCause() + ")");
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public void writeValues(String filePath, Collection<Map.Entry<String, Integer>> values) {
        final String header = "station;started_trips\n";
        final String path = String.join("/", filePath, name + ".csv");

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(path))){
            writer.append(header);
            for(Map.Entry<String, Integer> entry : values){
                writer.append(entry.getKey()).append(';').append(Integer.toString(entry.getValue())).append('\n');
            }
        } catch (IOException e){
            throw new FileWriteException(path);
        }
    }

}
