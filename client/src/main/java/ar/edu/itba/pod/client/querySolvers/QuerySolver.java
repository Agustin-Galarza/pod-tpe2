package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.client.exceptions.MapReduceExecutionException;
import ar.edu.itba.pod.client.exceptions.FileWriteException;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.function.Function;

public interface QuerySolver {
    /**
     * Creates and executes a MapReduce job and then uploads the results to a csv file.
     * @param hazelcastInstance the Hazelcast client to create the job.
     * @param getRentalsMap a function to get the rentals map from the Hazelcast client
     * @param getStationsMap a function to get the stations map from the Hazelcast client
     * @param filePath the path to the directory in which the resulting csv file will be created.
     *                 The name of the csv file will be created by appending the name of the solver
     *                 (obtained from the {@link  ar.edu.itba.pod.client.querySolvers.QuerySolver#getName()} method)
     *                 with '.csv'.
     * @throws MapReduceExecutionException if there is any problem while executing the job
     * @throws FileWriteException if there is any problem while writing into the csv file
     */
    void solveQuery(HazelcastInstance hazelcastInstance,
                        Function<HazelcastInstance, IMap<Integer,BikeRental>> getRentalsMap,
                        Function<HazelcastInstance, IMap<Integer, Station>> getStationsMap,
                        String filePath);

    String getName();
}
