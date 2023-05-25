package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.aggregation.mappers.MemberTripCounterMapper;
import ar.edu.itba.pod.aggregation.reducers.MemberTripCounterReducerFactory;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.FnResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public class Query1Solver implements QuerySolver {
    private static final Logger logger = LoggerFactory.getLogger(Query1Solver.class);

    public String getName(){
        return "query1";
    }
    @Override
    @SuppressWarnings("deprecation")
    public FnResult solveQuery(
            HazelcastInstance hazelcastInstance,
            KeyValueSource<Integer, BikeRental> rentalsSource,
            String stationsMapName,
            String filePath
    ){
        FnResult result = FnResult.OK;

        JobCompletableFuture<Map<Integer,Integer>> jobFuture = hazelcastInstance.getJobTracker(getName())
                .newJob(rentalsSource)
                .mapper(new MemberTripCounterMapper())
                .reducer(new MemberTripCounterReducerFactory())
                .submit();

        // Sort values and write to file
        try {
            final IMap<Integer, Station> stationsMap = hazelcastInstance.getMap(stationsMapName);
            final Function<Integer, String> stationNameMapper = id -> stationsMap.get(id).name();

            final ToIntFunction<Map.Entry<String,Integer>> tripsAmount = Map.Entry::getValue;
            final Function<Map.Entry<String,Integer>, String> alphabetical = Map.Entry::getKey;
            Set<Map.Entry<Integer,Integer>> computedValues = jobFuture.get().entrySet();
            // ordenar los valores, primero por cant de viajes luego alfabético por nombre de estación
            var processedValues = computedValues.stream()
                    .parallel()
                    .filter(entry -> stationsMap.containsKey(entry.getKey()))
                    .map(entry -> Map.entry(stationNameMapper.apply(entry.getKey()), entry.getValue()))
                    .sorted(Comparator.comparingInt(tripsAmount).thenComparing(alphabetical))
                    .toList();

            writeValues(filePath, processedValues);

            //TODO: find out where to sort the values

        } catch (InterruptedException e) {
            logger.error("MapReduce computation was interrupted: ", e);
            result = FnResult.ERROR;
        } catch (ExecutionException e) {
            logger.error("MapReduce computation failed: ", e);
            result = FnResult.ERROR;
        } catch (IOException e) {
            logger.error("Error while writing to file: ", e);
            result = FnResult.ERROR;
        } catch (Exception e){
            logger.error("Unexpected exception: ", e);
            result = FnResult.ERROR;
        }
        return result;
    }

    public void writeValues(String filePath, List<Map.Entry<String, Integer>> values) throws IOException{
        final String header = "station;started_trips\n";
        final String path = String.join("/", filePath, getName() + ".csv");

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(path))){
            writer.append(header);
            for(Map.Entry<String, Integer> entry : values){
                writer.append(entry.getKey()).append(';').append(Integer.toString(entry.getValue())).append('\n');
            }
        }
    }

}
