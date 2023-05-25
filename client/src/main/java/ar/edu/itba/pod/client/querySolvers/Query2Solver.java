package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.aggregation.collators.Query2Collator;
import ar.edu.itba.pod.aggregation.combiners.FastestTripCombinerFactory;
import ar.edu.itba.pod.aggregation.mappers.FastestTripMapper;
import ar.edu.itba.pod.aggregation.reducers.FastestTripReducerFactory;
import ar.edu.itba.pod.client.exceptions.FileWriteException;
import ar.edu.itba.pod.client.exceptions.MapReduceExecutionException;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.Computations;
import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query2Solver implements QuerySolver{
    
    private static final Logger logger = LoggerFactory.getLogger(Query2Solver.class);
    private static final String DECIMAL_FORMAT = "#.##";
    private static final String DATE_FORMAT = "dd/MM/YYYY HH:mm:ss";
    private final int maxStations;
    private IMap<Integer, Station> stationsMap; // Pueden ser campos o deberían ser locales??
    private IMap<Integer, BikeRental> rentalsMap;
    private final String name;
    private final DecimalFormat decimalFormat;
    private final DateTimeFormatter dateFormatter;

    public Query2Solver(String name, int n){
        this.name = name;
        this.maxStations = n;
        this.decimalFormat = new DecimalFormat(DECIMAL_FORMAT);
        decimalFormat.setRoundingMode(RoundingMode.CEILING);
        this.dateFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
    }
    @Override
    @SuppressWarnings("deprecation")
    public void solveQuery(HazelcastInstance hazelcastInstance,
                           Function<HazelcastInstance, IMap<Integer,BikeRental>> getRentalsMap,
                           Function<HazelcastInstance, IMap<Integer, Station>> getStationsMap,
                           String filePath) {
        if(maxStations < 0){
            throw new IllegalArgumentException("Invalid value for n: " + maxStations);
        }

        stationsMap = getStationsMap.apply(hazelcastInstance);
        rentalsMap = getRentalsMap.apply(hazelcastInstance);

        ICompletableFuture<List<Map.Entry<String,Pair<Double,BikeRental>>>> jobFuture = hazelcastInstance.getJobTracker(name)
                .newJob(KeyValueSource.fromMap(rentalsMap))
                .mapper(new FastestTripMapper(stationsMap.getName()))
                .combiner(new FastestTripCombinerFactory())
                .reducer(new FastestTripReducerFactory())
                .submit(new Query2Collator(hazelcastInstance, stationsMap.getName(), rentalsMap.getName(), maxStations));

        try {
            // Wait for computation
            List<Map.Entry<String,Pair<Double,BikeRental>>> computedValues = jobFuture.get();

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


    private void writeValues(String filePath, List<Map.Entry<String, Pair<Double, BikeRental>>> values){
        final String header = "start_station;end_station;start_date;end_date;distance;speed\n";
        final String path = String.join("/", filePath, name + ".csv");

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(path))){
            writer.append(header);
            for(Map.Entry<String, Pair<Double, BikeRental>> entry : values){
                var stationName = entry.getKey();
                var tripSpeed = entry.getValue().left();
                var rental = entry.getValue().right();
                var startLocation = stationsMap.get(rental.startStationId()).coordinate();
                var endLocation = stationsMap.get(rental.endStationId()).coordinate();

                writer.append(stationName).append(';')
                        .append(stationsMap.get(rental.endStationId()).name()).append(';')
                        .append(parseDate(rental.startDate())).append(';')
                        .append(parseDate(rental.endDate())).append(';')
                        .append(parseDoubleValue(Computations.haversineDistance(startLocation,endLocation))).append(';')
                        .append(parseDoubleValue(tripSpeed))
                        .append('\n');
            }
        } catch (IOException e){
            throw new FileWriteException(path);
        }
    }

    private String parseDate(LocalDateTime date){
        return dateFormatter.format(date);
    }

    private String parseDoubleValue(double value){
        return this.decimalFormat.format(value);
    }
}
