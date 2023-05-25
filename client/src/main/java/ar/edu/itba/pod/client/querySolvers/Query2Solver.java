package ar.edu.itba.pod.client.querySolvers;

import ar.edu.itba.pod.aggregation.mappers.FastestTripMapper;
import ar.edu.itba.pod.aggregation.reducers.FastestTripReducerFactory;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.Computations;
import ar.edu.itba.pod.utils.FnResult;
import ar.edu.itba.pod.utils.Pair;
import com.hazelcast.core.HazelcastInstance;
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
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

public class Query2Solver implements QuerySolver{
    
    private static final Logger logger = LoggerFactory.getLogger(Query2Solver.class);
    private static final String DECIMAL_FORMAT = "#.##";
    private static final String DATE_FORMAT = "dd/MM/YYYY HH:mm:ss";
    private final int maxStations;
    private final String rentalsMapName;
    private IMap<Integer, Station> stationsMap; // Pueden ser campos o deberían ser locales??
    private IMap<Integer, BikeRental> rentalsMap;
    private final DecimalFormat decimalFormat;
    private final DateTimeFormatter dateFormatter;

    public Query2Solver(int n, String rentalsMapName){
            this.maxStations = n;
            this.rentalsMapName = rentalsMapName;
            this.decimalFormat = new DecimalFormat(DECIMAL_FORMAT);
            decimalFormat.setRoundingMode(RoundingMode.CEILING);
            this.dateFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
    }
    @Override
    @SuppressWarnings("deprecation")
    public FnResult solveQuery(HazelcastInstance hazelcastInstance, KeyValueSource<Integer, BikeRental> rentalsSource, String stationsMapName, String filePath) {
        FnResult result = FnResult.OK;
        if(maxStations < 0){
            logger.error("Invalid value for n: " + maxStations);
            logger.info("Invalid value for n: " + maxStations);
            return FnResult.ERROR;
        }

        var jobFuture = hazelcastInstance.getJobTracker(getName())
                .newJob(rentalsSource)
                .mapper(new FastestTripMapper(stationsMapName))
                .reducer(new FastestTripReducerFactory())
                .submit();

        try {
            //////////////// Functions and variables ////////////////
            stationsMap = hazelcastInstance.getMap(stationsMapName);
            rentalsMap = hazelcastInstance.getMap(rentalsMapName);
            final Function<Integer, String> stationsNameMapper = id -> stationsMap.get(id).name();

            final Predicate<Map.Entry<Integer, Pair<Double, Integer>>> stationIsPresent =
                    tripRecord -> {
                        int statStationId = tripRecord.getKey();
                        int rentalId = tripRecord.getValue().right();
                        int endStationId = rentalsMap.get(rentalId).endStationId();
                        return stationsMap.containsKey(statStationId) && stationsMap.containsKey(endStationId);
                    };
            // Searches in stations map and replaces stationId for its name
            final Function<Map.Entry<Integer, Pair<Double,Integer>>, Map.Entry<String, Pair<Double,Integer>>>
                    getStationName = tripRecord -> Map.entry(stationsNameMapper.apply(tripRecord.getKey()), tripRecord.getValue());
            // Searches in rentals map and replaces tripId for its BikeRental
            final Function<Map.Entry<String, Pair<Double,Integer>> ,Map.Entry<String, Pair<Double,BikeRental>>>
                    getRental = tripRecord -> Map.entry(tripRecord.getKey(), new Pair<>(tripRecord.getValue().left(), rentalsMap.get(tripRecord.getValue().right())));
            final ToDoubleFunction<Map.Entry<String, Pair<Double,Integer>>> tripSpeed =
                    tripRecord -> tripRecord.getValue().left();
            final Function<Map.Entry<String, Pair<Double,Integer>>, String> alphabetical =
                    Map.Entry::getKey;
            ////////////////////////////////////////////////

            // Wait for computation
            var computedValues = jobFuture.get().entrySet();

            // Sort values, fill missing data and limit values to the given amount
            var processedValues = computedValues.stream().parallel()
                    .filter(stationIsPresent)
                    .map(getStationName)
                    .sorted(Comparator.comparingDouble(tripSpeed).reversed().thenComparing(alphabetical))
                    .limit(maxStations)
                    .map(getRental)
                    .toList();

            writeValues(filePath, processedValues);

            //FIXME: repeated exception handling
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

    @Override
    public String getName() {
        return "query2";
    }

    public void writeValues(String filePath, List<Map.Entry<String, Pair<Double, BikeRental>>> values) throws IOException{
        final String header = "start_station;end_station;start_date;end_date;distance;speed\n";
        final String path = String.join("/", filePath, getName() + ".csv");

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
        }
    }

    private String parseDate(LocalDateTime date){
        return dateFormatter.format(date);
    }

    private String parseDoubleValue(double value){
        return this.decimalFormat.format(value);
    }
}
