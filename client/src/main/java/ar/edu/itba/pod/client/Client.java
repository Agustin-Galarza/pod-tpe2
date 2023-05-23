package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.utils.CSVReader;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Coordinate;
import ar.edu.itba.pod.model.Station;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.KeyValueSource;
import io.github.cdimascio.dotenv.Dotenv;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Client implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static final Logger performanceLogger = LoggerFactory.getLogger("performance");

    private HazelcastInstance hazelcastInstance;
    private final QuerySolver solver;
    private String inPath;
    private String outPath;

    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";

    private static final String ADDRESSES_PROPERTY_NAME = "addresses";
    private static final String INPATH_PROPERTY_NAME = "inPath";
    private static final String OUTPATH_PROPERTY_NAME = "outPath";

    private static final String STATIONS_MAP_NAME = "stations";
    private static final String RENTALS_MAP_NAME = "rentals";
    private static final String STATIONS_CSV_FILENAME = "stations.csv";
    private static final String RENTALS_CSV_FILENAME = "bikes.csv";

    private static final Map<String, Class<? extends QuerySolver>> solvers = new HashMap<>(){{
       put("query1", Query1Solver.class);
    }};

    public Client(@NotNull QuerySolver solver){
        this.solver = solver;
    }

    public void startClient(@NotNull String[] nodeAddresses,@NotNull String inPath,@NotNull String outPath) throws IOException{
        logger.info("Starting client ...");
        hazelcastInstance = HazelcastClient.newHazelcastClient(configClient(nodeAddresses));
        this.inPath = inPath;
        this.outPath = outPath;

        performanceLogger.info("Inicio de la lectura de los archivos.");
        uploadRentals();
        readStations();
        performanceLogger.info("Fin de la lectura de los archivos.");
    }
    public ClientConfig configClient(String[] nodeAddresses){
        Dotenv dotenv = Dotenv.load();
        ClientConfig config = new ClientConfig();

        // Group Config
        GroupConfig groupConfig = new GroupConfig()
                .setName(dotenv.get(GROUP_NAME_ENV, GROUP_NAME_DEFAULT))
                .setPassword(dotenv.get(GROUP_PASSWORD_ENV, GROUP_PASSWORD_DEFAULT));
        config.setGroupConfig(groupConfig);
        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();

        clientNetworkConfig.addAddress(nodeAddresses);

        config.setNetworkConfig(clientNetworkConfig);

        return config;
    }
    public void shutdown(){
        logger.info("Shutting down client ...");
        hazelcastInstance.shutdown();
    }

    public void uploadRentals() throws IOException{
        /*
         * ○ start_date: Fecha y hora del alquiler de la bicicleta (inicio del viaje) en formato
         * yyyy-MM-dd HH:mm:ss
         * ○ emplacement_pk_start: Identificador de la estación de inicio (número entero)
         * ○ end_date: Fecha y hora de la devolución de la bicicleta (fin del viaje) en formato
         * yyyy-MM-dd HH:mm:ss
         * ○ emplacement_pk_end: Identificador de la estación de fin (número entero)
         * ○ is_member: Si el usuario del alquiler es miembro del sistema de alquiler (0 si no es
         * miembro, 1 si lo es)
         */
        ConcurrentMap<String, BikeRental> rentalsMap = getRentalsMap();
        final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

        var reader = new CSVReader<BikeRental>(
                "./bikes.csv",
                Arrays.asList(
                        new CSVReader.CSVColumn<>("start_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<>("emplacement_pk_start", Integer::parseInt),
                        new CSVReader.CSVColumn<>("end_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<>("emplacement_pk_end", Integer::parseInt),
                        new CSVReader.CSVColumn<>("is_member", value -> value.equals("1"))
                )
        );
        AtomicInteger counter = new AtomicInteger();
        try {
            reader.processItems(
                    values -> {
                        var rental = new BikeRental(
                                (int) values.get("emplacement_pk_start"),
                                (LocalDateTime) values.get("start_date"),
                                (int) values.get("emplacement_pk_end"),
                                (LocalDateTime) values.get("end_date"),
                                (boolean) values.get("is_member")
                        );
                        rentalsMap.put(Integer.toString(counter.getAndIncrement()), rental);
                    }
            );
        } catch (IOException e) {
            //FIXME: throwing and logging exceptions twice
            logger.error("Could not read from stations file: ", e);
            throw e;
        }

    }

    public void readStations() throws IOException{
        /*
         * ○ pk: Identificador de la estación (número entero)
         * ○ name: Nombre de la estación (cadena de caracteres)
         * ○ latitude: Latitud de la ubicación de la estación (número real)
         * ○ longitude: Longitud de la ubicación de la estación (número real)
         */
        ConcurrentMap<String, Station> stationsMap = getStationsMap();

        CSVReader<Station> reader = new CSVReader<>(
                "./stations.csv",
                Arrays.asList(
                        new CSVReader.CSVColumn<>("pk", Integer::parseInt),
                        new CSVReader.CSVColumn<>("name", s -> s),
                        new CSVReader.CSVColumn<>("latitude", Double::parseDouble),
                        new CSVReader.CSVColumn<>("longitude", Double::parseDouble)
                )
        );
        try {
            reader.processItems(
                    values -> {
                        var station = new Station(
                                (int) values.get("pk"),
                                (String) values.get("name"),
                                new Coordinate((double) values.get("latitude"), (double) values.get("longitude"))
                        );
                        stationsMap.put(station.name(), station);
                    }
            );
        } catch (IOException e) {
            logger.error("Could not read from stations file: ", e);
            throw e;
        }
    }
    @SuppressWarnings({"deprecated", "deprecation"})
    public void solveQuery(){
        solver.solveQuery(hazelcastInstance.getJobTracker("query-tracker"), KeyValueSource.fromMap(hazelcastInstance.getMap(RENTALS_MAP_NAME)), STATIONS_MAP_NAME);
    }

    public ConcurrentMap<String, Station> getStationsMap(){
        return hazelcastInstance.getMap(STATIONS_MAP_NAME);
    }

    public ConcurrentMap<String, BikeRental> getRentalsMap(){
        return hazelcastInstance.getMap(RENTALS_MAP_NAME);
    }

    //TODO: remove
    public static long elapsedMilisOf(Runnable task){
        final long NANOS_IN_MILI = 1_000_000;
        var start = Instant.now();
        task.run();
        var end = Instant.now();
        return Duration.between(start, end).dividedBy(NANOS_IN_MILI).getNano();
    }

    private static void exitWithError(String infoMsg, String errorMsg, Throwable ex){
        logger.error(errorMsg, ex);
        logger.info(infoMsg);
        System.exit(1);
    }

    public static void main(String[] args) {
        String addressesRawList = getProperty(ADDRESSES_PROPERTY_NAME, String.class).orElseThrow(() -> new IllegalArgumentException("No addresses chosen for connection to cluster"));
        String[] addressesList = addressesRawList.split(",");
        String inPath = getProperty(INPATH_PROPERTY_NAME, String.class).orElseGet(() -> ".");
        String outPath = getProperty(OUTPATH_PROPERTY_NAME, String.class).orElseGet(() -> ".");
        String query = args[0];

        QuerySolver querySolver = null;
        try {
            querySolver = solvers.get(query).getDeclaredConstructor().newInstance();
        }catch (NullPointerException | NoSuchMethodException e){
            exitWithError("Error: no solver for " + query, "Class not found for entry " + query, e);
        } catch (Exception e){
            exitWithError("Unexpected error, aborting.", "Unexpected exception:", e);
        }
        assert querySolver != null;

        try (Client client = new Client(querySolver)) {
            client.startClient(addressesList, inPath, outPath);

            client.solveQuery();

        }catch (IllegalStateException e){
            exitWithError("Error: could not connect to server. Stopping.", "Could not connect to server:", e);
        }
        catch (IOException e){
            exitWithError("Error: could not read file. Stopping.", "Error reading file:", e);
        }catch (Exception e){
            exitWithError("Unexpected error, aborting.", "Unexpected exception:", e);
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private static <T> Optional<T> getProperty(String name, Class<T> type){
        String rawProperty = System.getProperty(name);
        if( rawProperty == null ) return Optional.empty();
        try{
            T property;
            if (type == Integer.class) {
                property = type.cast(Integer.parseInt(rawProperty));
            } else if (type == Boolean.class) {
                property = type.cast(Boolean.parseBoolean(rawProperty));
            } else if (type == Double.class) {
                property = type.cast(Double.parseDouble(rawProperty));
            } else if (type == Float.class) {
                property = type.cast(Float.parseFloat(rawProperty));
            } else {
                property = type.cast(rawProperty);
            }
            return Optional.of(property);
        } catch (Exception e){
            logger.error("Could not cast property " + name + " as " + rawProperty + " to " + type.getName());
        }
        return Optional.empty();
    }
}
