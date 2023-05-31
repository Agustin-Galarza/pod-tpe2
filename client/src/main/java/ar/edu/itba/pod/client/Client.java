package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.exceptions.FileOpeningException;
import ar.edu.itba.pod.client.exceptions.MissingSystemPropertyException;
import ar.edu.itba.pod.client.querySolvers.Query1Solver;
import ar.edu.itba.pod.client.querySolvers.Query2Solver;
import ar.edu.itba.pod.client.querySolvers.QuerySolver;
import ar.edu.itba.pod.client.utils.CSVReader;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Coordinate;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.SystemUtils;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import io.github.cdimascio.dotenv.Dotenv;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Client implements AutoCloseable{

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

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static void exitWithError(String infoMsg, String errorMsg, Throwable ex){
        logger.error(errorMsg, ex);
        logger.info(infoMsg);
        System.exit(1);
    }

    private enum Query {
        QUERY1("query1", () -> new Query1Solver("query1")),
        QUERY2("query2", () -> {
            Integer n = SystemUtils.getProperty("n", Integer.class).orElseThrow(() -> new MissingSystemPropertyException("Missing property 'n' for max stations to print"));
            return new Query2Solver("query2", n);
        })
        ;
        private final String name;
        private final Supplier<QuerySolver> solverGenerator;
        Query(String name, Supplier<QuerySolver> solverGenerator) {
            this.name = name;
            this.solverGenerator = solverGenerator;
        }
        public String getName(){
            return name;
        }
        public QuerySolver getSolver(){
            return solverGenerator.get();
        }
    }

    public static void main(String[] args) {
        //TODO: test
        String addressesRawList = SystemUtils.getProperty(ADDRESSES_PROPERTY_NAME, String.class).orElseThrow(() -> new IllegalArgumentException("No addresses chosen for connection to cluster"));
        String[] addressesList = addressesRawList.split(",");
        String inPath = SystemUtils.getProperty(INPATH_PROPERTY_NAME, String.class).orElseGet(() -> ".");
        String outPath = SystemUtils.getProperty(OUTPATH_PROPERTY_NAME, String.class).orElseGet(() -> ".");
        if(args.length != 1){
            exitWithError("Error: missing query param.", "No query solver name given", new IllegalArgumentException());
        }
        String queryArg = args[0];

        Optional<Query> query = Arrays.stream(Query.values()).filter(e -> e.getName().equals(queryArg)).findFirst();
        if(query.isEmpty()){
            logger.info("No solver for " + queryArg);
            return;
        }

        try (Client client = new Client(query.get().getSolver())) {
            client.startClient(addressesList, inPath, outPath);

            client.solveQuery();

        } catch (FileOpeningException e){
            exitWithError("Error: could not open file.", e.getMessage(), e);
        } catch (MissingSystemPropertyException e){
            exitWithError("Error: missing property: " + e.getMessage(), e.toString(), e);
        }catch (Exception e){
            exitWithError("Unexpected error, aborting.", "Unexpected exception:", e);
        }
    }

    private HazelcastInstance hazelcastInstance;
    private final QuerySolver solver;
    private String inPath;
    private String outPath;
    private PerformanceLogger performanceLogger;

    public Client(@NotNull QuerySolver solver){
        this.solver = solver;
    }

    public void startClient(@NotNull String[] nodeAddresses,@NotNull String inPath,@NotNull String outPath) {
        logger.info("Starting client ...");
        hazelcastInstance = HazelcastClient.newHazelcastClient(configClient(nodeAddresses));
        this.inPath = inPath;
        this.outPath = outPath;

        performanceLogger = PerformanceLogger.logTo(outPath, solver.getName().replace("query", "text") + ".txt");

        performanceLogger.logReadStart();
        uploadRentals();
        uploadStations();
        performanceLogger.logReadEnd();
    }
    public ClientConfig configClient(@NotNull String[] nodeAddresses){
        Dotenv dotenv = Dotenv.load();
        ClientConfig config = new ClientConfig();

        config.setProperty( "hazelcast.logging.type", "none" );

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
        // Destroy maps before closing to clear up the memory
        hazelcastInstance.getMap(RENTALS_MAP_NAME).destroy();
        hazelcastInstance.getMap(STATIONS_MAP_NAME).destroy();

        hazelcastInstance.shutdown();
    }

    private void uploadRentals() {
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
        var rentalsMap = getRentalsMap();
        final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
        final String path = String.join("/", inPath, RENTALS_CSV_FILENAME);

        var reader = new CSVReader<BikeRental>(
                path,
                Arrays.asList(
                        new CSVReader.CSVColumn<>("start_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<>("emplacement_pk_start", Integer::parseInt),
                        new CSVReader.CSVColumn<>("end_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<>("emplacement_pk_end", Integer::parseInt),
                        new CSVReader.CSVColumn<>("is_member", value -> value.equals("1"))
                )
        );
        // TODO: design decision -> cada rental tiene un id para recuperarlo luego y minimizar la cantidad de datos que envío por la red
        AtomicInteger rentalId = new AtomicInteger(0);
        reader.processItems(
                values -> {
                    var rental = new BikeRental(
                            (int) values.get("emplacement_pk_start"),
                            (LocalDateTime) values.get("start_date"),
                            (int) values.get("emplacement_pk_end"),
                            (LocalDateTime) values.get("end_date"),
                            (boolean) values.get("is_member")
                    );
                    rentalsMap.put(rentalId.getAndIncrement(), rental);
                }
        );

    }

    private void uploadStations() {
        /*
         * ○ pk: Identificador de la estación (número entero)
         * ○ name: Nombre de la estación (cadena de caracteres)
         * ○ latitude: Latitud de la ubicación de la estación (número real)
         * ○ longitude: Longitud de la ubicación de la estación (número real)
         */
        ConcurrentMap<Integer, Station> stationsMap = getStationsMap();
        final String path = String.join("/", inPath, STATIONS_CSV_FILENAME);

        CSVReader<Station> reader = new CSVReader<>(
                path,
                Arrays.asList(
                        new CSVReader.CSVColumn<>("pk", Integer::parseInt),
                        new CSVReader.CSVColumn<>("name", s -> s),
                        new CSVReader.CSVColumn<>("latitude", Double::parseDouble),
                        new CSVReader.CSVColumn<>("longitude", Double::parseDouble)
                )
        );
        reader.processItems(
                values -> {
                    var station = new Station(
                            (int) values.get("pk"),
                            (String) values.get("name"),
                            new Coordinate((double) values.get("latitude"), (double) values.get("longitude"))
                    );
                    stationsMap.put(station.id(), station);
                }
        );

    }

    public void solveQuery(){
        performanceLogger.logMapReduceStart();
        solver.solveQuery(
                hazelcastInstance,
                hzInstance -> hzInstance.getMap(RENTALS_MAP_NAME),
                hzInstance -> hzInstance.getMap(STATIONS_MAP_NAME),
                outPath
        );
        performanceLogger.logMapReduceEnd();

    }

    public ConcurrentMap<Integer, Station> getStationsMap(){
        return hazelcastInstance.getMap(STATIONS_MAP_NAME);
    }

    public ConcurrentMap<Integer, BikeRental> getRentalsMap(){
        return hazelcastInstance.getMap(RENTALS_MAP_NAME);
    }


    @Override
    public void close() {
        shutdown();
    }

}
