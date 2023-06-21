package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.exceptions.FileOpeningException;
import ar.edu.itba.pod.client.exceptions.MissingSystemPropertyException;
import ar.edu.itba.pod.client.querySolvers.Query1Solver;
import ar.edu.itba.pod.client.querySolvers.Query2Solver;
import ar.edu.itba.pod.client.querySolvers.QuerySolver;
import ar.edu.itba.pod.client.utils.RentalsCSVUploader;
import ar.edu.itba.pod.client.utils.StationsCSVUploader;
import ar.edu.itba.pod.model.BikeRental;
import ar.edu.itba.pod.model.Station;
import ar.edu.itba.pod.utils.SystemUtils;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import io.github.cdimascio.dotenv.Dotenv;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

public class Client implements AutoCloseable{

    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";

    private static final String ADDRESSES_PROPERTY_NAME = "addresses";
    private static final String INPATH_PROPERTY_NAME = "inPath";
    private static final String OUTPATH_PROPERTY_NAME = "outPath";
    private static final String LOGTOCONSOLE_PROPERTY_NAME = "logPerformance";

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
        String addressesRawList = SystemUtils.getProperty(ADDRESSES_PROPERTY_NAME, String.class).orElseThrow(() -> new IllegalArgumentException("No addresses chosen for connection to cluster"));
        String[] addressesList = addressesRawList.split(",");
        String inPath = SystemUtils.getProperty(INPATH_PROPERTY_NAME, String.class).orElse(".");
        String outPath = SystemUtils.getProperty(OUTPATH_PROPERTY_NAME, String.class).orElse(".");
        // No es tan importante que se especifique este parámetro, solo afecta a los logs de tiempos de ejecución en la consola.
        boolean logToConsole = SystemUtils.getProperty(LOGTOCONSOLE_PROPERTY_NAME, Boolean.class).orElse(true);

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
            client.startClient(addressesList, inPath, outPath, logToConsole);

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

    public void startClient(@NotNull String[] nodeAddresses, @NotNull String inPath, @NotNull String outPath,
            boolean writeToConsole) {
        logger.info("Starting client ...");
        hazelcastInstance = HazelcastClient.newHazelcastClient(configClient(nodeAddresses));
        this.inPath = inPath;
        this.outPath = outPath;

        performanceLogger = PerformanceLogger.logTo(
                outPath,
                solver.getName().replace("query", "text") + ".txt",
                writeToConsole);

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
        hazelcastInstance.getMap(RENTALS_MAP_NAME).clear();
        hazelcastInstance.getMap(STATIONS_MAP_NAME).clear();

        hazelcastInstance.shutdown();
    }

    private void uploadRentals() {
        final String path = String.join("/", inPath, RENTALS_CSV_FILENAME);
        new RentalsCSVUploader(path, this::getRentalsMap).uploadItems();
    }

    private void uploadStations() {
        final String path = String.join("/", inPath, STATIONS_CSV_FILENAME);
        new StationsCSVUploader(path, this::getStationsMap).uploadItems();
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

    public IMap<Integer, Station> getStationsMap(){
        return hazelcastInstance.getMap(STATIONS_MAP_NAME);
    }

    public IMap<Integer, BikeRental> getRentalsMap(){
        return hazelcastInstance.getMap(RENTALS_MAP_NAME);
    }


    @Override
    public void close() {
        shutdown();
    }

}
