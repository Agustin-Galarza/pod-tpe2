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
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.*;

public class Client implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static final Logger performanceLogger = LoggerFactory.getLogger("performance");

    private HazelcastInstance hazelcastInstance;
    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";

    public void startClient(){
        logger.info("Starting client ...");
        hazelcastInstance = HazelcastClient.newHazelcastClient(configClient());
    }
    public ClientConfig configClient(){
        final Dotenv dotenv = Dotenv.load();
        final ClientConfig config = new ClientConfig();

        // Group Config
        final GroupConfig groupConfig = new GroupConfig()
                .setName(dotenv.get(GROUP_NAME_ENV, GROUP_NAME_DEFAULT))
                .setPassword(dotenv.get(GROUP_PASSWORD_ENV, GROUP_PASSWORD_DEFAULT));
        config.setGroupConfig(groupConfig);
        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        // TODO: configure network setup and discovery
        config.setNetworkConfig(clientNetworkConfig);

        return config;
    }
    public void shutdown(){
        logger.info("Shutting down client ...");
        hazelcastInstance.shutdown();
    }

    public Set<BikeRental> readRentals(){
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
        final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

        final var reader = new CSVReader<BikeRental>(
                "./bikes.csv",
                Arrays.asList(
                        new CSVReader.CSVColumn<Object>("start_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<Object>("emplacement_pk_start", Integer::parseInt),
                        new CSVReader.CSVColumn<Object>("end_date", rawDate -> LocalDateTime.parse(rawDate, DateTimeFormatter.ofPattern(DATE_TIME_PATTERN))),
                        new CSVReader.CSVColumn<Object>("emplacement_pk_end", Integer::parseInt),
                        new CSVReader.CSVColumn<>("is_member", Boolean::parseBoolean)
                ),
                values ->
                        new BikeRental(
                                (int) values.get("emplacement_pk_start"),
                                (LocalDateTime) values.get("start_date"),
                                (int) values.get("emplacement_pk_end"),
                                (LocalDateTime) values.get("end_date"),
                                (boolean) values.get("is_member")
                        )
        );
        return new HashSet<>(reader.getItems());
    }

    public Set<Station> readStations(){
        /*
         * ○ pk: Identificador de la estación (número entero)
         * ○ name: Nombre de la estación (cadena de caracteres)
         * ○ latitude: Latitud de la ubicación de la estación (numéro real)
         * ○ longitude: Longitud de la ubicación de la estación (número real)
         */

        CSVReader<Station> reader = new CSVReader<>(
                "./stations.csv",
                Arrays.asList(
                        new CSVReader.CSVColumn<Integer>("pk", Integer::parseInt),
                        new CSVReader.CSVColumn<String>("name", s -> s),
                        new CSVReader.CSVColumn<Double>("latitude", Double::parseDouble),
                        new CSVReader.CSVColumn<Double>("longitude", Double::parseDouble)
                ),
                values -> new Station(
                        (int) values.get("pk"),
                        (String) values.get("name"),
                        new Coordinate((double) values.get("latitude"), (double) values.get("longitude"))
                )
        );
        return new HashSet<>(reader.getItems());
    }

    //TODO: remove
    public static long elapsedMilisOf(Runnable task){
        final long NANOS_IN_MILI = 1_000_000;
        final var start = Instant.now();
        task.run();
        final var end = Instant.now();
        return Duration.between(start, end).dividedBy(NANOS_IN_MILI).getNano();
    }

    public static void main(String[] args) {
        try (final Client client = new Client()) {
            client.startClient();

            performanceLogger.info("Inicio de la lectura de los archivos.");
            client.readRentals();
            client.readStations();
            performanceLogger.info("Fin de la lectura de los archivos.");


        }catch (IllegalStateException e){
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
