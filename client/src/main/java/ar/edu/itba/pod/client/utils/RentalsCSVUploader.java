package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.model.BikeRental;
import com.hazelcast.core.IMap;

import java.nio.file.InvalidPathException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RentalsCSVUploader extends CSVUploader<Integer, BikeRental> {
    private final AtomicInteger rentalsIdCounter = new AtomicInteger(0);

    public RentalsCSVUploader(String path, Supplier<IMap<Integer, BikeRental>> hazelcastMapSupplier) throws InvalidPathException {
        super(path, hazelcastMapSupplier);
    }

    @Override
    public BikeRental parseLine(String line) {
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
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String[] fields = line.split(";");

        LocalDateTime startDate =  LocalDateTime.parse(fields[0], dateTimeFormatter);
        int emplacementStart = Integer.parseInt(fields[1]);
        LocalDateTime endDate = LocalDateTime.parse(fields[2], dateTimeFormatter);
        int emplacementEnd = Integer.parseInt(fields[3]);
        boolean isMember = fields[4].equals("1");

        return new BikeRental(
                emplacementStart,
                startDate,
                emplacementEnd,
                endDate,
                isMember
        );
    }

    @Override
    public Integer getKeyFor(BikeRental item) {
        return rentalsIdCounter.getAndIncrement();
    }
}
