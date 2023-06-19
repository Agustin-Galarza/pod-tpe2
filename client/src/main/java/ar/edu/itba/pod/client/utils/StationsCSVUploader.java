package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.model.Coordinate;
import ar.edu.itba.pod.model.Station;
import com.hazelcast.core.IMap;

import java.nio.file.InvalidPathException;
import java.util.function.Supplier;

public class StationsCSVUploader extends CSVUploader<Integer,Station>{

    public StationsCSVUploader(String path, Supplier<IMap<Integer, Station>> hazelcastMapSupplier) throws InvalidPathException {
        super(path, hazelcastMapSupplier);
    }

    @Override
    public Station parseLine(String line) {
        /*
         * ○ pk: Identificador de la estación (número entero)
         * ○ name: Nombre de la estación (cadena de caracteres)
         * ○ latitude: Latitud de la ubicación de la estación (número real)
         * ○ longitude: Longitud de la ubicación de la estación (número real)
         */
        String[] fields = line.split(";");
        return new Station(
                Integer.parseInt(fields[0]),
                fields[1],
                new Coordinate(Double.parseDouble(fields[2]), Double.parseDouble(fields[3]))
        );
    }

    @Override
    public Integer getKeyFor(Station item) {
        return item.id();
    }
}
