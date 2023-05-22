package ar.edu.itba.pod.client.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class CSVReader<T> {
    private final Path path;
    private final List<CSVColumn<?>> columns = new ArrayList<>();
    private final Function<Map<String, ?>, T> itemGenerator;
    private final String VALUE_SEP = ";";

    public CSVReader(
            String path,
            List<CSVColumn<?>> columns,
            Function<Map<String, ?>, T> itemGenerator
    ) throws InvalidPathException {
        this.path = Paths.get(path);
        this.columns.addAll(columns);
        this.itemGenerator = itemGenerator;
    }

    public Collection<T> getItems(){
        final Function<String, T> itemMapper =
                line -> {
                    final Map<String, Object> extractedValues = new HashMap<>();
                    final String[] rowValues = line.split(VALUE_SEP);
                    for(int i = 0; i < columns.size(); i++){
                        final CSVColumn<?> column = columns.get(i);
                        extractedValues.put(column.name, column.parse(rowValues[i]));
                    }
                    return itemGenerator.apply(extractedValues);
                };
        try(Stream<String> lines = Files.lines(path)){
            return lines.skip(1).parallel().map(itemMapper).toList();
        }catch (IOException e){
            return null;
        }
    }

    public static class CSVColumn<V> {
        private final String name;
        private final Function<String, V> parser;

        public CSVColumn(String name, Function<String, V> parser) {
            this.name = name;
            this.parser = parser;
        }

        public V parse(String value){
            return parser.apply(sanitize(value));
        }

        private String sanitize(String str){
            //TODO
            return str;
        }

        public String getName(){
            return name;
        }
    }
}
