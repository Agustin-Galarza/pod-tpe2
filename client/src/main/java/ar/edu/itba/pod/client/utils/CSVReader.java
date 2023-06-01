package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.client.exceptions.FileOpeningException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class CSVReader<T> {
    private final Path path;
    private final List<CSVColumn<?>> columns = new ArrayList<>();
    @SuppressWarnings("FieldCanBeLocal")
    private final String VALUE_SEP = ";";
    private final Map<String, Object> extractedValues = new HashMap<>();

    public CSVReader(
            String path,
            List<CSVColumn<?>> columns
    ) throws InvalidPathException {
        this.path = Paths.get(path);
        this.columns.addAll(columns);
        this.columns.forEach(column -> extractedValues.put(column.name, null));
    }

    /**
     * Reads the csv file and processes each row with the given procedure.
     * @param processor a function that consumes the values of a row.
     *                      The consumer receives a map with all the values from a row,
     *                      where the column name is the key to its corresponding value.
     * @throws FileOpeningException if there are problems while trying to open the csv file.
     */
    public void processItems(Consumer<Map<String, Object>> processor)  {
        try (Stream<String> lines = Files.lines(path)) {
            lines.skip(1).parallel().forEach(line -> processor.accept(extractValues(line)));
        }catch (IOException e){
            throw new FileOpeningException(path.toString());
        }
    }

    /**
     * Reads the csv file and returns a collection with all the elements extracted from it.
     * @param itemGenerator a function to create an item from the values of a row.
     *                      The function receives a map with all the values from a row,
     *                      where the column name is the key to its corresponding value.
     * @throws FileOpeningException if there are problems while trying to open the csv file.
     */
    public Collection<T> getItems(Function<Map<String, Object>, T> itemGenerator) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines.skip(1).parallel().map(line -> itemGenerator.apply(extractValues(line))).toList();
        } catch (IOException e) {
            throw new FileOpeningException(path.toString());
        }
    }

    private Map<String, Object> extractValues(String line) {
        String[] rowValues = line.split(VALUE_SEP);
        for (int i = 0; i < columns.size(); i++) {
            CSVColumn<?> column = columns.get(i);
            extractedValues.put(column.name, column.parse(rowValues[i]));
        }
        return extractedValues;
    }

    @Override
    public String toString() {
        return String.format("Reader of: %s", path.toString());
    }

    public static class CSVColumn<V> {
        private final String name;
        private final Function<String, V> parser;

        public CSVColumn(String name, Function<String, V> parser) {
            this.name = name;
            this.parser = parser;
        }

        public V parse(String value) {
            return parser.apply(value);
        }


        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Column: " + name;
        }
    }
}
