package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.client.exceptions.FileOpeningException;
import com.hazelcast.core.IMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class CSVUploader<K,V> {
    private final Path path;
    private final Supplier<IMap<K, V>> hazelcastMap;
    private final int BATCH_SIZE = 100_000;
    private final Map<K,V> cacheMap = new HashMap<>(BATCH_SIZE);

    public CSVUploader(
            String path,
            Supplier<IMap<K, V>> hazelcastMapSupplier
    ) throws InvalidPathException {
        this.path = Paths.get(path);
        this.hazelcastMap = hazelcastMapSupplier;
    }

    public void uploadItems(){
        try(var lines = Files.lines(path)) {
            lines.skip(1).map(this::parseLine).forEach(this::consumeItem);
            clearCache();
        } catch (IOException e) {
            throw new FileOpeningException(path.toString());
        }
    }
    public abstract V parseLine(String line);
    public abstract K getKeyFor(V item);
    private void consumeItem(V item){
        cacheMap.put(getKeyFor(item), item);
        if(cacheMap.size() == BATCH_SIZE)
            clearCache();
    }

    private void clearCache(){
        hazelcastMap.get().putAll(cacheMap);
        cacheMap.clear();
    }

    @Override
    public String toString() {
        return String.format("Reader of: %s", path.toString());
    }

}
