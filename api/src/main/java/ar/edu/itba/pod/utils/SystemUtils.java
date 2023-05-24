package ar.edu.itba.pod.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SystemUtils {
    private static final Logger logger = LoggerFactory.getLogger(SystemUtils.class);

    public static <T> Optional<T> getProperty(String name, Class<T> type){
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
