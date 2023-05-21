package org.example.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private HazelcastInstance hazelcastInstance;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int MAX_MINUTES = 10;
    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";

    private Config configServer(){
        final Dotenv dotenv = Dotenv.load();
        final Config config = new Config();
//        config.setProperty( "hazelcast.logging.type", "log4j" );

        // Group Config
        GroupConfig groupConfig = new GroupConfig()
                .setName(dotenv.get(GROUP_NAME_ENV, GROUP_NAME_DEFAULT))
                .setPassword(dotenv.get(GROUP_PASSWORD_ENV, GROUP_PASSWORD_DEFAULT));
        config.setGroupConfig(groupConfig);

        // Network Config
        NetworkConfig networkConfig = new NetworkConfig();
        config.setNetworkConfig(networkConfig);

        return config;
    }
    public void startServer(){
        logger.info(" Server Starting ...");
        hazelcastInstance = Hazelcast.newHazelcastInstance(configServer());
    }
    public void awaitTermination(int maxMinutes){
        try {
            if(countDownLatch.await(maxMinutes, TimeUnit.MINUTES)){
                logger.info("Server timed-out.");
            }
        } catch (InterruptedException e) {
            logger.warn("Server execution interrupted. Reason: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    public void awaitTermination(){
        awaitTermination(MAX_MINUTES);
    }
    public void shutdown(){
        logger.info("Shutting down server.");
        hazelcastInstance.shutdown();
        countDownLatch.countDown();
    }

    public static void main(String[] args) {
        Server server = new Server();

        server.startServer();

        server.awaitTermination();

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
    }

    private static <T> Optional<T> getProperty(final String name, final Class<T> type){
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
