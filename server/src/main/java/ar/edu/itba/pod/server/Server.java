package ar.edu.itba.pod.server;

import ar.edu.itba.pod.utils.SystemUtils;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private HazelcastInstance hazelcastInstance;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int MAX_MINUTES = 30;
    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";
    private static final String MANCENTERURL_PROPERTY_NAME = "mancenter";
    private static final String TIMEOUT_PROPERTY_NAME = "timeout";

    private Config configServer(){
        Dotenv dotenv = Dotenv.load();
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j");

        // Group Config
        GroupConfig groupConfig = new GroupConfig()
                .setName(dotenv.get(GROUP_NAME_ENV, GROUP_NAME_DEFAULT))
                .setPassword(dotenv.get(GROUP_PASSWORD_ENV, GROUP_PASSWORD_DEFAULT));
        config.setGroupConfig(groupConfig);

        // Network Config
        NetworkConfig networkConfig = new NetworkConfig();
        JoinConfig joinConfig = new JoinConfig();
        MulticastConfig multicastConfig = new MulticastConfig();
        joinConfig.setMulticastConfig(multicastConfig);
        networkConfig.setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);

        // Mancenter config
        SystemUtils.getProperty(MANCENTERURL_PROPERTY_NAME, String.class)
                .ifPresent(url -> {
                    try {
                        ManagementCenterConfig mancenterConfig = new ManagementCenterConfig();
                        mancenterConfig.setEnabled(true);
                        mancenterConfig.setUrl(url);
                        config.setManagementCenterConfig(mancenterConfig);
                    } catch (Exception e) {
                        String message = "Could not connect to Management Center.";
                        logger.error(message, e);
                        logger.info(message);
                    }
                });

        return config;
    }
    public void startServer(){
        logger.info(" Server Starting ...");
        try{
            hazelcastInstance = Hazelcast.newHazelcastInstance(configServer());
        }catch (Exception e){
            logger.error(e.getMessage());
            System.exit(1);
        }
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

        SystemUtils.getProperty(TIMEOUT_PROPERTY_NAME, Integer.class).ifPresentOrElse(server::awaitTermination,
                server::awaitTermination);
        server.awaitTermination();

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
    }
}
