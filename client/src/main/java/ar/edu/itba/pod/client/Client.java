package ar.edu.itba.pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private HazelcastInstance hazelcastInstance;
    private static final String GROUP_NAME_ENV = "GROUP_NAME";
    private static final String GROUP_NAME_DEFAULT = "name";
    private static final String GROUP_PASSWORD_ENV = "GROUP_PASSWORD";
    private static final String GROUP_PASSWORD_DEFAULT = "password";

    public void startClient(){
        logger.info("Starting client ...");
        try{
            hazelcastInstance = HazelcastClient.newHazelcastClient(configClient());
        }catch (IllegalStateException e){
            logger.error(e.getMessage());
            System.exit(1);
        }
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


    public static void main(String[] args) {
        try (final Client client = new Client()) {
            client.startClient();
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
