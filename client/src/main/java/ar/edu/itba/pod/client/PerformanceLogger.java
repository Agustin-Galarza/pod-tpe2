package ar.edu.itba.pod.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class PerformanceLogger {
    private static final Logger logger = LoggerFactory.getLogger("performance");
    private static final String DATE_FMT = " dd/MM/yyyy HH:mm:ss:xxxx";

    public static PerformanceLogger logTo(String targetDir, String filename){
        return new PerformanceLogger(Path.of(String.join("/", targetDir, filename)));
    }

    private enum Messages{
        READ_START("Inicio de la lectura de los archivos."),
        READ_END("Fin de la lectura de los archivos."),
        MAPREDUCE_START("Inicio del proceso mapReduce."),
        MAPREDUCE_END("Fin del proceso mapReduce.");
        private final String msg;
        Messages(String msg){
            this.msg = msg;
        }
        @Override
        public String toString(){
            return msg;
        }
    }

    private final Path path;

    private PerformanceLogger(Path path){
        this.path = path;
    }

    private void log(Messages message){
        logger.info(message.toString());
    }

    public void logReadStart(){
        log(Messages.READ_START);
    }

    public void logReadEnd(){
        log(Messages.READ_END);
    }

    public void logMapReduceStart(){
        log(Messages.MAPREDUCE_START);
    }

    public void logMapReduceEnd(){
        log(Messages.MAPREDUCE_END);
    }
}
