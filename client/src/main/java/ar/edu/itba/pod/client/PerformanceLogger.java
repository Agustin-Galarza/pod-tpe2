package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.exceptions.FileOpeningException;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PerformanceLogger {
    private static final String DATE_FMT = "dd/MM/YYYY  HH:mm:ss:SSS";
    private static final String LOGGER_NAME = "PERFORMANCE";
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_FMT);

    private static String buildLine(String message){
        String dateString = dateFormatter.format(LocalDateTime.now());
        String thread = Thread.currentThread().getName();
        // [date] [name] [thread]: [message]
        return String.format("%s %s [%s]: %s\n", dateString, LOGGER_NAME, thread, message);
    }
    public static PerformanceLogger logTo(String targetDir, String filename, boolean writeToConsole){
        return new PerformanceLogger(Path.of(String.join("/", targetDir, filename)), writeToConsole);
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
    private boolean writeToConsole;

    private PerformanceLogger(Path path, boolean writeToConsole){
        this.path = path;
        this.writeToConsole = writeToConsole;
        if(!Files.exists(path)){
            try{
                Files.createFile(path);
            } catch (IOException e){
                throw new RuntimeException(e);
            }
        }

    }

    private void log(Messages message){
        var line = buildLine(message.msg);
        try {
            Files.write(path, line.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e){
            throw new FileOpeningException(path.toString());
        }
        if(writeToConsole){
            System.out.print(line);
        }
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
