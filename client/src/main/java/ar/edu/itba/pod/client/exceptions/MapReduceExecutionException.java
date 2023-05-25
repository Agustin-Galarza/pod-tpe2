package ar.edu.itba.pod.client.exceptions;

public class MapReduceExecutionException extends RuntimeException{
    public MapReduceExecutionException(String cause){
        super("There was a problem while executing the MapReduce job: " + cause);
    }
}
