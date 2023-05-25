package ar.edu.itba.pod.client.exceptions;

public class MissingSystemPropertyException extends RuntimeException{
    public MissingSystemPropertyException(String message){
        super(message);
    }
}
