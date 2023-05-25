package ar.edu.itba.pod.client.exceptions;

public class FileOpeningException extends RuntimeException {
    public FileOpeningException(String path){
        super("Could not open file at " + path);
    }
}
