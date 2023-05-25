package ar.edu.itba.pod.client.exceptions;

public class FileWriteException extends RuntimeException{
    public FileWriteException(String path){
        super("There was a problem while writing into " + path);
    }
}
