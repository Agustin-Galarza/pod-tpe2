package ar.edu.itba.pod.utils;

import java.util.Arrays;

public enum FnResult {
    OK(true),
    ERROR(false);
    private boolean ok;
    FnResult(boolean ok){
        this.ok = ok;
    }
    public boolean isOk(){
        return this.ok;
    }

    public FnResult and(FnResult otherResult){
        return both(this, otherResult);
    }

    public static FnResult both(FnResult result1, FnResult result2){
        if(result1.ok && result2.ok){
            return OK;
        }
        return ERROR;
    }

    public static FnResult all(FnResult... results){
        return Arrays.stream(results).reduce(FnResult.OK, FnResult::both);
    }
}
