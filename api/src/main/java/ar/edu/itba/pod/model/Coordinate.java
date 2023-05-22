package ar.edu.itba.pod.model;

import java.io.Serializable;
import java.util.Objects;

public record Coordinate(double latitude, double longitude) implements Serializable {

    @Override
    public String toString(){
        return String.format("(%.4f,%.4f)", latitude, longitude);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Coordinate that)) return false;
        return Double.compare(that.latitude, latitude) == 0 && Double.compare(that.longitude, longitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(latitude, longitude);
    }
}
