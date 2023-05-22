package ar.edu.itba.pod.model;

import java.io.Serializable;
import java.util.Objects;

public record Station(int id, String name, Coordinate coordinate) implements Serializable {

    @Override
    public String toString(){
        return "Station " + id + ": " + name + '\n' +
                "\t- location: " + coordinate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Station station)) return false;
        return id == station.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
