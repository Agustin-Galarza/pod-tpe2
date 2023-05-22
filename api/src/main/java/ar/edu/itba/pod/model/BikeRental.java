package ar.edu.itba.pod.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public record BikeRental(
        int startStationId,
        LocalDateTime startDate,
        int endStationId,
        LocalDateTime endDate,
        boolean isMember) {
    @Override
    public String toString(){
        var dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        return String.format("""
                Rental:
                    -from station %d at %s
                    -to station %d at %s
                    - %s member
                """,
                startStationId, dateTimeFormatter.format(startDate),
                endStationId, dateTimeFormatter.format(endDate),
                isMember? "IS":"IS NOT");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BikeRental that)) return false;
        return startStationId == that.startStationId && endStationId == that.endStationId && isMember == that.isMember && Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startStationId, startDate, endStationId, endDate, isMember);
    }
}
