package ar.edu.itba.pod.model;

import java.time.LocalDateTime;

public record BikeRental(
        int startStationId,
        LocalDateTime startDate,
        int endStationId,
        LocalDateTime endDate,
        boolean isMember) {
}
