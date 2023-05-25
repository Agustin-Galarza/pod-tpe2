package ar.edu.itba.pod.utils;

import java.io.Serializable;

public record Pair<L,R>(L left, R right) implements Serializable {
}
