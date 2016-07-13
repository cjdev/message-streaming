package com.cj.messagestreaming;

import java.util.function.Function;
import java.util.stream.Stream;

public class Types {

    public interface Subscription<T> extends Function<String, Stream<T>> {
        default Stream<T> startingFrom(String pos){
            return this.apply(pos);
        }
    }
}
