package com.cj.messagestreaming;

import java.util.function.Function;
import java.util.stream.Stream;

public class Types {

    public interface Subscription extends Function<String, Stream<byte[]>> {
        default Stream<byte[]> startingFrom(String pos){
            return this.apply(pos);
        }
    }
}
