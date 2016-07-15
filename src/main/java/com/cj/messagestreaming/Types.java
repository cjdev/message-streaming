package com.cj.messagestreaming;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class Types {

    public interface Subscription extends Function<String, Stream<byte[]>> {
        default Stream<byte[]> startingFrom(String pos){
            return this.apply(pos);
        }
    }

    public interface Publication extends Consumer<byte[]> {
        default void publish(byte[] obj) {
            this.accept(obj);
        }
    }
}
