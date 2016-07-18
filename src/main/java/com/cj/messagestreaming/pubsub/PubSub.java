package com.cj.messagestreaming.pubsub;

import com.cj.messagestreaming.Types;

import java.util.stream.Stream;

public class PubSub {

    public static Types.Subscription subscribe(PubSubConfig config) {
        return (pos) -> subscribe(config, pos);
    }

    public static Types.Publication publish(PubSubConfig config) {
        return (data) -> {};
    }

    private static Stream<byte[]> subscribe(PubSubConfig config, String id) {
        return Stream.empty();
    }
}
