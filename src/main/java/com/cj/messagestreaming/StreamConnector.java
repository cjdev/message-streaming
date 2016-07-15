package com.cj.messagestreaming;

public class StreamConnector {
    public static void connect(Types.Subscription subscription, Types.Publication publication, String startingPosition) {
        subscription.startingFrom(startingPosition).forEach(publication::publish);
    }
}
