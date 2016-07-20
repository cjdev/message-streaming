package com.cj.messagestreaming;

public class StreamLinker {
    public static void link(Types.Subscription subscription, Types.Publication publication, String startingPosition) {
        subscription.forEach(publication::publish);
    }
}
