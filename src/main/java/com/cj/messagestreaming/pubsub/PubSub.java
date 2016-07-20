package com.cj.messagestreaming.pubsub;

import com.cj.messagestreaming.Types;
import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Publisher;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.Puller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.spotify.google.cloud.pubsub.client.Puller.builder;

public class PubSub {

    public static Types.Subscription subscribe(PubSubConfig config) throws ExecutionException, InterruptedException {
        return subscribe(config, Pubsub.builder().build());
    }

    public static Types.Publication publish(PubSubConfig config) throws ExecutionException, InterruptedException {
        return publish(config, Pubsub.builder().build());
    }

    protected static Types.Subscription subscribe(PubSubConfig config, Pubsub pubsub) throws ExecutionException, InterruptedException {

        final String subscription = config.getSubscription();
        final String project = config.getProject();
        final String topic = config.getTopic();

        final List<byte[]> records = new ArrayList<>();

        pubsub.createSubscription(project, subscription, topic).get();

        final Puller.MessageHandler handler = (puller, sub, message, ackId) -> {
            records.add(message.decodedData());
            return CompletableFuture.completedFuture(ackId);
        };

        final Puller puller = builder()
                .pubsub(pubsub)
                .project(project)
                .subscription(subscription)
                .concurrency(32)
                .messageHandler(handler)
                .build();

        Stream<byte[]> results = records.stream();

        return null;
    }

    protected static Types.Publication publish(PubSubConfig config, Pubsub pubsub) throws ExecutionException, InterruptedException {

        final String topic = config.getTopic();
        final String project = config.getProject();

        pubsub.createTopic(project, topic).get();

        final Publisher publisher = Publisher.builder()
                .pubsub(pubsub)
                .project(project)
                .concurrency(128)
                .build();

        return (data) -> publisher.publish(topic, Message.of(Message.encode(data)));
    }
}
