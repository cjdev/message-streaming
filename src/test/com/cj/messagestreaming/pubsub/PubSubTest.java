package com.cj.messagestreaming.pubsub;

import com.cj.messagestreaming.Types;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PubSubTest {

    Mockery context = new Mockery(){{setImposteriser(ClassImposteriser.INSTANCE);}};

    @Test
    public void test() throws Exception {
        List<byte[]> data = Arrays.asList(
                "message 1".getBytes(),
                "message 2".getBytes()
        );
        List<byte[]> dataReceived = new ArrayList<>();

        Pubsub mockPubsub = context.mock(Pubsub.class);

        final String project = "cj-dev";
        final String topic = "dddRedirect";
        final String sub = "sub1";


        context.checking(new Expectations(){{
            oneOf(mockPubsub).createTopic(project, topic);
            oneOf(mockPubsub).createSubscription(project, sub, topic);

            allowing(mockPubsub).pull(with(equal(project)), with(equal(sub)), with(any(Boolean.class)), with(any(Integer.class)));
        }});

        PubSubConfig config = new PubSubConfig(project, topic, sub);
        Types.Publication publication = PubSub.publish(config, mockPubsub);
        Types.Subscription subscription = PubSub.subscribe(config, mockPubsub);

        data.forEach(publication::publish);
        subscription.forEach(dataReceived::add);

        assertEquals(data.size(), dataReceived.size());
    }
}
