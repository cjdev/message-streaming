package com.cj.messagestreaming;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import scala.collection.JavaConverters;



public class Kafka {
	
    public static Types.Subscription subscribe(KafkaConfig config) {
        return (id) -> subscribe(config, id);

    }


	public static Types.Publication publish(KafkaConfig config) {
	    return (data) -> {};
    }
	
	

    private static Stream<byte[]> subscribe(KafkaConfig config, String id) {
        final Integer numThreads = 1;
        final String topic = "myCoolTopic";
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConsumerConfig());
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(new HashMap<String, Integer>() {{
            put(topic, numThreads);
        }});
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        final ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();


        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                consumer.shutdown();
            }
        });
        
        Iterable<MessageAndMetadata<byte[], byte[]>> i = JavaConverters.asJavaIterableConverter(it.toIterable()).asJava();
    	return StreamSupport.stream(i.spliterator(), false).map(metadata->metadata.message());
	}

}
