package com.alex.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final String consumerKey = "o8PYt4hJJ93tXGL0sTUPH0o4S";
    public static final String consumerSecret = "cr1i27VUP7oYr8PEE2bO5EFApoLd5YpPK6hwKLGyC25AZ1CbfN";
    public static final String token = "908683888184217600-m6TjCjA8RGtajtLnBW52rIUrwNzpHhE";
    public static final String tokenSecret = "M17ucQQ4lIwwRjWzV24RNl81qgesY6aNyPc4A0c2ysRWY";


    public TwitterProducer() {
    }

    public void run() {
        System.out.println("Setup");
        BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(blockingQueue);
        client.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping application");
            System.out.println("Shutting down client from twitter");
            client.stop();
            System.out.println("Closing producer");
            kafkaProducer.close();
            System.out.println("Done!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = blockingQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                kafkaProducer.close();
                client.stop();
            }
            if (msg != null) {
                System.out.println(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), sendCallBack());
            }
        }

        System.out.println("End of application");
    }

    public Callback sendCallBack() {
        return (metadata, exception) -> {
            if (exception != null) {
                System.out.println("Eror while producing message");
            }
        };
    }

    public Client createTwitterClient(BlockingQueue<String> blockingQueue) {
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(Lists.newArrayList("bitcoin"));

        return new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(new OAuth1(consumerKey, consumerSecret, token, tokenSecret))
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(blockingQueue))
                .build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
