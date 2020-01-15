package com.github.kafka.Producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
    List<String> terms = Lists.newArrayList("CAB");
    String ConsumerKey="OeImFTSRl6S8XaB4dnmd6y3Ud";
    String ConsumerSecret="NH7Qc98UE4rO2qihzzOHNnPjfYvxPMrLMBqtMlAPXlMudXgnDW";
    String token="265331746-AVhN3HlQl8Cd1oeGry81b3oBe6VfwssYh4zVN9Zk";
    String Secret="B0kULK0xMFkjzfxYiaDUhOHIZgczkhuM77IAoUsPIhwn5";


    public TwitterProducer(){}
    public static void main(String[] args) {

    new TwitterProducer().run();


    }

    public void run()
    {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client client=createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String,String> producer=createKafkaProducer();
        while (!client.isDone()) {
            String msg=null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets",null,msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                     if(e !=null)
                     {
                         logger.error("wrong",e);
                     }
                    }
                });

            }
            logger.info("End of application");
        }
    }
    public  Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(ConsumerKey, ConsumerSecret, token,Secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        // Attempts to establish a connection.
    }
    public  KafkaProducer<String,String> createKafkaProducer()
    {
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        return producer;
    }
}
