package com.rbc.cloud.hackathon.kafka.consumer.service;

import com.rbc.cloud.hackathon.data.Transactions;
import com.rbc.cloud.hackathon.data.TransactionsSum;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


/**
 * Created by Chris Matta on 12/15/17.
 */
public class KafkaStreamsExample {
    public static void main (String[] args){

        final String apiKey = args[0];
        final String apiSecret = args[1];

        // args: topic apikey apisecret
        // Load properties from the application.properties file
        Properties applicationProperties = new Properties();

        try {
            File applicationPropertiesFile = new File("kafka-streams-applciation.properties");
            FileInputStream applicationPropertiesInput = new FileInputStream(applicationPropertiesFile);
            applicationProperties.load(applicationPropertiesInput);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, applicationProperties.get("kafka.bootstrap.servers"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-transactions");
        streamsProps.put("schema.registry.url", applicationProperties.get("schema.registry.url"));
        streamsProps.put("sasl.mechanism", applicationProperties.get("sasl.mechanism"));
        streamsProps.put("security.protocol", applicationProperties.get("security.protocol"));
        streamsProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + args[1] + "\" password=\""+ args[2] +"\"" +
            "sasl.mechanism=PLAIN");

        final String transactionsTopic = applicationProperties.get("topic.name").toString();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transactions> transactionsKStream = builder.stream(transactionsTopic);

        KTable<Windowed<String>, TransactionsSum> transactionsSumKTable = transactionsKStream
            // extract the custId as the key, and group by it
            .selectKey((k, v) -> v.getCustId().toString())
            .groupByKey()
            // set our windowing
            .windowedBy(TimeWindows.of(30 * 1000L))
            // Now we're aggregating on that key in the above window,
            // for each window and key, initialize a new TransactionsSum object
            // then add the TransactionAmount
            .aggregate(
                new Initializer<TransactionsSum>() {
                    @Override
                    public TransactionsSum apply() {
                        return new TransactionsSum();
                    }
                },
                new Aggregator<String, Transactions, TransactionsSum>() {
                    @Override
                    public TransactionsSum apply(String key, Transactions value, TransactionsSum aggregate) {
                        aggregate.setCustId(key);
                        aggregate.setTransactionSum(
                            Integer.valueOf(value.getTransactionAmount().toString()) +
                                aggregate.getTransactionSum());
                        return aggregate;
                    }
                });

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
