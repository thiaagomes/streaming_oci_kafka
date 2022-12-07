package com.example;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

public class StreamProducer {

    public void produce(String msg) {

        String authToken = "HGz)um)mxd3ui-N-8DjH";
        String tenancyName = "latinoamerica";
        String username = "thiago.gomes@oracle.com";
        String streamingId = "ocid1.streampool.oc1.sa-saopaulo-1.amaaaaaafioir7iajzs7mf3kowzvijlpax4k4bvtdc6aihyj4hng6bja3nqq";
        String topicName = "streamin_kafka_demo";


        Properties properties = new Properties();
        properties.put("bootstrap.servers", "https://cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + streamingId + "\" "
                        + "password=\""
                        + authToken + "\";");

        properties.put("retries", 5); // retries on transient errors and load balancing disconnection
        properties.put("max.request.size", 1024 * 1024); // limit request size to 1MB

        KafkaProducer producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, UUID.randomUUID().toString(), msg);
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                System.out.println(
                        "Sent msg to "
                                + md.partition()
                                + " with offset "
                                + md.offset()
                                + " at "
                                + md.timestamp());
            }
        });

        producer.flush();
        producer.close();
        System.out.println("produced 1 messages");
    }

}