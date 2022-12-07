package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StreamConsumer {
	
	public void consume() {
			
		String authToken = "HGz)um)mxd3ui-N-8DjH";
        String tenancyName = "latinoamerica";
        String username = "thiago.gomes@oracle.com";
        String streamingId = "ocid1.streampool.oc1.sa-saopaulo-1.amaaaaaafioir7iajzs7mf3kowzvijlpax4k4bvtdc6aihyj4hng6bja3nqq";
        String topicName = "streamin_kafka_demo";
		
		
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "https://cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-0");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + streamingId + "\" "
                        + "password=\""
                        + authToken + "\";"
        );
        properties.put("max.partition.fetch.bytes", 1024 * 1024); 

        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        try {
            consumer.subscribe(Collections.singletonList( topicName ) );

            while(true) {
                Duration duration = Duration.ofMillis(1000);
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(duration);
                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                });
             
                consumer.commitAsync();
            }
        }
        catch(WakeupException e) {
          
        }
        finally {
            System.out.println("closing consumer");
            consumer.close();
        }

		
		
	}

}