package org.example;
import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerPOC2 {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:9081");
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(props);
        String topic="Kafkaschemaevolution";
        String key="key1";
        Customer customer=Customer.newBuilder()
                .setFirstName("sangeetha")
                .setLastName("Gobinaath")
                .setCustomerId(15)
                .setAutomatedEmail(false)
                .build();

        ProducerRecord<String,Customer> producerRecord = new ProducerRecord<String, Customer>(topic,key,customer
        );
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if (exception == null) {
                    System.out.println("Success!");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}



