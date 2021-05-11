package org.example;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerPOC2 {
    public static void main(String[] args) throws IOException {
        Properties props=new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"my-avro-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url","http://localhost:9081");
        props.put("specific.avro.reader","true");

        FileWriter f = new FileWriter("/home/ubuntu/IdeaProjects/Kafka_Avro_poc2/target/output.txt");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(props);
        String topic="Kafkaschemaevolution";
        consumer.subscribe(Collections.singleton(topic));

        System.out.println("My Data");

        while(true){
            ConsumerRecords<String,Customer> records=consumer.poll(500);
            for(ConsumerRecord<String, Customer> record:records){
                Customer customer=record.value();

                //System.out.println(record.key());
                System.out.println(customer);

                f.write(String.valueOf(customer));
                f.close();

            }
            consumer.commitSync();
        }
        //consumer.close();
    }
}

