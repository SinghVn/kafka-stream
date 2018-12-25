package com.stream.kafka.bank.app;





import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {

    public static void main(String[] args) {

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        Producer<String, String> producer = new KafkaProducer(producerProps);
        int i = 0;

        while (true) {

            try {
                System.out.println("messages batch " + i);
                producer.send(getRandomTransaction("vipin"));
                Thread.sleep(1000);


                producer.send(getRandomTransaction("rogers"));
                Thread.sleep(1000);

                producer.send(getRandomTransaction("anand"));
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }

        producer.close();

    }

    private static  ProducerRecord<String,String> getRandomTransaction(String name){

        ObjectNode transaction=JsonNodeFactory.instance.objectNode();
        Integer amount=ThreadLocalRandom.current().nextInt(0,100);
        Instant now=Instant.now();
        transaction.put("name",name);
        transaction.put("amount",amount);
        transaction.put("time",now.toString());

        return new ProducerRecord<>("bank-trans-producer",name,transaction.toString());

    }

}


