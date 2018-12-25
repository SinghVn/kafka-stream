package com.stream.kafka.bank.app;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Properties;

public class BankTransactionStream {

    public static void main(String[] args) {

        Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"bank-trans-stream-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        properties.put(StreamsConfig.EXACTLY_ONCE,"true");
        KStreamBuilder builder=new KStreamBuilder();
        final Serializer<JsonNode> serializer=new JsonSerializer();
        final Deserializer<JsonNode> deserializer=new JsonDeserializer();
        final Serde<JsonNode> serdes=Serdes.serdeFrom(serializer,deserializer);

       // transactionStream.groupByKey().aggregate(()->{0},)

        com.fasterxml.jackson.databind.node.ObjectNode intialBalance=JsonNodeFactory.instance.objectNode();
        intialBalance.put("count",0);
        intialBalance.put("balance",0);
        intialBalance.put("time",Instant.ofEpochMilli(0L).toString());
        KStream<String,JsonNode> transactionStream=builder.stream(Serdes.String(),serdes,"bank-trans-producer");
        KTable<String,JsonNode> bankBalance=transactionStream.groupByKey(Serdes.String(),serdes)
                .aggregate(()->intialBalance,(key,transaction,balance)->newBalance(transaction,balance),serdes,"bank-bal-aggr");
        bankBalance.to(Serdes.String(),serdes,"bank-balance-exactly-once");
        KafkaStreams streams=new KafkaStreams(builder,properties);
        streams.start();
        System.out.println(streams.toString());
    }

private static JsonNode newBalance(JsonNode transaction,JsonNode balance){

    com.fasterxml.jackson.databind.node.ObjectNode newBalance=JsonNodeFactory.instance.objectNode();
    newBalance.put("count",balance.get("count").asInt()+1);
    newBalance.put("balance",balance.get("balance").asInt()+transaction.get("amount").asInt());
    Long balanceTime=Instant.parse(balance.get("time").asText()).toEpochMilli();
    Long transactionTime=Instant.parse(transaction.get("time").asText()).toEpochMilli();
    Instant newBalanceTime=Instant.ofEpochMilli(Math.max(balanceTime,transactionTime));
    newBalance.put("time",newBalanceTime.toString());
    return newBalance;
}
}
