package com.capstoe.producer;

/*import com.capstone.domain.Account;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import java.util.*;

public class KafkaAccountSenderApp {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.capstone.serializer.AccountSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.capstone.producer.AccountTypePartitioner");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        List<Account> accounts = Arrays.asList(
                new Account(1001,501,"CA","Hebbal"),
                new Account(1002,502,"SB","Ulsoor"),
                new Account(1003,503,"RD","Hebbal"),
                new Account(1004,504,"LOAN","Panjagutta"),
                new Account(1005,505,"SB","Panjagutta")
        );

        for (Account acc : accounts) {
            String value = mapper.writeValueAsString(acc);
            ProducerRecord<String, String> rec =
                new ProducerRecord<>("account-openings", acc.getAccountType(), value);
            producer.send(rec, (md, ex) -> {
                if (ex != null) ex.printStackTrace();
                else System.out.printf("Sent %s to partition %d offset %d%n",
                        acc.getAccountType(), md.partition(), md.offset());
            });
        }

        producer.close();
    }
}*/


import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.capstone.domain.Account;
import com.capstone.serializer.AccountSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAccountSenderApp {
        public static void main(String[] args) throws Exception {
      
                
                
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountSerializer.class);
            KafkaProducer<String, Account> producer = new KafkaProducer<>(props);
            String topic = "acc-topic";

            for (int i = 5001; i <= 5010; i++) {
                    int cust = 23;
                    Account account = new Account(i + cust, i, "CA", "Hebbal");
                    ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "CA", account);
                    producer.send(record);
            }
            for (int i = 6001; i <= 6010; i++) {
                    int cust = 56;
                    Account account = new Account(i + cust, i, "SB", "Ulsoor");
                    ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "SB", account);
                    producer.send(record);
            }

            for (int i = 7001; i <= 7010; i++) {
                    int cust = 67;
                    Account account = new Account(i + cust, i, "RD", "Hebbal");
                    ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "RD", account);
                    producer.send(record);
            }
            for (int i = 8001; i <= 8010; i++) {
                    int cust = 78;
                    Account account = new Account(i + cust, i, "Loan", "Panjagutta");
                    ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "Loan", account);
                    producer.send(record);
            }
            producer.close();
            System.out.println("Messages sent");

    }        
}