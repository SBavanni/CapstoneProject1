package com.capstone.serializer;


import com.capstone.domain.Account;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class AccountSerializer implements Serializer<Account> {

	private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, Account account) {
        System.out.println("serializing employee with id " + account.getAccountNumber());
        byte[] array=null;
        try {
            array=objectMapper.writeValueAsBytes(account);
        } catch (JsonProcessingException a) {
            throw new RuntimeException(a);
        }
        return array;
    }
}