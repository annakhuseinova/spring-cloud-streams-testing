package com.annakhuseinova.springcloudstreamstesting.services;

import com.annakhuseinova.springcloudstreamstesting.bindings.ListenerBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@EnableBinding(ListenerBinding.class)
public class ListenerService  {

    @StreamListener("process-in-0")
    @SendTo("process-out-0")
    public KStream<String, String> process(KStream<String, String> input){
        input.foreach((key, value)-> log.info("Received input: {}", value));
        return input.mapValues((ValueMapper<String, String>) String::toUpperCase);
    }
}
