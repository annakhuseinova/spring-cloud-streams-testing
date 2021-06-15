package com.annakhuseinova.springcloudstreamstesting;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {"server.port=0"})
@EmbeddedKafka(partitions = 1, topics = {"input-topic", "output-topic"}, controlledShutdown = true)
class SpringCloudStreamsTestingApplicationTests {

    @BeforeAll
    static void beforeAll() {

    }

    @Test
    void contextLoads() {
    }

}
