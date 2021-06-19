package com.annakhuseinova.springcloudstreamstesting;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.*;

/**
 * We create an embedded kafka cluster
 * */
@RunWith(SpringRunner.class)
@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {"server.port=0"})
public class SpringCloudStreamsTestingApplicationTests {

    /**
     * count - the number of brokers
     * controlledShutdown - should the broker have controlled shutdown
     * */
    @ClassRule
    private static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, 1,
            "input-topic", "output-topic");

    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private static Consumer<String, String> consumer;

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> consumerProps = consumerProps("group", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic");
    }

    @Test
    void simpleProcessorApplicationTest() {
        Set<String> actualResultSet = new HashSet<>();
        Set<String> expectedResultSet = new HashSet<>();
        expectedResultSet.add("HELLO1");
        expectedResultSet.add("HELLO2");

        Map<String, Object> senderProps = producerProps(embeddedKafka);
        DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProps);
        try {
            KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory, true);
            template.setDefaultTopic("input-topic");

            template.sendDefault("hello1");
            template.sendDefault("hello2");

            int receivedAll = 0;

            while (receivedAll < 2){
                ConsumerRecords<String, String> consumerRecords = getRecords(consumer);
                receivedAll = receivedAll + consumerRecords.count();
                consumerRecords.iterator().forEachRemaining(record -> actualResultSet.add(record.value()));
            }

            assertThat(actualResultSet.equals(expectedResultSet)).isTrue();

        } finally {
            producerFactory.destroy();
        }

    }


    @AfterAll
    static void afterAll() {
        consumer.close();
    }
}
