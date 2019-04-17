/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that shows how to aggregate messages via `groupByKey()` and `reduce()`.
 *
 * This example can be adapted to structured (nested) data formats such as Avro or JSON in case you need to concatenate
 * only certain field(s) in the input.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ReduceIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldConcatenate() throws Exception {

    final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
        new KeyValue<>(456, "stream"),
        new KeyValue<>(123, "hello"),
        new KeyValue<>(123, "world"),
        new KeyValue<>(456, "all"),
        new KeyValue<>(123, "kafka"),
        new KeyValue<>(456, "the"),
        new KeyValue<>(456, "things"),
        new KeyValue<>(123, "streams")
    );


    final List<KeyValue<Integer, String>> expectedOutputRecords = Arrays.asList(
        new KeyValue<>(456, "stream all the things"),
        new KeyValue<>(123, "hello world kafka streams")
    );

    //
    // Step 1: Define and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "concatenation-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<Integer, String> input = builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()));
    final KTable<Integer, String> concatenated = input
        .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
        .reduce((v1, v2) -> v1 + " " + v2);
    concatenated.toStream().to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));

    try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)) {
      // Start the topology.
      streams.start();

      //
      // Step 2: Produce some input data to the input topic.
      //
      writeInputData(inputRecords);

      //
      // Step 3: Verify the application's output data.
      //
      final List<KeyValue<Integer, String>> actualRecords = readOutputData(expectedOutputRecords.size());
      assertThat(actualRecords).isEqualTo(expectedOutputRecords);
    }
  }

  private void writeInputData(final List<KeyValue<Integer, String>> inputRecords)
      throws ExecutionException, InterruptedException {
    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, inputRecords, producerConfig);
  }

  private List<KeyValue<Integer, String>> readOutputData(final int numExpectedRecords)
      throws InterruptedException {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "concatenation-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, numExpectedRecords);
  }

}