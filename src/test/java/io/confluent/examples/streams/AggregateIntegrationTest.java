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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
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
 * End-to-end integration test that shows how to aggregate messages via `groupBy()` and `aggregate()`.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class AggregateIntegrationTest {

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
  public void shouldAggregate() throws Exception {

    final List<String> inputValues = Arrays.asList(
        "stream", "all", "the", "things", "hi", "world", "kafka", "streams", "streaming"
    );


    final List<KeyValue<String, Long>> expectedOutputRecords = Arrays.asList(
        new KeyValue<>("a", 3L),
        new KeyValue<>("t", 9L),
        new KeyValue<>("h", 2L),
        new KeyValue<>("w", 5L),
        new KeyValue<>("k", 5L),
        new KeyValue<>("s", 22L)
    );

    //
    // Step 1: Define and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<byte[], String> input = builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.String()));
    final KTable<String, Long> aggregated = input
        .groupBy(
            (key, value) -> (value != null && value.length() > 0) ? value.substring(0, 1).toLowerCase() : "",
            Grouped.with(Serdes.String(), Serdes.String()))
        // We could also use `reduce()` in this specific example, but we want to showcase `aggregate()`.
        .aggregate(
            () -> 0L,
            (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("agg-store").withValueSerde(Serdes.Long())
        );
    aggregated.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)) {
      // Start the topology.
      streams.start();

      //
      // Step 2: Produce some input data to the input topic.
      //
      writeInputData(inputValues);

      //
      // Step 3: Verify the application's output data.
      //
      final List<KeyValue<String, Long>> actualRecords = readOutputData(expectedOutputRecords.size());
      assertThat(actualRecords).isEqualTo(expectedOutputRecords);
    }
  }

  private void writeInputData(final List<String> inputRecords)
      throws ExecutionException, InterruptedException {
    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputRecords, producerConfig);
  }

  private List<KeyValue<String, Long>> readOutputData(final int numExpectedRecords)
      throws InterruptedException {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregation-integration-test-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, numExpectedRecords);
  }

}