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
package io.confluent.examples.streams

import java.util.Properties

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that shows how to aggregate messages via `groupBy()` and `aggregate()`.
  *
  * See [[AggregateIntegrationTest]] for the equivalent Java example.
  */
class AggregateScalaIntegrationTest extends AssertionsForJUnit {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  private val privateCluster: EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  @Rule def cluster: EmbeddedSingleNodeKafkaCluster = privateCluster

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() {
    cluster.createTopic(inputTopic)
    cluster.createTopic(outputTopic)
  }

  @Test
  def shouldAggregate() {
    val inputValues: Seq[String] =
      Seq("stream", "all", "the", "things", "hi", "world", "kafka", "streams", "streaming")

    val expectedOutputRecords: Seq[KeyValue[String, Long]] = Seq(
      ("a", 3L),
      ("t", 9L),
      ("h", 2L),
      ("w", 5L),
      ("k", 5L),
      ("s", 22L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val builder = new StreamsBuilder
    val input: KStream[Array[Byte], String] = builder.stream[Array[Byte], String](inputTopic)
    val aggregated: KTable[String, Long] = input
      .groupBy(
        (key: Array[Byte], value: String) => Option(value) match {
          case Some(s) if s.nonEmpty => s.head.toString
          case _ => ""
        })
      // We could also use `reduce()` in this specific example, but we want to showcase `aggregate()`.
      .aggregate(0L)((aggKey: String, newValue: String, aggValue: Long) => aggValue + newValue.length)
    aggregated.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    //
    // Step 2: Produce some input data to the input topic.
    //
    writeInputData(inputValues)

    //
    // Step 3: Verify the application's output data.
    //
    val actualOutputRecords: java.util.List[KeyValue[String, Long]] = readOutputData(expectedOutputRecords.size)
    streams.close()

    import collection.JavaConverters._
    assert(actualOutputRecords === expectedOutputRecords.asJava)
  }

  private def writeInputData(inputRecords: Seq[String]): Unit = {
    val producerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p
    }
    import collection.JavaConverters._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputRecords.asJava, producerConfig)
  }

  private def readOutputData(numExpectedRecords: Int): java.util.List[KeyValue[String, Long]] = {
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregation-scala-integration-test-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
      p
    }
    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, numExpectedRecords)
  }

}