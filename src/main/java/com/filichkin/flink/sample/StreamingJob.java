/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.filichkin.flink.sample;

import com.filichkin.flink.sample.functions.EventAggregateFunction;
import com.filichkin.flink.sample.functions.EventMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * Sample Flink example
 *
 * <p>Read messages(eventNames) from Kafka topic "flink-source" remove events that start with
 * "ignore" window it for 30 sec select the most popular event. send popular event to Kafka topic
 * "flink-sink"
 */
public class StreamingJob {

    public static final String KAFKA_SOURCE_TOPIC = "flink-source";
    public static final String KAFKA_SINK_TOPIC = "flink-sink";
    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String KAFKA_GROUP_ID = "test";

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final int parallelism = 1;
        final Configuration configuration = new Configuration();
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 100);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, configuration);


        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KAFKA_SERVER);
        kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);


        env.addSource(new FlinkKafkaConsumer010<>(KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), kafkaProperties))
//                .slotSharingGroup("Kafka-Source")
                .name("Kafka-Source")
                .map(new EventMapFunction())
//                .slotSharingGroup("MAP-Filter")
                .name("MAP function")
                .filter(event -> !event.getName().startsWith("ignore"))
//                .slotSharingGroup("MAP-Filter")
                .addSink(
                        new FlinkKafkaProducer010<>(
                                "localhost:9092", KAFKA_SINK_TOPIC, (event) -> event.toString().getBytes()))
//                .slotSharingGroup("Flink sink")
                .name("Kafka Sink");

//                DataStream<Event> mostPopularEventIn30Second =  env.addSource(new FlinkKafkaConsumer010<>(KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), kafkaProperties)).map(new EventMapFunction()).name("MAP function").slotSharingGroup("Map")
//                        .filter(event -> !event.getName().startsWith("ignore")).slotSharingGroup("Filter")
//                        .timeWindowAll(Time.seconds(30))
//                        .aggregate(new EventAggregateFunction()).name("Aggregation").slotSharingGroup("Window Aggregation");
//        //        mostPopularEventIn30Second.print();
//                mostPopularEventIn30Second.addSink(new FlinkKafkaProducer010<>("localhost:9092", KAFKA_SINK_TOPIC, (event) -> event.getName().getBytes())).name("Kafka Sink").slotSharingGroup("Kafka Sink");

        env.execute("Flink Streaming Java API Skeleton");
    }
}
