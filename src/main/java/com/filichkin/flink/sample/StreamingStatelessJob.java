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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;

/**
 * Sample Flink example
 *
 * <p>
 *      1)Read messages(eventNames) from Kafka topic "flink-source"
 *      2)remove events that start with "ignore"
 *      3)make some transformation/calculation
 *      4)send request to Kafka topic "flink-sink"
 *  <p>
 */
public class StreamingStatelessJob {

    public static final String KAFKA_SOURCE_TOPIC = "flink-source";
    public static final String KAFKA_SINK_TOPIC = "flink-sink";
    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String KAFKA_GROUP_ID = "test-group12";

    public static void main(String[] args) throws Exception {
//        Uncomment to run it locally via IDE (https://stackoverflow.com/questions/51417258/how-to-increase-flink-taskmanager-numberoftaskslots-to-run-it-without-flink-serv)
//        final int parallelism = 1;
//        final Configuration configuration = new Configuration();
//        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 100);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, configuration);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KAFKA_SERVER);
        kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);


        env.addSource(new FlinkKafkaConsumer010<>(KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), kafkaProperties)).name("Kafka consumer").slotSharingGroup("Kafka and filter")
                .filter(x -> !x.startsWith("ignore")).name("filter").slotSharingGroup("Kafka and filter")
                .map(s -> {
                    Thread.sleep(1);//some calculation or request to some service/ DB
                    return new Event(s, LocalDateTime.now());
                }).name("Building event").slotSharingGroup("Building event")
                .addSink(new FlinkKafkaProducer010<Event>(KAFKA_SERVER, KAFKA_SINK_TOPIC, event -> event.toString().getBytes())).name("Kafka sink").slotSharingGroup("Result");
        env.execute();


    }
}
