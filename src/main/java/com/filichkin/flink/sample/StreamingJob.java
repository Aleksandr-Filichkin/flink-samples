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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * Sample Flink example
 *
 * Read messages(eventNames)  from Kafka topic "flink-source"
 * remove events that start with "ignore"
 * window it for 30 sec
 * select the most popular event.
 * send popular event to Kafka topic "flink-sink"
 *
 *
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "test");


        DataStream<String> kafkaSource = env.addSource(new FlinkKafkaConsumer010<>("flink-source", new SimpleStringSchema(), kafkaProperties)).name("Kafka-Source");


//        DataStream<Event> allGoodEvents = kafkaSource.map(new EventMapFunction()).name("MAP function")
//                .filter(event -> !event.getName().startsWith("ignore"));
//        allGoodEvents.print();
//        allGoodEvents.addSink(new FlinkKafkaProducer010<>("localhost:9092", "flink-sink", (event) -> event.getName().getBytes())).name("Kafka Sink");


        DataStream<Event> mostPopularEventIn30Second = kafkaSource.map(new EventMapFunction()).name("MAP function")
                .filter(event -> !event.getName().startsWith("ignore"))
                .timeWindowAll(Time.seconds(30))
                .aggregate(new EventAggregateFunction());
        mostPopularEventIn30Second.print();
        mostPopularEventIn30Second.addSink(new FlinkKafkaProducer010<>("localhost:9092", "flink-sink", (event) -> event.getName().getBytes())).name("Kafka Sink")


        env.execute("Flink Streaming Java API Skeleton");
    }


}
