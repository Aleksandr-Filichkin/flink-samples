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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * Add message to Kafka/ message format "event+${currentDateTime}"
 */
public class KafkaProducerJob {

    public static final String KAFKA_SINK_TOPIC = "flink-source";
    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String KAFKA_GROUP_ID = "test";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KAFKA_SERVER);
        kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);

        env.addSource(new RichSourceFunction<String>() {
            private boolean running;
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                running=true;
                while (running) {
                    Thread.sleep(1);
                    sourceContext.collect("event-"+LocalDateTime.now());
                }
            }

            @Override
            public void cancel() {
                running=false;
            }
        }).addSink(new FlinkKafkaProducer010<String>(KAFKA_SERVER, KAFKA_SINK_TOPIC,new SimpleStringSchema()));

        env.execute("Start Flink Kafka Producer job ");
    }
}
