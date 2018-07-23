package com.filichkin.flink.sample.functions;

import com.filichkin.flink.sample.Event;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.util.Date;


public class EventMapFunction implements MapFunction<String, Event> {

    @Override
    public Event map(String s) throws InterruptedException {
         //make some time expensive logic
        Thread.sleep(10);

        return new Event(s, LocalDateTime.now());
    }
}
