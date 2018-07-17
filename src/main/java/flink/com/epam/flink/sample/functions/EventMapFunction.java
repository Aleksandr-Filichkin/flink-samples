package flink.com.epam.flink.sample.functions;

import flink.com.epam.flink.sample.Event;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;


public class EventMapFunction implements MapFunction<String, Event> {

    @Override
    public Event map(String s) {
        return new Event(s,new Date());
    }
}
