package flink.com.flink.sample.functions;

import flink.com.flink.sample.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregation function that calculates the most popular event (base on event name)
 */
public class EventAggregateFunction implements AggregateFunction<Event, MyAccumulator, Event> {

    @Override
    public MyAccumulator createAccumulator() {
        return new MyAccumulator();
    }

    @Override
    public MyAccumulator add(Event value, MyAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Event getResult(MyAccumulator accumulator) {
        Comparator<? super Map.Entry<Event, Integer>> maxValueComparator = Comparator.comparing(Map.Entry::getValue);
        return accumulator.getStringIntegerMap().entrySet().stream().max(maxValueComparator).get().getKey();
    }

    @Override
    public MyAccumulator merge(MyAccumulator a, MyAccumulator b) {
        return a.merge(b);
    }
}

class MyAccumulator {
    private final Map<Event, Integer> stringIntegerMap = new HashMap<>();

    public Map<Event, Integer> getStringIntegerMap() {
        return stringIntegerMap;
    }

    public void add(Event event, Integer count) {
        if (stringIntegerMap.containsKey(event)) {
            stringIntegerMap.put(event, stringIntegerMap.get(event) + count);
        } else {
            stringIntegerMap.put(event, count);
        }
    }

    public void add(Event event) {
        add(event, 1);
    }

    public MyAccumulator merge(MyAccumulator myAccumulator) {
        myAccumulator.getStringIntegerMap().entrySet().forEach((entry) -> add(entry.getKey(), entry.getValue()));
        return this;
    }
}
