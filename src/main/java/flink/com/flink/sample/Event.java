package flink.com.flink.sample;

import java.util.Date;

public class Event  {
    private final String name;
    private final Date generationTime;

    public Event(String name, Date generationTime) {
        this.name = name;
        this.generationTime = generationTime;
    }

    public String getName() {
        return name;
    }

    public Date getGenerationTime() {
        return generationTime;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        return name.equals(event.name);
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", generationTime=" + generationTime +
                '}';
    }
}
