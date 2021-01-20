package core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.*;

/**
 * An abstract Combiner class defining generic combine functionality
 *
 * Areas for improvement:
 * - Implement runnable interface for execution as a thread
 * - Replace operations on the global map object with thread-safe alternatives
 */
public abstract class Combiner {
    // Intermediate records for this combiner instance to process
    protected ConcurrentHashMap records;

    // Default constructor
    public Combiner() {}

    // Setters
    public void setRecords(ConcurrentHashMap records) {
        this.records = records;
    }

    // Execute the reduce function for each key-value pair in the intermediate results output by the mapper
    public void run() {
        Iterator iterator = records.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<String, List<Object>> entry = (Map.Entry) iterator.next();
            combine(entry.getKey(), entry.getValue());
        }
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void combine(String key, List values);

    // Simply replace the intermediate and final result for each key
    // Map <KEY, List<VALUES>> -> Map <KEY, VALUE>
    public void EmitIntermediate(String key, Object value) {
        Job.map1.put(key, value);
    }
    public void EmitIntermediate3(String key, Object value) {
        // Get the existing values linked to the observed key else create a new map entry with an empty list
        List values;
        if(Job.map1.containsKey(key)) {
            values = (List) Job.map1.get(key);
        } else {
            values = new ArrayList<>();
            Job.map1.put(key, values);       
        }
        // Add the new value to the list
        values.add(value);        
    }
}
