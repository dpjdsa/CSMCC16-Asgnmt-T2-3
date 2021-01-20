package core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An abstract Reducer class defining generic reduce functionality
 *
 * Areas for improvement:
 * - Implement runnable interface for execution as a thread
 * - Replace operations on the global map object with thread-safe alternatives
 */
public abstract class Reducer {
    // Intermediate records for this reducer instance to process
    protected Map records;

    // Default constructor
    public Reducer() {}

    // Setters
    public void setRecords(Map records) {
        this.records = records;
    }

    // Execute the reduce function for each key-value pair in the intermediate results output by the mapper
    public void run() {
        Iterator iterator = records.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<String, List<Object>> entry = (Map.Entry) iterator.next();
            reduce(entry.getKey(), entry.getValue());
        }
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void reduce(String key, List values);

    // Simply replace the intermediate and final result for each key
    // Map <KEY, List<VALUES>> -> Map <KEY, VALUE>
    public void Emit(String key, Object value) {
        Job.map1.put(key, value);
    }
}