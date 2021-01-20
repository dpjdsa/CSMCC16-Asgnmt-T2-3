package core;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * An abstract Mapper class defining generic map functionality
 *
 * Areas for improvements:
 *  - Operate on portions of the input in parallel rather than on entire files
 *  - Implement the runnable interface for execution as a thread
 *  - Replace operations on the global map object with thread-safe alternatives
 */
public abstract class Mapper {
    // The input file for this mapper to process
    protected File file;

    // Default constructor
    public Mapper() {}

    // Set the input file for the given instance
    public void setFile(File file) {
        this.file = file;
    }

    // Execute the map function for each line of the provided file
    public void run() throws IOException {
        // Read the file
        Iterator<String> records = Config.read(this.file);
        // Call map() for each line
        while(records.hasNext())
            map(records.next());
    }

    // Abstract map function to be overwritten by objective-specific class
    public abstract void map(String value);

    // Adds values to a list determined by a key
    // Map<KEY, List<VALUES>>
    public void EmitIntermediate(String key, Object value) {
        // Get the existing values linked to the observed key else create a new map entry with an empty list
        List values;
        if(Job.map.containsKey(key)) {
            values = (List) Job.map.get(key);
            if (!values.contains(value)){
                // Add the value to the list if it is not in it already
                values.add(value);
            }
        } else {
            values = new ArrayList<>();
            Job.map.put(key, values);
            // Add the new value to the list
            values.add(value);
        }
    }
    public void EmitIntermediate2(String key, Object value) {
        // Only add the key value pair if it doesn't already exist in the list.
        List values;
        if(Job.map.containsKey(key)) {
            //values = (List) Job.map.get(key);
        } else {
            values = new ArrayList<>();
            Job.map.put(key, values);
        // Add the new value to the list
            values.add(value);
        }
    }
}