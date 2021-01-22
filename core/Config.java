package core;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.concurrent.*;


/**
 * MapReduce Job Configuration
 * Stores the file specifications provided at run-time and
 * uses reflection to set objective-specific mapper and reducer classes.
 *
 * @author BD837672
 * @version 22nd January 2021
 */
public class Config {
    // Input files to process
    private File[] files;

    // Classes to implement job-specific map and reduce functions
    private Class mapper, reducer, combiner;

    // Constructor
    public Config(Class mapper, Class reducer, Class combiner) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.combiner = combiner;
    }
    // Reads the Passenger List as if it was a file
    protected static int read(PassengerList pListIn){
        String line;
        for (int i=0;i<pListIn.size();i++){
            Passenger passenger=pListIn.getPassenger(i);
            line=passenger.toCSV();
            Job.record.add(line);
        }
        System.out.println("Size of file = "+Job.record.size());
        return Job.record.size();
    }
    // Return the list of files to process
    protected File[] getFiles() {
        return this.files;
    }

    
    // Using reflection get an instance of the mapper operating on a specified file
    //protected Mapper getMapperInstance(File file) throws Exception {
    protected Mapper getMapperInstance() throws Exception {
        Mapper mapper = (Mapper) this.mapper.getConstructor().newInstance();
        return mapper;
    }

    // Using reflection get an instance of the reducer operating on a chunk of the intermediate results
    protected Reducer getReducerInstance(ConcurrentHashMap results) throws Exception {
        Reducer reducer = (Reducer) this.reducer.getConstructor().newInstance();
        reducer.setRecords(results);
        return reducer;
    }
    // Using reflection get an instance of the combiner operating on a chunk of the intermediate results
    protected Combiner getCombinerInstance(ConcurrentHashMap results) throws Exception {
        Combiner combiner = (Combiner) this.combiner.getConstructor().newInstance();
        combiner.setRecords(results);
        return combiner;
    }
}
