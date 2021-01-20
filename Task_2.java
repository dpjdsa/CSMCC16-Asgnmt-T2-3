package objective;

import core.*;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.HashMap;
import java.util.Date;
import java.text.*;
/**
 * Assignment Tasks 2 and 3:
 * Creates the List of flights based on flight ID.
 * A multi-threaded solution which creates a mapper for processing the passenger records file.
 * Also error checks and corrects file input.
 *
 * To run:
 * java Task_2.java <files>
 *     i.e. java Task_2.java Top30_airports_LatLong.csv AComp_Passenger_data.csv
 *  
 */
class Task_2
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper function
    private static AirportList aList=new AirportList(30);
    private static PassengerList pList=new PassengerList();
    public static void main(String[] args) throws Exception {
        ReadAndErrorCheck.run(args);
        aList=ReadAndErrorCheck.getAList();
        pList=ReadAndErrorCheck.getPList();
        Config config = new Config(mapper.class, reducer.class, combiner.class);
        Job job = new Job(config,pList);
        job.run();
        DisplayFlightList(Job.getMap());
        DisplayTotPassengers(Job.getMap());
    }
    
    // Displays list of flights and passenger ids based on output map
    private static void DisplayFlightList(ConcurrentHashMap<String, Object> mapIn){
        System.out.println("***** Flight List *****");
        System.out.println("\n        Flight ID  ORIG  DEST  Dep Time   Arr Time   mins");
        for (Map.Entry<String,Object> entry : mapIn.entrySet()){
            String fltid = entry.getKey();
            List values=(List) entry.getValue();
            System.out.format("\nFlight: %-50s Passenger Ids: ",fltid);
            for (Object value:values){System.out.format(" %-10s",value);}
            System.out.println();
        }
        System.out.println("***** End of Flight List *****");
    }
    // Derives the total passengers on each flight and displays it form output map
    private static void DisplayTotPassengers(ConcurrentHashMap<String,Object> mapIn){
        System.out.println("\n***** Passengers per Flight *****");
        System.out.println("\n        Flight ID  ORIG  DEST  Dep Time   Arr Time   mins");
        for (Map.Entry<String,Object> entry : mapIn.entrySet()){
            String key = entry.getKey();
            List value=(List) entry.getValue();
            System.out.format("Flight: %-50s  Number of Passengers: %2d\n",key,value.size());
        }
    }
    // Flightid Passenger ID count mapper:
    // Converts dates from Unix Epoch time to H:M:S format using Simple Date Format
    // after having first converted times from seconds to milliseconds.
    // Output list of Passenger IDs for each occurrence of key.
    // KEY = Flightid+From Airport+ Dest Airport + Departure Time + Arrival Time + Flight Time
    // VALUE = Passenger ID
    public static class mapper extends Mapper {
        public void map(String line) {
            String[] Fields=line.split(",");
            long dept,arrt,fltt;
            dept = Long.parseLong(Fields[4])*1000;
            fltt = Long.parseLong(Fields[5])*60000;
            Date deptd = new Date(dept);
            Date arrtd = new Date(dept+fltt);
            String depthms= new SimpleDateFormat("HH:mm:ss").format(deptd);
            String arrthms = new SimpleDateFormat("HH:mm:ss").format(arrtd);
            EmitIntermediate(Fields[1]+" | "+Fields[2]+" | "+Fields[3]+" | "+depthms+" | "+arrthms+" | "+Fields[5],Fields[0]);
        }
    }
    // Airport Code count combiner (not used for this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class combiner extends Combiner {
        public void combine(Object key, List values) {
            EmitIntermediate3(key.toString().substring(0,3), values);
        }
    }
    // Airport Code count reducer (not used for this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class reducer extends Reducer {
        public void reduce(Object key, List values) {
            int count = 0;
            for (Object lst : values){
                for (Object value : (List) lst) count += (int) value;
                Emit(key, count);
            }
        }
    }
}