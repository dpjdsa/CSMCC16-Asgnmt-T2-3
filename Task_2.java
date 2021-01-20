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
 * Creates List of flights based on Flight id.
 * A multi-threaded solution which creates a mapper for the input file and does not need combiner or reducer stages
 * Also satisfies task 3 by summing the passenger list to give the total number of
 * passengers on each flight.
 *
 * To run:
 * java Task_2.java <files>
 *     i.e. java Task_2.java AComp_Passenger_data_no_error.csv
 *
 * Potential Areas for improvement:
 * 
 * - Error checking and handling
 * 
 *  
 */
class Task_2
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper function
    private static AirportList aList=new AirportList(30);
    public static void main(String[] args) throws Exception {
        ReadAirports();
        Config config = new Config(args, mapper.class, reducer.class, combiner.class);
        Job job = new Job(config);
        job.run();
        DisplayFlightList(Job.getMap());
        DisplayTotPassengers(Job.getMap());
    }
    // Read in airports file
    public static void ReadAirports()
    {
        String csvFile1="Top30_airports_LatLong.csv";
        BufferedReader br = null;
        String line = "";
        try {
                br = new BufferedReader(new FileReader(csvFile1));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String name=Field[0];
                        String code=Field[1];
                        double lat=Double.parseDouble(Field[2]);
                        double lon=Double.parseDouble(Field[3]);
                        Airport airport = new Airport(name,code,lat,lon);
                        aList.addAirport(airport);
                    }
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        System.out.println(aList);
        System.out.println("*** no of airports is: "+aList.size());
    }
    // Displays the flight list based on the output map
    private static void DisplayFlightList(ConcurrentHashMap<String, Object> mapIn){
        System.out.println("\n****** Flight List *****");
        for (Map.Entry<String,Object> entry : mapIn.entrySet()){
            String fltid = entry.getKey().substring(0,9);
            List values=(List) entry.getValue();
            System.out.format("\nFlight: %-8s Passenger Ids: ",fltid);
            for (Object value:values){System.out.format(" %-10s",value);}
            System.out.println();
        }
        System.out.println("***** End of Flight List *****");
    }
    // Evaluates and displays the total number of passengers based on the output map.
    private static void DisplayTotPassengers(ConcurrentHashMap<String,Object> mapIn){
        System.out.println("\n        Flight ID  ORIG  DEST  Dep Time   Arr Time   mins");
        for (Map.Entry<String,Object> entry : mapIn.entrySet()){
            String key = entry.getKey();
            List value=(List) entry.getValue();
            System.out.format("Flight: %-50s  Number of Passengers: %2d\n",key,value.size());
        }
    }
    // Flightid Passenger ID count mapper:
    // Output Passenger ID for each occurrence of unique key.
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
    // Airport Code count combiner (not used in this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class combiner extends Combiner {
        public void combine(Object key, List values) {
            EmitIntermediate3(key.toString().substring(0,3), values);
        }
    }
    // Airport Code count reducer (not used in this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    
    public static class reducer extends Reducer {
        public void reduce(String key, List values) {
            int count = 0;
            for (Object lst : values){
                for (Object value : (List) lst) count += (int) value;
                Emit(key, count);
            }
        }
    }
}