package core;
import java.util.Map;
import java.util.*;
import java.util.regex.*;
import java.io.*;
/** Class used to store list of airports
 * @author BD837672
 * @version 20th December 2020
 */
public class AirportList {
    private Map<String, Airport> apList;
    public int MAX;
    /**Constructor
     * 
     */
    public AirportList(int MaxIn)
    {        
        apList=new HashMap<>();
        MAX=MaxIn;
    }
    // Adds airport to airport list
    public void addAirport(Airport airportIn)
    {
        apList.put(airportIn.getCode(),airportIn);
    }
    // Returns the size of the airport list
    public int size()
    {
        return apList.size();
    }
    // Gets name of airport from code
    public String getName(String airportCodeIn)
    {
        return apList.get(airportCodeIn).getName();
    }
    // Returns the set of airport code keys
    public Set<String> getKeys()
    {
        return apList.keySet();
    }
    // Used for printing airport list
    public String toString()
    {
        return apList.toString();
    }
}
