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
    // Gets size of airport list
    public int size()
    {
        return apList.size();
    }
    // Gets name of airport based on airport code
    public String getName(String airportCodeIn)
    {
        return apList.get(airportCodeIn).getName();
    }
    // Returns Set of airport codes
    public Set<String> getKeys()
    {
        return apList.keySet();
    }
    // Checks that airport code is valid
    public boolean validCode(String codeIn)
    {
        if (apList.containsKey(codeIn)){
            return true;
        }   else {
            return false;
        }
    }
    // Returns HashSet of airport codes
    public HashSet<String> getKeysHashSet()
    {
        HashSet<String> set=new HashSet<String>();
        set.addAll(apList.keySet());
        return set; 
    }
    // For output
    public String toString()
    {
        return apList.toString();
    }
}
