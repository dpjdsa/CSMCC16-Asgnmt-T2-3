package core;
import java.util.List;
import java.util.ArrayList;
/**
 * Class used to store list of passenger records
 * @author BD837672
 * @version 20th December 2020
 */
public class PassengerList {
    private List<Passenger> pList;
    /** Constructor
     * 
     */
    public PassengerList()
    {
        pList = new ArrayList<>();
    }
    // Adds passenger record to passenger record list
    public void addPassenger(Passenger passengerIn)
    {
        pList.add(passengerIn);
    }
    // Gets size of passenger record list
    public int size()
    {
        return pList.size();
    }
    // Gets passenger record at positionIn in list
    public Passenger getPassenger(int positionIn){
        return pList.get(positionIn);
    }
    // For output
    @Override
    public String toString()
    {
        return pList.toString();
    }
}
