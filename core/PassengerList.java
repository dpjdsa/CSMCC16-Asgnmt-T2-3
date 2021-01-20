package core;
import java.util.List;
import java.util.ArrayList;
/**
 * Class used to store list of passengers
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
    // Adds passenger to passenger list
    public void addPassenger(Passenger passengerIn)
    {
        pList.add(passengerIn);
    }
    // Returns size of passenger list
    public int size()
    {
        return pList.size();
    }
    // Used for printing passenger list
    @Override
    public String toString()
    {
        return pList.toString();
    }
}
