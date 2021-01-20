package core;
/** Class used to store details of single passenger on flight
 * 
 */
public class Passenger {
    private String passId;
    private String flightId;
    private String fromApt;
    private String destApt;
    private Double depTime;
    private Double flightTime;
    /** Constructor
     * 
     */
    public Passenger(String passIdIn,String flightIdIn, String fromAptIn, String destAptIn, Double depTimeIn, Double flightTimeIn)
    {
        passId=passIdIn;
        flightId=flightIdIn;
        fromApt=fromAptIn;
        destApt=destAptIn;
        depTime=depTimeIn;
        flightTime=flightTimeIn;
    }
    @Override
    public String toString()
    {
        return "( ID: " + passId + " Flight ID: " + flightId+" From: "+fromApt+" To: "+destApt+" Time: "+depTime+" Duration: "+flightTime;
    }
}
