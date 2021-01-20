# CSMCC16-Asgnmt-T2-3
Repository of Java Code for Task 2 and 3 of CSMCC16 Assignment

/**
 * Assignment Task 2:
 * Assembles a list of flights with passengers by flight.
 * A single threaded solution which creates a mapper to process the passenger list.
 * No need for the combiner or reducer phases.
 * Also satisfies task 3 by summing the passenger list to give the total number of
 * passengers on each flight.
 * 
 * To run:
 * java Task_2.java <file>
 *     i.e. java Task_2.java AComp_Passenger_data_no_error.csv
 *
 * Potential Areas for improvement:
 * - Multi-threading
 *   - Partitioning of input for parallel processing
 *   - Synchronisation and thread-safe operations
 * - Error checking and handling
 */
