/*
 *  Copyright (C) 2013-2017 Alvaro Monge <alvaro.monge@csulb.edu>
 *  California State University Long Beach (CSULB) ALL RIGHTS RESERVED
 * 
 * Licensed under the Open Software License (OSL 3.0).
 *     http://opensource.org/licenses/AFL-3.0
 * 
 *  Use of this software is authorized for CSULB students in Dr. Monge's classes, so long
 *  as this copyright notice remains intact. Students must request permission from Dr. Monge
 *  if the code is to be used in other venues outside of Dr. Monge's classes.
 * 
 *  This program is distributed to CSULB students in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * 
 */
package toybankjdbcdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alvaro Monge <alvaro.monge@csulb.edu>
 */
public class ToybankJDBCDemo {

   /**
    * Logger object for logging error messages and warnings
    */
   private final static Logger LOGGER = Logger.getLogger(ToybankJDBCDemo.class.getName());
   
   /**
    * JDBC MySQL Driver
    */
   private final static String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";
   /**
    * JDBC Connection URL for the toybank database, using the MySQL driver
    */
   private final static String MYSQL_JDBC_URL = "jdbc:mysql://cecs-db01.coe.csulb.edu:3306/toybank";

   /**
    * Other JDBC Driver and connection string combinations
    */
   // JDBC SQLite Driver 
   private final static String SQLITE_DRIVER = "org.sqlite.JDBC"; 
   private final static String SQLITE_JDBC_URL = "jdbc:sqlite:/Users/alvaro/sqlite/pa2.db";

   // JDBC JavaDB (Derby) Driver -- Client/Server (Network)
   private final static String DERBY_NETWORK_DRIVER = "org.apache.derby.jdbc.ClientDriver"; 
   private final static String DERBY_NETWORK_JDBC_URL = "jdbc:derby://localhost:1527/toybank";

   // JDBC JavaDB (Derby) Driver -- Embedded 
   private final static String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
   private final static String DERBY_EMBEDDED_JDBC_URL = "jdbc:derby:/Users/alvaro/.netbeans-derby/toybank";
   // Note the absolute  path to the location of the DB (assumes Unix-based OS)


   private final static String JDBC_DRIVER = DERBY_EMBEDDED_DRIVER;
   private final static String JDBC_URL = DERBY_EMBEDDED_JDBC_URL;


   /**
    * Query to retrieve all loans including the owner of each loan
    */
   private final static String SQL_FIND_ALL_LOANS
           = "SELECT fname, lname, loan_number, amount "
           + "FROM customers INNER JOIN loans ON id=owner_id";
   /**
    * Query to retrieve the loans (number and amount) of a specified customer
    */
   private final static String SQL_FIND_LOANS_TO_ADJUST
           = "SELECT loan_number, amount "
           + "FROM customers INNER JOIN loans ON id=owner_id "
           + "WHERE fname=? AND lname=?";
   /**
    * Update statement to change the amount of a specified loan
    */
   private final static String SQL_ADJUST_LOAN
           = "UPDATE loans "
           + "SET amount = ? "
           + "WHERE loan_number = ?";

   /**
    * Scanner object attached to the user's input (via keyboard)
    */
   private final Scanner userInput = new Scanner(System.in);

   /**
    * DB connection object field.
    */
   private Connection connection = null;

   /**
    * A small demo of the JDBC API to open a DB connection, execute queries and
    * updates.
    *
    * @param args the command line arguments
    */
   public static void main(String[] args) {
      LOGGER.setLevel(Level.INFO);

      ToybankJDBCDemo demo = new ToybankJDBCDemo();

      demo.connectToDB();

      if (demo.isConnected()) {
         demo.displayAllLoans();

         demo.adjustLoans();

         // Connection is still open... so can execute other statements.
         demo.commitChanges();
         demo.closeConnection();
      } else {
         System.out.println("Exiting, connection was not established!");
      }
   }

   /**
    * Loads the JDBC driver class, no longer required for Type 4 JDBC Drivers
    */
//   public ToybankJDBCDemo() {
//      try {
//         Class.forName(JDBC_DRIVER);
//      } catch (ClassNotFoundException cnfe) {
//         LOGGER.log(Level.SEVERE, "Unable to load JDBC driver, due to error: {0}", cnfe);
//         System.exit(1);
//      }
//   }

   /**
    * Gets a connection to the database using the JDBC driver.
    */
   public void connectToDB() {
      try {
         System.out.print("Connecting to DB:: name of database user: ");
         String dbUser = userInput.nextLine();
         System.out.print("Connecting to DB:: password: ");
         String dbPassword = userInput.nextLine();

         // TODO: connecting to DB, disable autocommit, set trasation isolation level
         connection = DriverManager.getConnection(
                 JDBC_URL, // The URL specifying the location of the server and name of database
                 dbUser,
                 dbPassword);

         connection.setAutoCommit(false); // commits must be explicit, rather than implicit
         connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      } catch (SQLException sqe) {
         LOGGER.log(Level.SEVERE, "Unable to establish a connection to the database due to error {0}", sqe.getMessage());
         connection = null;
      }
   }

   /**
    * Determines whether or not the program is connected to the database
    *
    * @return true if connected, false otherwise
    */
   public boolean isConnected() {
      boolean status = false;
      try {
         status = connection != null && !connection.isClosed();
      } catch (SQLException ex) {
         LOGGER.log(Level.SEVERE, "Unable to check connection status: {0}", ex);
      }

      return status;
   }

   /**
    * Close the connection associated with this object
    */
   public void closeConnection() {
      //It's important to close the connection when you are done with it
      try {
         connection.close();
         DriverManager.getConnection(JDBC_URL + ";shutdown=true");  // shut Derby down, See: https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html#shutdown
      } catch (SQLException ignore) {
         LOGGER.log(Level.SEVERE, "Unable to close DB connection due to error {0}", ignore.getMessage());

         // Alternative: Cascade the original exception (or rewrap it as a more specific exception)
         // Here, we just log it 
      }
   }

   /**
    * Issues a commit to the DB to make permanent all changes made to the DB.
    */
   public void commitChanges() {
      try {
         connection.commit();
      } catch (SQLException sqle) {
         LOGGER.log(Level.SEVERE, "Unable to commit changes to the DB due to error {0}", sqle.getMessage());
      }
   }

   /**
    * Executes a SELECT statement on the opened connection and displays its
    * retrieved information
    */
   public void displayAllLoans() {
      try (Statement stmt = connection.createStatement(); // TODO: explain try with resources
           ResultSet   rs = stmt.executeQuery(SQL_FIND_ALL_LOANS)) {

         System.out.print("\n\nThe following are the loans in ToyBank:\n\n");
         while (rs.next()) {  // TODO: iterating through the ResultSet
            displayOneLoan(rs);
         }
         System.out.print("\n\n");
      } catch (SQLException sqle) {
         LOGGER.log(Level.SEVERE, "Unable to execute DB statement due to error {0}", sqle.getMessage());
      }
   }

   /**
    * Process one row in the ResultSet of the SQL query by displaying it on the
    * console.
    *
    * @param rs the ResultSet object position at a valid row.
    */
   private void displayOneLoan(ResultSet rs) throws SQLException {
      // Note the two different ways in which to retrieve the value of a column
      // Either by specifying the column number or by specifying the column name

      String name = rs.getString(1) + " " + rs.getString(2);

      System.out.printf("%s owns Loan # %s, in the amount of $%.2f\n",
              name, rs.getString(3), rs.getFloat("amount"));

   }

   /**
    * Asks user to provide customer name whose loans can then be adjusted. Uses
    * a JDBC prepare statement to retrieve all loan information given the name
    * of a customer. For each loan, prompts whether or not loan is to be
    * adjusted and by what value.
    */
   public void adjustLoans() {
      // TODO: demo of two PreparedStatements: query and update
      try (PreparedStatement findLoansByCustomer = connection.prepareStatement(SQL_FIND_LOANS_TO_ADJUST);
           PreparedStatement updateStatement = connection.prepareStatement(SQL_ADJUST_LOAN);) {

         do {
            System.out.println("Name of customer whose loans are to be adjusted (first last): ");
            String line = userInput.nextLine();
            String[] name = line.split(" ");

            // TODO: binding the PreparedStatement parameters to values and calling executeQuery
            findLoansByCustomer.setString(1, name[0]);
            findLoansByCustomer.setString(2, name[1]);

            ResultSet loansToProcess = findLoansByCustomer.executeQuery();

            while (loansToProcess.next()) {
               int loanNumber = loansToProcess.getInt(1);
               float loanAmount = loansToProcess.getFloat(2);

               System.out.printf("Loan # %d in the amount of $%.2f\n", loanNumber, loanAmount);
               System.out.println("Would you like to adjust this loan (y/n)? ");

               if (isResponseYes(userInput.nextLine())) {
                  System.out.println("By how much should it be adjusted?");
                  System.out.print("Enter a positive or negative amount: ");
                  String inputAmount = userInput.nextLine();
                  float adjustment = Float.valueOf(inputAmount);

                  if ((adjustment < 0 && Math.abs(adjustment) < loanAmount) || (adjustment > 0)) {  // adjust only if in range
                     loanAmount += adjustment;
                     int rowsAffected = adjustLoan(updateStatement, loanNumber, loanAmount);
                     if (rowsAffected == 1)
                        System.out.printf("Loan successfully adjusted, the new loan amount is $%.2f\n", loanAmount);
                  } else
                     System.out.println("The loan cannot be adjusted by that amount. Resuming with next loan");
               }
            }
            System.out.println("Continue with another customer's loans (y/n)? ");
         } while (isResponseYes(userInput.nextLine()));

      } catch (SQLException sqle) {
         LOGGER.log(Level.SEVERE, "Unable to process result due to error {0}", sqle.getMessage());
      }

   }

   /**
    * Set a loan's amount to the adjustedLoanAmount
    *
    * @param loanNumber the unique identifier of the loan to be adjusted
    * @param adjustedLoanAmount the new amount of the loan
    * @return the number of rows affected in the database
    * @throws SQLException if a database error occurs or makes calls on a closed
    * connection, etc.
    */
   private int adjustLoan(PreparedStatement updateLoan, int loanNumber, float adjustedLoanAmount) throws SQLException {
      updateLoan.setFloat(1, adjustedLoanAmount);
      updateLoan.setInt(2, loanNumber);
      return updateLoan.executeUpdate();  // TODO: call to executeUpdate on a PreparedStatement
   }

   /**
    * Tests the String to see if it's a yes response, ignoring case.
    *
    * @param userResponse the String with the user's input
    * @return true if it starts with y or Y, false otherwise
    */
   public static boolean isResponseYes(String userResponse) {
      boolean result = false;
      if (null != userResponse) {
         char firstCharacter = userResponse.charAt(0);
         result = (firstCharacter == 'y' || firstCharacter == 'Y');
      }

      return result;
   }
}
