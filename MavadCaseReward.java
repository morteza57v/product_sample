package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadCaseReward {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadCaseReward(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PROPER_REWARD_ID,FIRST_NAME,LAST_NAME,REWARD,\n" +
                "FLD_ENTER_DATE,\n" +
                "FLD_DESC,PER_CODE from " +sourceSchema+".TBL_PROPER_REWARD where PER_CODE is not null" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_CASE_REWARD(" +
                "    CASE_REWARD_ID ,\n" +
                "    AMOUNT      ,\n" +
                "    CREATED_BY ,\n " +
                "    CREATION_TIME ,\n " +
                "    DELETED ,\n " +
                "    DESCRIPTION ,\n " +
                "    LAST_UPDATE_TIME ,\n " +
                "    LAST_UPDATED_BY ,\n " +
                "    EMPLOYEE_ID ,\n " +
                "    STATUS \n " +
                "    )\n" +
                "values (?,?,'root',?,0,?,sysdate,'root',?,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_PROPER_REWARD_ID = resultSet.getString("FLD_PROPER_REWARD_ID");
            String REWARD = resultSet.getString("REWARD");
            Date FLD_ENTER_DATE = resultSet.getDate("FLD_ENTER_DATE");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String PER_CODE = resultSet.getString("PER_CODE");

            String selectEmployee ="select EMPLOYEE_ID from "+targetSchema+".NAP_EMPLOYEE where PERSONNEL_CODE ='"+PER_CODE+"'";

            PreparedStatement preparedStatement = connectiontarget.prepareStatement(selectEmployee);
            ResultSet rs = preparedStatement.executeQuery();

            while(rs.next()){

                String EMPLOYEE_ID = rs.getString("EMPLOYEE_ID");

                insertStmt.setLong(1, Long.parseLong(FLD_PROPER_REWARD_ID));
                insertStmt.setString(2, REWARD);
                insertStmt.setDate(3,FLD_ENTER_DATE);
                insertStmt.setString(4, FLD_DESC);
                insertStmt.setString(5, EMPLOYEE_ID);

                insertStmt.executeQuery();
            }

            connectionFactory.closeStatement(insertStmt);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
