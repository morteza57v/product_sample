package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadStaffReward {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadStaffReward(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select \t\n" +
                "FLD_STAFF_REWARD_ID,PER_CODE,\n" +
                "SALARY_CODELAST_NAME,REWARD,\n" +
                "FLD_ENTER_DATE,\n" +
                "FLD_DESC from " +sourceSchema+".TBL_STAFF_REWARD" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_STAFF_REWARD(" +
                "    STAFF_REWARD_ID ,\n" +
                "    ANTI_NARCOTICS_POLICE_REWARD_EXPERT_AGREE      ,\n" +
                "    ANTI_NARCOTICS_POLICE_REWARD_EXPERT_COMMENT      ,\n" +
                "    CREATED_BY ,\n " +
                "    CREATION_TIME ,\n " +
                "    DELETED ,\n " +
                "    DEPUTY_OF_PLAN_AND_DEVELOPMENT_AGREE ,\n " +
                "    DEPUTY_OF_PLAN_AND_DEVELOPMENT_COMMENT ,\n " +
                "    DESCRIPTION ,\n " +
                "    FARAJA_RESPONSIBLE_AGENT_AGREE ,\n " +
                "    FARAJA_RESPONSIBLE_AGENT_COMMENT ,\n " +
                "    LAST_UPDATE_TIME ,\n " +
                "    LAST_UPDATED_BY ,\n " +
                "    PROCESS_STATUS ,\n " +
                "    PROVINCE_BOSS_AGREE ,\n " +
                "    PROVINCE_BOSS_COMMENT ,\n " +
                "    PROVINCE_DISCIPLINARY_CHIEF_AGREE ,\n " +
                "    PROVINCE_DISCIPLINARY_CHIEF_COMMENT ,\n " +
                "    PROVINCE_RESPONSIBLE_AGENT_AGREE ,\n " +
                "    PROVINCE_RESPONSIBLE_AGENT_COMMENT ,\n " +
                "    PROVINCE_REWARD_MANAGER_AGREE ,\n " +
                "    PROVINCE_REWARD_MANAGER_COMMENT ,\n " +
                "    STATUS ,\n " +
                "    ADVANTAGES_OF_YEARS ,\n " +
                "    BASE_REWARD ,\n " +
                "    POLICE_TYPE ,\n " +
                "    TOTAL_AMOUNT\n " +
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

            String selectEmployee ="select EMPLOYEE_ID from "+sourceSchema+".NAP_EMPLOYEE where PERSONNEL_CODE ='"+PER_CODE+"'";

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
