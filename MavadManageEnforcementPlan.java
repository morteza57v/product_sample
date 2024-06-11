package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadManageEnforcementPlan {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadManageEnforcementPlan(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_SOCIAL_SEC_IMPROV_PLAN_ID, FLD_PLAN_EXECUTION_METHOD_ID, FLD_PLAN_TYPE_ID,\n" +
                "       FLD_CITY_ID, FLD_PROVINCE_ID,FLD_PLAN_TITLE, FLD_COMMAND_LETTER_NO,\n" +
                "       FLD_COMMAND_LETTER_DATE, FLD_EXECUTION_DATE, FLD_REGISTER_DATE, FLD_DESC,\n" +
                "       FLD_EXECUTION_ADDRESS,FLD_EXECUTION_END_DATE from " + sourceSchema + ".TBL_SOCIAL_SEC_IMPROVMENT_PLAN";

        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema + ".NAP_MANAGEMENT_ENFORCEMENT_PLAN(" +
                "MANAGEMENT_ENFORCEMENT_PLAN_ID," +
                "AGREE," +
                "CHIEF_AGREE," +
                "CHIEF_COMMENT," +
                "CREATED_BY," +
                "CREATION_TIME," +
                "DELETED," +
                "DESCRIPTION," +
                "DISCOVERY_FOLDER_NUMBER ," +
                "END_DATE ," +
                "HEADQUARTER_LICENSE_CODE ," +
                "HEADQUARTER_LICENSE_CODE_DATE ," +
                "JUDICIAL_LICENSE_CODE ," +
                "JUDICIAL_LICENSE_DATE ," +
                "LAST_UPDATE_TIME ," +
                "LAST_UPDATED_BY ," +
                "MANAGER_COMMENT ," +
                "PLAN_CODE ," +
                "PLAN_NAME ," +
                "PROCESS_STATUS ," +
                "REGISTRATION_DATE ," +
                "REQUIRE_DESCRIPTION_PLAN ," +
                "START_DATE ," +
                "CITY_ID ," +
                "COUNTRY_ID ," +
                "PLAN_EXECUTOR_ID ," +
                "PLAN_GROUP_ID ," +
                "PROVINCE_ID ," +
                "REGION_ID ," +
                "STATUS ," +
                "SUBJECT_PLAN_ID)\n" +
                "values (?,'YES','YES',null,'root',sysdate,0,?,null,?,?,?," +
                "null,null,sysdate,'root',null,?,?,null,?,null,?,?,1,null,?,?,null,'ACTIVE',?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_SOCIAL_SEC_IMPROV_PLAN_ID = resultSet.getString("FLD_SOCIAL_SEC_IMPROV_PLAN_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_PLAN_EXECUTION_METHOD_ID = resultSet.getString("FLD_PLAN_EXECUTION_METHOD_ID");
            String FLD_PLAN_TYPE_ID = resultSet.getString("FLD_PLAN_TYPE_ID");
            String FLD_CITY_ID = resultSet.getString("FLD_CITY_ID");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
            String FLD_PLAN_TITLE = resultSet.getString("FLD_PLAN_TITLE");
            String FLD_COMMAND_LETTER_NO = resultSet.getString("FLD_COMMAND_LETTER_NO");
            Date FLD_COMMAND_LETTER_DATE = resultSet.getDate("FLD_COMMAND_LETTER_DATE");
            Date FLD_EXECUTION_DATE = resultSet.getDate("FLD_EXECUTION_DATE");
            Date FLD_REGISTER_DATE = resultSet.getDate("FLD_REGISTER_DATE");
            Date FLD_EXECUTION_END_DATE = resultSet.getDate("FLD_EXECUTION_END_DATE");


            insertStmt.setLong(1, Long.parseLong(FLD_SOCIAL_SEC_IMPROV_PLAN_ID));
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setDate(3, FLD_EXECUTION_END_DATE);
            insertStmt.setString(4, FLD_COMMAND_LETTER_NO);
            insertStmt.setDate(5, FLD_COMMAND_LETTER_DATE);
            insertStmt.setString(6, FLD_PLAN_TITLE);
            insertStmt.setString(7, FLD_PLAN_TITLE);
            insertStmt.setDate(8, FLD_REGISTER_DATE);
            insertStmt.setDate(9, FLD_EXECUTION_DATE);
            insertStmt.setString(10, FLD_CITY_ID);
            insertStmt.setString(11, FLD_PLAN_EXECUTION_METHOD_ID);
            insertStmt.setString(12, FLD_PROVINCE_ID);
            insertStmt.setString(13, FLD_PLAN_TYPE_ID);

            insertStmt.executeQuery();

            connectionFactory.closeStatement(insertStmt);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }


}
