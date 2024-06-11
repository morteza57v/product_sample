package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadManageEnforcementInspection {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadManageEnforcementInspection(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CASE_ID,FLD_SOCIAL_SEC_IMPROV_PLAN_ID from "
                +sourceSchema+".TBL_CASES where FLD_SOCIAL_SEC_IMPROV_PLAN_ID is not null and FLD_SOCIAL_SEC_IMPROV_PLAN_ID != 6019363" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
//        String insertQuery = "Insert into " + targetSchema +".NAP_PLAN_GROUP(plan_group_id,title)\n" +
//                "values (?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

//            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            String FLD_SOCIAL_SEC_IMPROV_PLAN_ID = resultSet.getString("FLD_SOCIAL_SEC_IMPROV_PLAN_ID");


            String selectInspection = "select FOLDER_ID from "
                    +targetSchema+".NAP_INSPECTION_MEETING_NOTE where FOLDER_ID = "+ FLD_CASE_ID ;

            PreparedStatement preparedStatement = connectiontarget.prepareStatement(selectInspection);
            ResultSet resultInspection = preparedStatement.executeQuery();

            while (resultInspection.next()){
                String FOLDER_ID = resultInspection.getString("FOLDER_ID");
                String sql = "update "+targetSchema+".NAP_INSPECTION_MEETING_NOTE set MANAGEMENT_ENFORCEMENT_PLAN_ID = "
                        +FLD_SOCIAL_SEC_IMPROV_PLAN_ID+" where FOLDER_ID = "+FOLDER_ID;
                PreparedStatement prepare = connectiontarget.prepareStatement(sql);
                prepare.execute();
                connectionFactory.closeStatement(prepare);
            }

//            insertStmt.setLong(1, Long.parseLong(FLD_CASE_ID));
//            insertStmt.setString(2, FLD_DESC);

//            insertStmt.executeQuery();
            connectionFactory.closeResultSet(resultInspection);
            connectionFactory.closeStatement(preparedStatement);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
