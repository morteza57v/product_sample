package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadVehicle {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadVehicle(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CAR_FINDINGS_ID,FLD_CHASSIS_NO,FLD_CAR_NO,FLD_DESC," +
                "FLD_CAR_CATAGORY_TYPE_ID,FLD_CAR_TYPE,FLD_COLOR_TYPE_ID," +
                "FLD_CASE_ID,FLD_BODY_NO,FLD_DATE from " + sourceSchema + ".TBL_CAR_FINDINGS";

        String insertQuery = "Insert into " + targetSchema + ".NAP_VEHICLE( " +
                "    VEHICLE_ID                    ,\n" +
                "    CHASSIS_NO                 ,\n" +
                "    CREATED_BY                 ,\n" +
                "    CREATION_TIME              ,\n" +
                "    DELETED                    ,\n" +
                "    DESCRIPTION                ,\n" +
                "    ENGINE_NO                  ,\n" +
                "    LAST_UPDATE_TIME           ,\n" +
                "    LAST_UPDATED_BY            ,\n" +
                "    PLAQUE_NO                  ,\n" +
                "    VEHICLE_WEIGHT_TYPE        ,\n" +
                "    VIN                        ,\n" +
                "    VIN_NUMBER                 ,\n" +
                "    INSPECTION_MEETING_NOTE_ID ,\n" +
                "    PERSON_ID                  ,\n" +
                "    STATUS                     ,\n" +
                "    VEHICLE_BRAND_ID           ,\n" +
                "    VEHICLE_CLASS_ID           ,\n" +
                "    VEHICLE_COLOR_ID           ,\n" +
                "    VEHICLE_TYPE_ID  )\n" +
                "values ( ?,?,'root',?,0,?," +
                "null,sysdate,'root',?,?,null," +
                "null,?,null,'ACTIVE',?,null,?,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_CAR_FINDINGS_ID = resultSet.getString("FLD_CAR_FINDINGS_ID");
            System.out.println(FLD_CAR_FINDINGS_ID);
            String FLD_CAR_CATAGORY_TYPE_ID = resultSet.getString("FLD_CAR_CATAGORY_TYPE_ID");
            String FLD_CHASSIS_NO = resultSet.getString("FLD_CHASSIS_NO");
            String FLD_CAR_NO = resultSet.getString("FLD_CAR_NO");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_CAR_TYPE = resultSet.getString("FLD_CAR_TYPE");
            String FLD_COLOR_TYPE_ID = resultSet.getString("FLD_COLOR_TYPE_ID");
            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            Date FLD_DATE = resultSet.getDate("FLD_DATE");

            if (FLD_DATE == null) {
                java.util.Date date = new java.util.Date();
                java.sql.Date sqlDate = new java.sql.Date(date.getDate());
                FLD_DATE = sqlDate;
            }

            if (FLD_CAR_CATAGORY_TYPE_ID != null) {
                if (FLD_CAR_CATAGORY_TYPE_ID.equals("1")) {
                    FLD_CAR_CATAGORY_TYPE_ID = "LIGHT";
                } else if (FLD_CAR_CATAGORY_TYPE_ID.equals("2")) {
                    FLD_CAR_CATAGORY_TYPE_ID = "HEAVY";
                } else if (FLD_CAR_CATAGORY_TYPE_ID.equals("3")) {
                    FLD_CAR_CATAGORY_TYPE_ID = "MEDIUM";
                }
            }

            String selectQuery1 = "select INSPECTION_MEETING_NOTE_ID from " + targetSchema + ".NAP_INSPECTION_MEETING_NOTE WHERE  FOLDER_ID =" + FLD_CASE_ID;
            PreparedStatement selectStmt4 = connectiontarget.prepareStatement(selectQuery1);
            ResultSet resultSet4 = selectStmt4.executeQuery();
            String INSPECTION_MEETING_NOTE_ID = null;
            while (resultSet4.next()) {
                INSPECTION_MEETING_NOTE_ID = resultSet4.getString("INSPECTION_MEETING_NOTE_ID");
            }

            connectionFactory.closeResultSet(resultSet4);
            connectionFactory.closeStatement(selectStmt4);


            insertStmt.setLong(1, Long.parseLong(FLD_CAR_FINDINGS_ID));
            insertStmt.setString(2, FLD_CHASSIS_NO);
            insertStmt.setDate(3, FLD_DATE);
            insertStmt.setString(4, FLD_DESC);
            insertStmt.setString(5, FLD_CAR_NO);
            insertStmt.setString(6, FLD_CAR_CATAGORY_TYPE_ID);
            insertStmt.setString(7, INSPECTION_MEETING_NOTE_ID);
            insertStmt.setString(8, FLD_CAR_TYPE);
            insertStmt.setString(9, FLD_COLOR_TYPE_ID);
//            insertStmt.setString(10, FLD_CAR_CATAGORY_TYPE_ID);


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
