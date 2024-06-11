package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadDogReq {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadDogReq(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_DOG_MISSION_ID, FLD_GOING_DATE, FLD_RETURN_DATE, FLD_MISSION_LOC,\n" +
                "       FLD_DOG_ID, FLD_GOING_TRANSPORT_TYPE_ID,\n" +
                "       FLD_DESC, FLD_MISS_CITY_ID, FLD_MISS_PROVINCE_ID, FLD_MISS_REQUEST_ORG_ID,\n" +
                "       FLD_GOING_DOG_STATUS_TYPE_ID, FLD_RETURN_TRANSPORT_TYPE_ID, FLD_RETURN_DOG_STATUS_TYPE_ID,\n" +
                "       FLD_DESC1, FLD_MOJAVEZ,FLD_PERSONEL_ID, FLD_HEADER from " +sourceSchema+".TBL_DOG_MISSION where FLD_GOING_DATE is not null" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_DOG_REQ(" +
                "    DOG_REQ_ID ,\n" +
                "    AGREE      ,\n" +
                "    CREATED_BY ,\n " +
                "    CREATION_TIME ,\n " +
                "    DELETED ,\n " +
                "    DESCRIPTION ,\n " +
                "    END_DATE ,\n " +
                "    LAST_UPDATE_TIME ,\n " +
                "    LAST_UPDATED_BY ,\n " +
                "    DOG_NUMBER ,\n " +
                "    ORGANIZATION_REQ ,\n " +
                "    PROCESS_STATUS ,\n " +
                "    REQUEST_DATE ,\n " +
                "    REQUEST_NUMBER ,\n " +
                "    START_DATE ,\n " +
                "    BEFORE_ACTION_MEETING_NOTE_ID ,\n " +
                "    CITY_ID ,\n " +
                "    COUNTRY_ID ,\n " +
                "    DOG_SPECIALIZATION_TYPE_ID ,\n " +
                "    PROVINCE_ID ,\n " +
                "    REGION_ID ,\n " +
                "    STATUS )\n" +
                "values (?,'YES','root',?,0,?,?,sysdate,'root',1,null,'SPECIFIED',?,?,?,null,?,1,null,?,null,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_DOG_MISSION_ID = resultSet.getString("FLD_DOG_MISSION_ID");
            Date FLD_GOING_DATE = resultSet.getDate("FLD_GOING_DATE");
            Date FLD_RETURN_DATE = resultSet.getDate("FLD_RETURN_DATE");
            String FLD_MISSION_LOC = resultSet.getString("FLD_MISSION_LOC");
            String FLD_DOG_ID = resultSet.getString("FLD_DOG_ID");
            String FLD_MISS_CITY_ID = resultSet.getString("FLD_MISS_CITY_ID");
            String FLD_MISS_PROVINCE_ID = resultSet.getString("FLD_MISS_PROVINCE_ID");
            String FLD_MOJAVEZ = resultSet.getString("FLD_MOJAVEZ");
            String FLD_PERSONEL_ID = resultSet.getString("FLD_PERSONEL_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            insertStmt.setLong(1, Long.parseLong(FLD_DOG_MISSION_ID));
            insertStmt.setDate(2, FLD_GOING_DATE);
            insertStmt.setString(3, FLD_DESC);
            insertStmt.setDate(4, FLD_RETURN_DATE);
            insertStmt.setDate(5, FLD_GOING_DATE);
            insertStmt.setString(6, FLD_MOJAVEZ);
            insertStmt.setDate(7, FLD_GOING_DATE);
            insertStmt.setString(8, FLD_MISS_CITY_ID);
            insertStmt.setString(9, FLD_MISS_PROVINCE_ID);

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
