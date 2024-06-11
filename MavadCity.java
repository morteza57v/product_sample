package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MavadCity {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadCity(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CITY_ID,FLD_PROVINCE_ID,FLD_DESC from " +sourceSchema+".TBL_CITY" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_CITY(CITY_ID, CITY_NAME, CREATED_BY, CREATION_TIME, LAST_UPDATE_TIME, LAST_UPDATED_BY, PHONE_PREFIX,PROVINCE_ID)\n" +
                "values (?, ?,'root',sysdate,sysdate,'root',null,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            Long FLD_CITY_ID = resultSet.getLong("FLD_CITY_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");


            long newId = idGenerator.getNextId();
            long newId1 = newId-2500000;


            insertStmt.setLong(1, FLD_CITY_ID);

            System.out.println("FLD_CITY_ID:"+FLD_CITY_ID);

            insertStmt.setString(2, FLD_DESC);

            insertStmt.setLong(3, Long.parseLong(FLD_PROVINCE_ID));


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
