package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadInventory {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadInventory(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_WAREHOUSE_ID,FLD_NAME,FLD_ADDRESS,FLD_CITY_ID,FLD_PROVINCE_ID from " +sourceSchema+".TBL_WAREHOUSE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_INVENTORY(INVENTORY_ID, ADDRESS, CODE,           \n" +
                "        CREATED_BY, CREATION_TIME, DELETED, DESCRIPTION, DIMENSION, LAST_UPDATE_TIME, LAST_UPDATED_BY,LATITUDE, LOCATION,LONGITUDE, \n" +
                "        MANAGER_NAME, SEALED_MECHANISM, TITLE,EMPLOYEE_ID, STATUS, CITY_ID,PROVINCE_ID)   \n" +
                "values (?,?,?,'root',SYSDATE,0,null,null,SYSDATE,'root',null,null,null,null,null,?,'253','ACTIVE',?,?)\n";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_ADDRESS = resultSet.getString("FLD_ADDRESS");
            String FLD_WAREHOUSE_ID = resultSet.getString("FLD_WAREHOUSE_ID");
            String FLD_TITEL = resultSet.getString("FLD_NAME");
            String FLD_CITY_ID = resultSet.getString("FLD_CITY_ID");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");

//            if(FLD_DESC == null)
//                FLD_DESC = "نامشخص";

            long newId = idGenerator.getNextId();
            long newId1 = newId-2590000;


            long newId2 = newId-2500000;


            insertStmt.setLong(1, Long.parseLong(FLD_WAREHOUSE_ID));

            insertStmt.setString(2, FLD_ADDRESS);

            insertStmt.setString(3, String.valueOf(newId1));

            insertStmt.setString(4, FLD_TITEL);
            insertStmt.setString(5, FLD_CITY_ID);
            insertStmt.setString(6, FLD_PROVINCE_ID);

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
