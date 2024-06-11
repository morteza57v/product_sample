package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.cnv.RandomStringUUID;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;



public class MavadVehicleClass {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadVehicleClass(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select VEHICLE_BRAND_ID,TITLE from " +targetSchema+".NAP_VEHICLE_BRAND" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_VEHICLE_CLASS( " +
                "    VEHICLE_CLASS_ID ,\n" +
                "    TITLE          ,\n" +
                "    VEHICLE_BRAND_ID ,\n" +
                "    VEHICLE_TYPE_ID  )\n" +
                "values ( ?,?,?,'1')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connectiontarget.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String TITLE = resultSet.getString("TITLE");
            String VEHICLE_BRAND_ID = resultSet.getString("VEHICLE_BRAND_ID");


            long newId = idGenerator.getNextId();
            long newId1 = newId-2500000;

            insertStmt.setLong(1, newId1);
            insertStmt.setString(2, TITLE);
            insertStmt.setLong(3, Long.parseLong(VEHICLE_BRAND_ID));




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
