package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadZonePartition {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadZonePartition(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CABINET_ID,FLD_NAME,FLD_WAREHOUSE_ID from " +sourceSchema+".TBL_CABINET" ;


        String insertQuery = "Insert into " + targetSchema +".NAP_ZONE(ZONE_ID,TITLE,INVENTORY_ID)\n" +
                "values (?,?,?)\n";

        String insertQuery1 = "Insert into " + targetSchema +".NAP_ZONE_PARTITION(ZONE_PARTITION_ID, TITLE,INVENTORY_ID,ZONE_ID)\n" +
                "values (?,?,?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();


        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);
            PreparedStatement insertStmt1 = connectiontarget.prepareStatement(insertQuery1);

            String FLD_CABINET_ID = resultSet.getString("FLD_CABINET_ID");
            String FLD_NAME = resultSet.getString("FLD_NAME");
            String FLD_WAREHOUSE_ID = resultSet.getString("FLD_WAREHOUSE_ID");

            insertStmt.setLong(1, Long.parseLong(FLD_CABINET_ID));

            insertStmt.setString(2, FLD_NAME);

            insertStmt.setLong(3, Long.parseLong(FLD_WAREHOUSE_ID));

            insertStmt1.setLong(1, idGenerator.getNextId());

            insertStmt1.setString(2, "قفسه 1");

            insertStmt1.setLong(3, Long.parseLong(FLD_WAREHOUSE_ID));
            insertStmt1.setLong(4, Long.parseLong(FLD_CABINET_ID));


            insertStmt.executeQuery();
            insertStmt1.executeQuery();

            connectionFactory.closeStatement(insertStmt);
            connectionFactory.closeStatement(insertStmt1);

        }


        while(resultSet.next()){

            System.out.println("Start1");

            PreparedStatement insertStmt1 = connectiontarget.prepareStatement(insertQuery1);

            String FLD_CABINET_ID = resultSet.getString("FLD_CABINET_ID");
            String FLD_NAME = resultSet.getString("FLD_NAME");
            String FLD_WAREHOUSE_ID = resultSet.getString("FLD_WAREHOUSE_ID");


            insertStmt1.setLong(1, Long.parseLong(FLD_CABINET_ID));

            insertStmt1.setString(2, FLD_NAME+" "+"سالن قدیم مواد");

            insertStmt1.setLong(3, Long.parseLong(FLD_WAREHOUSE_ID));
            insertStmt1.setLong(4, Long.parseLong(FLD_CABINET_ID));

            insertStmt1.executeQuery();

            connectionFactory.closeStatement(insertStmt1);

            System.out.println("Finish");

        }


        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }


}
