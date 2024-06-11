package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadMeasure {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadMeasure(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_MEASURMENT_UNIT_TYPE_ID,FLD_DESC from " +sourceSchema+".TBL_MEASURMENT_UNIT_TYPE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_MEASURE_UNIT(MEASURE_UNIT_ID,TITLE,PRODUCT_GROUP_ID)\n" +
                "values (?,?,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_MEASURMENT_UNIT_TYPE_ID = resultSet.getString("FLD_MEASURMENT_UNIT_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");


            insertStmt.setLong(1, Long.parseLong(FLD_MEASURMENT_UNIT_TYPE_ID));

            insertStmt.setString(2, FLD_DESC);


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
