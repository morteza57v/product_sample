package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadProvince {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;
    private Connection connectionCamunda;

    public MavadProvince(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PROVINCE_ID,FLD_DESC from " +sourceSchema+".TBL_PROVINCE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_PROVINCE(PROVINCE_ID, CREATED_BY, CREATION_TIME, LAST_UPDATE_TIME, LAST_UPDATED_BY, PROVINCE_NAME, COUNTRY_ID)\n" +
                "values (?,'root',SYSDATE,SYSDATE, 'root', ?,1)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);


            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");

            if(FLD_DESC == null)
                FLD_DESC = "نامشخص";


            insertStmt.setLong(1, Long.parseLong(FLD_PROVINCE_ID));

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
