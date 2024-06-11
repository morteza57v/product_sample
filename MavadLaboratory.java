package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadLaboratory {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private Connection connection;
    private Connection connectionTarget;

    public MavadLaboratory(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_ADDICT_TEST_LABORATORY_ID,FLD_NAME,FLD_ADDRESS,FLD_DESC," +
                "FLD_PROVINCE_ID,FLD_CITY_ID from " +sourceSchema+".TBL_ADDICT_TEST_LABORATORY " +
                "where FLD_CITY_ID is not null and FLD_ADDRESS is not null" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_LABORATORY( LABORATORY_ID,\n" +
                "    ADDRESS,\n" +
                "    CODE,\n" +
                "    CREATED_BY,\n" +
                "    CREATION_TIME,\n" +
                "    DELETED,\n" +
                "    DESCRIPTION,\n" +
                "    LAST_UPDATE_TIME,\n" +
                "    LAST_UPDATED_BY,\n" +
                "    LATITUDE,\n" +
                "    LOCATION,\n" +
                "    LONGITUDE,\n" +
                "    TITLE,\n" +
                "    MANAGER_NAME_EMPLOYEE_ID,\n" +
                "    STATUS,CITY_ID,PROVINCE_ID)\n" +
                "values (?,?,null,'root',sysdate,0,?," +
                "sysdate,'root',null,null,null,?,NULL,'ACTIVE',?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);

            String FLD_ADDICT_TEST_LABORATORY_ID = resultSet.getString("FLD_ADDICT_TEST_LABORATORY_ID");
            String FLD_NAME = resultSet.getString("FLD_NAME");
            String FLD_ADDRESS = resultSet.getString("FLD_ADDRESS");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_CITY_ID = resultSet.getString("FLD_CITY_ID");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");


            insertStmt.setLong(1, Long.parseLong(FLD_ADDICT_TEST_LABORATORY_ID));
            insertStmt.setString(2, FLD_ADDRESS);
            insertStmt.setString(3, FLD_DESC);
            insertStmt.setString(4, FLD_NAME);
            insertStmt.setLong(5, Long.parseLong(FLD_CITY_ID));
            insertStmt.setLong(6, Long.parseLong(FLD_PROVINCE_ID));

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
