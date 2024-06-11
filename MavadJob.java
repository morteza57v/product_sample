package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

public class MavadJob {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadJob(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PERSONEL_JOB_TITLE_ID,FLD_DESC from " +sourceSchema+".TBL_PERSONEL_JOB_TITLE" ;
        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery =
                "Insert into " + targetSchema + ".NAP_JOB " +
                        "(JOB_ID,TITLE) values (?,?)";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);
            String title = resultSet.getString("FLD_DESC");
            String id_job = resultSet.getString("FLD_PERSONEL_JOB_TITLE_ID");

            if(title == null)
                title = "نامشخص";
            insertStmt.setString(1, id_job);
            insertStmt.setString(2, title);


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
