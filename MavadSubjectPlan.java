package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.cnv.RandomStringUUID;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;



public class MavadSubjectPlan {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadSubjectPlan(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PLAN_TYPE_ID,FLD_DESC from " +sourceSchema+".TBL_PLAN_TYPE " ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_SUBJECT_PLAN(SUBJECT_PLAN_ID, TITLE)\n" +
                "values ( ?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_PLAN_TYPE_ID = resultSet.getString("FLD_PLAN_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");


            insertStmt.setLong(1, Long.parseLong(FLD_PLAN_TYPE_ID));
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
