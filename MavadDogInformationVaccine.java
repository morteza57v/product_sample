package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadDogInformationVaccine {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadDogInformationVaccine(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_ANTISERUM_TYPE_ID,FLD_DOG_ID from " +sourceSchema+".TBL_DOG_ANTISERUM_INFO" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_DOG_INFORMATION_VACCINE(VACCINE_ID, DOG_INFORMATION_ID)\n" +
                "values (?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_ANTISERUM_TYPE_ID = resultSet.getString("FLD_ANTISERUM_TYPE_ID");
            String FLD_DOG_ID = resultSet.getString("FLD_DOG_ID");

            System.out.println("FLD_ANTISERUM_TYPE_ID:"+FLD_ANTISERUM_TYPE_ID);
            System.out.println("FLD_DOG_ID:"+FLD_DOG_ID);


            insertStmt.setLong(1, Long.parseLong(FLD_ANTISERUM_TYPE_ID));
            insertStmt.setLong(2, Long.parseLong(FLD_DOG_ID));


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
