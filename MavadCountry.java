package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadCountry {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;

    public MavadCountry(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CITIZENSHIP_TYPE_ID,FLD_DESC from " +sourceSchema+".TBL_CITIZENSHIP_TYPE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_COUNTRY(COUNTRY_ID,\n" +
                "    COUNTRY_NAME,\n" +
                "    CREATED_BY,\n" +
                "    CREATION_TIME,\n" +
                "    LAST_UPDATE_TIME,\n" +
                "    LAST_UPDATED_BY ,\n" +
                "    PHONE_PREFIX)\n" +
                "values (?,?,'root',SYSDATE,SYSDATE,'root',null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);

            Long FLD_CITIZENSHIP_TYPE_ID = resultSet.getLong("FLD_CITIZENSHIP_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            insertStmt.setLong(1, FLD_CITIZENSHIP_TYPE_ID);
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
