package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadUnitsType {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadUnitsType(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_ORGANIZATION_TYPE_ID,FLD_DESC from " +sourceSchema+".TBL_ORGANIZATION_TYPE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_UNITS_TYPE(" +
                "    UNITS_TYPE_ID        ,\n" +
                "    CREATED_BY      ,\n" +
                "    CREATION_TIME   ,\n" +
                "    DELETED         ,\n" +
                "    DESCRIPTION     ,\n" +
                "    LAST_UPDATE_TIME,\n" +
                "    LAST_UPDATED_BY ,\n" +
                "    TITLE           ,\n" +
                "    STATUS     )\n" +
                "values ( ?,'root',sysdate,0,null,sysdate,'root',?,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_ORGANIZATION_TYPE_ID = resultSet.getString("FLD_ORGANIZATION_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            insertStmt.setLong(1, Long.parseLong(FLD_ORGANIZATION_TYPE_ID));
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
