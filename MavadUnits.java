package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadUnits {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadUnits(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_ORGANIZATION_ID,FLD_ORGANIZATION_NAME,TBL_FLD_ORGANIZATION_ID,\n" +
                "       FLD_DESC,FLD_CITY_ID,FLD_PROVINCE_ID,\n" +
                "       FLD_PATH,FLD_ORGANIZATION_CODE,FLD_ORGANIZATION_TYPE_ID from " +sourceSchema+".TBL_ORGANIZATION " +
//                "where FLD_ORGANIZATION_ID = 928042" ;
                "where FLD_ORGANIZATION_ID = 14 order by TBL_FLD_ORGANIZATION_ID asc" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_UNITS(" +
                "    UNITS_ID        ,\n" +
                "    CREATED_BY      ,\n" +
                "    CREATION_TIME   ,\n" +
                "    DELETED         ,\n" +
                "    DESCRIPTION     ,\n" +
                "    LAST_UPDATE_TIME,\n" +
                "    LAST_UPDATED_BY ,\n" +
                "    TITLE           ,\n" +
                "    STATUS     ,\n" +
                "    CITY_ID     ,\n" +
                "    PROVINCE_ID     ,\n" +
                "    UNITS_TYPE_ID     ,\n" +
                "    PARET_UNITS_ID     )\n" +
                "values ( ?,'root',sysdate,0,null,sysdate,'root',?,'ACTIVE',?,?,?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_ORGANIZATION_ID = resultSet.getString("FLD_ORGANIZATION_ID");
            String FLD_ORGANIZATION_NAME = resultSet.getString("FLD_ORGANIZATION_NAME");
            String FLD_CITY_ID = resultSet.getString("FLD_CITY_ID");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
            String FLD_ORGANIZATION_TYPE_ID = resultSet.getString("FLD_ORGANIZATION_TYPE_ID");
            String TBL_FLD_ORGANIZATION_ID = resultSet.getString("TBL_FLD_ORGANIZATION_ID");

            insertStmt.setLong(1, Long.parseLong(FLD_ORGANIZATION_ID));
            insertStmt.setString(2,FLD_ORGANIZATION_NAME);
            insertStmt.setString(3,FLD_CITY_ID);
            insertStmt.setString(4,FLD_PROVINCE_ID);
            insertStmt.setString(5,FLD_ORGANIZATION_TYPE_ID);
            insertStmt.setString(6,TBL_FLD_ORGANIZATION_ID);

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
