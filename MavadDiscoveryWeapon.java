package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadDiscoveryWeapon {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadDiscoveryWeapon(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_GUN_FINDING_ID,FLD_DESC,FLD_CASE_ID,FLD_GUN_TYPE_ID from " +sourceSchema+".TBL_GUN_FINDINGS" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_DISCOVERY_WEAPON(" +
                "    DISCOVERY_WEAPON_ID     ,\n" +
                "    CREATED_BY              ,\n" +
                "    CREATION_TIME           ,\n" +
                "    DELETED                 ,\n" +
                "    DESCRIPTION             ,\n" +
                "    LAST_UPDATE_TIME        ,\n" +
                "    LAST_UPDATED_BY         ,\n" +
                "    SERIAL_NO               ,\n" +
                "    WEAPON_MEASURE_UNIT     ,\n" +
                "    INSPECTION_MEETING_NOTE_ID ,\n" +
                "    PRODUCT_ID                 ,\n" +
                "    STATUS        )\n" +
                "values (?,'root',sysdate,0,?,sysdate, 'root', '1','NUMBER',?,?,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_GUN_FINDING_ID = resultSet.getString("FLD_GUN_FINDING_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            String FLD_GUN_TYPE_ID = resultSet.getString("FLD_GUN_TYPE_ID");


            String selectQuery1 = "select INSPECTION_MEETING_NOTE_ID from " +targetSchema+".NAP_INSPECTION_MEETING_NOTE WHERE  FOLDER_ID =" + FLD_CASE_ID ;
            PreparedStatement selectStmt4 = connectiontarget.prepareStatement(selectQuery1);
            ResultSet resultSet4 = selectStmt4.executeQuery();
            String INSPECTION_MEETING_NOTE_ID = null;
            while(resultSet4.next()){
                INSPECTION_MEETING_NOTE_ID = resultSet4.getString("INSPECTION_MEETING_NOTE_ID");
            }

            connectionFactory.closeResultSet(resultSet4);
            connectionFactory.closeStatement(selectStmt4);

            insertStmt.setLong(1, Long.parseLong(FLD_GUN_FINDING_ID));
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setString(3, INSPECTION_MEETING_NOTE_ID);
            insertStmt.setString(4, FLD_GUN_TYPE_ID);

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
