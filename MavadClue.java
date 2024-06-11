package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import javax.xml.crypto.Data;
import java.sql.*;
import java.util.HashMap;


public class MavadClue {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadClue(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_NEWS_ID ,FLD_NEWS_TEXT,FLD_FINDING_DATE,FLD_REG_PERSONEL_ID,FLD_UPDATE,FLD_PERSON_POLICE_ID,FLD_NEWS_STATUS_TYPE_ID, from " +sourceSchema+".TBL_NEWS" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_CLUE (" +
                "        CLUE_ID   ,\n" +
                "        CLUE_NUMBER   ,        \n" +
                "        CONTENT    ,         \n" +
                "        CREATED_BY   ,       \n" +
                "        CREATION_TIME,        \n" +
                "        DELETED    ,         \n" +
                "        HOW_STATUS    ,     \n" +
                "        LAST_UPDATE_TIME  ,    \n" +
                "        LAST_UPDATED_BY  ,     \n" +
                "        OCCURRENCE_DATE   ,    \n" +
                "        PROCESS_DEFINITION_ID,\n" +
                "        PROCESS_INSTANCE_ID  ,\n" +
                "        PROCESS_STATUS  ,     \n" +
                "        STATUS        ,      \n" +
                "        VALIDITY      ,       \n" +
                "        WHAT_STATUS   ,    \n" +
                "        WHEN_STATUS   ,       \n" +
                "        WHERE_STATUS  ,      \n" +
                "        WHO_STATUS    ,      \n" +
                "        WHY_STATUS    ,       \n" +
                "        SOURCE_ID     ,       \n" +
                "        CLUE_SRC_ITEM_ID  )\n" +
                "values (?,?,?,'root',to_timestamp('08-JAN-2023 15.44.42.739000000 PM','DD-MON-RR HH.MI.SSXFF AM'), 0, ?,?," +
                " ?, ?,?, null , 'FOLDERED', 'ACTIVE', 1, null,null,null,null,null,1,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){




            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_NEWS_ID = resultSet.getString("FLD_NEWS_ID");
            String FLD_NEWS_TEXT = resultSet.getString("FLD_NEWS_TEXT");
            String FLD_NEWS_STATUS_TYPE_ID = resultSet.getString("FLD_NEWS_STATUS_TYPE_ID");
            Date FLD_UPDATE = resultSet.getDate("FLD_UPDATE");
            Date FLD_PERSON_POLICE_ID = resultSet.getDate("FLD_PERSON_POLICE_ID");


            insertStmt.setLong(1, Long.parseLong(FLD_NEWS_ID));
            insertStmt.setLong(2, Long.parseLong(FLD_NEWS_ID));
            insertStmt.setString(3, FLD_NEWS_TEXT);
            insertStmt.setLong(4, Long.parseLong(FLD_NEWS_STATUS_TYPE_ID));
            insertStmt.setDate(5, FLD_UPDATE);
            insertStmt.setDate(6, FLD_PERSON_POLICE_ID);
            insertStmt.setLong(7, Long.parseLong(FLD_NEWS_STATUS_TYPE_ID));


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
