package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadTelephone {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadTelephone(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PERSON_TEL_ID,FLD_PERSON_POLICE_ID,FLD_TEL_TYPE_ID,\n" +
                "       FLD_TEL_NO,FLD_REG_DATE,FLD_DESC from " + sourceSchema +".TBL_PERSON_TEL";

        String insertQuery = "Insert into " + targetSchema + ".NAP_TELEPHONE( " +
              "TELEPHONE_ID,CREATED_BY,\n" +
                "       CREATION_TIME,DELETED,\n" +
                "       DESCRIPTION,LAST_UPDATE_TIME,\n" +
                "       LAST_UPDATED_BY,PHONE_NUMBER,TELEPHONE_TYPE,PERSON_ID,\n" +
                "       STATUS )\n" +
                "values (?,'root',sysdate,0,?," +
                "sysdate,'root',?,?,?," +
                "'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_PERSON_TEL_ID = resultSet.getString("FLD_PERSON_TEL_ID");
            String FLD_PERSON_POLICE_ID = resultSet.getString("FLD_PERSON_POLICE_ID");
            String FLD_TEL_TYPE_ID = resultSet.getString("FLD_TEL_TYPE_ID");
            String FLD_TEL_NO = resultSet.getString("FLD_TEL_NO");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            if (FLD_TEL_TYPE_ID != null){
                if (FLD_TEL_TYPE_ID.equalsIgnoreCase("1")){
                    FLD_TEL_TYPE_ID = "MOBILE";
                }
                if (FLD_TEL_TYPE_ID.equalsIgnoreCase("2")){
                    FLD_TEL_TYPE_ID = "PHONE";
                }
            }

            insertStmt.setLong(1, Long.parseLong(FLD_PERSON_TEL_ID));
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setString(3, FLD_TEL_NO);
            insertStmt.setString(4, FLD_TEL_TYPE_ID);
            insertStmt.setString(5, FLD_PERSON_POLICE_ID);

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
