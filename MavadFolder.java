package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadFolder {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;

    public MavadFolder(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection, Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_CASE_ID,FLD_REG_PERSONEL_ID,FLD_CREATE_DATE" +
                ",FLD_DESC,FLD_CASE_TYPE_ID,FLD_ACCEPT_DATE2,FLD_REG_PERSONEL_ID3," +
                "FLD_REG_CITY_ID,FLD_PROVINCE_ID from " + sourceSchema + ".TBL_CASES where FLD_REG_CITY_ID is not null";


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema + ".NAP_FOLDER(" +
                "    FOLDER_ID             ,\n" +
                "    ACTION_TYPE           ,\n" +
                "    CREATED_BY            ,\n" +
                "    CREATION_TIME         ,\n" +
                "    DELETED               ,\n" +
                "    DESCRIPTION           ,\n" +
                "    FOLDER_NO             ,\n" +
                "    FOLDER_TYPE           ,\n" +
                "    LAST_UPDATE_TIME      ,\n" +
                "    LAST_UPDATED_BY       ,\n" +
                "    PROCESS_STATUS        ,\n" +
                "    STATUS                ,\n" +
                "    TITLE                 ,\n" +
                "    ACTION_REQ_ID         ,\n" +
                "    DISCOVERY_CITY_ID     ,\n" +
                "    DISCOVERY_PROVINCE_ID ,\n" +
                "    DISCOVERY_REGION_ID   ,\n" +
                "    PARENT_ID   )\n" +
                "values (?,'NORMAL',?,?,0,null,?,?,sysdate,?,'IN_FLOW','ACTIVE','CONVERT',null,?,?,null,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);

            String FLD_REG_PERSONEL_ID = resultSet.getString("FLD_REG_PERSONEL_ID");


            String selectQuery1 = "select USERNAME from " + sourceSchema + ".SEC_USER  where FLD_PERSONEL_ID=" + FLD_REG_PERSONEL_ID;

            PreparedStatement selectStmt1 = connection.prepareStatement(selectQuery1);
            ResultSet resultSet1 = selectStmt1.executeQuery();
            String USERNAME = null;
            while (resultSet1.next()) {
                USERNAME = resultSet1.getString("USERNAME");
            }

            connectionFactory.closeResultSet(resultSet1);
            connectionFactory.closeStatement(selectStmt1);


//            String FLD_REG_PERSONEL_ID3 = resultSet.getString("FLD_REG_PERSONEL_ID3");
//            String selectQuery2 = "select USERNAME from " +sourceSchema+".SEC_USER  where FLD_PERSONEL_ID="+FLD_REG_PERSONEL_ID3 ;
//
//            PreparedStatement selectStmt2 = connection.prepareStatement(selectQuery2);
//            ResultSet resultSet2 = selectStmt2.executeQuery();
//            String USERNAME2 = null;
//            while(resultSet2.next()){
//                USERNAME2 = resultSet2.getString("USERNAME");
//            }


            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");

            Date FLD_CREATE_DATE = resultSet.getDate("FLD_CREATE_DATE");
            String FLD_CASE_TYPE_ID = resultSet.getString("FLD_CASE_TYPE_ID");


            String FLD_REG_CITY_ID = resultSet.getString("FLD_REG_CITY_ID");
            System.out.println("FLD_REG_CITY_ID:"+FLD_REG_CITY_ID);
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");


            if (FLD_CASE_TYPE_ID == null) {
                FLD_CASE_TYPE_ID = "ACTION";
            } else if (FLD_CASE_TYPE_ID.equals("7")) {
                FLD_CASE_TYPE_ID = "INFORMATION";
            } else {
                FLD_CASE_TYPE_ID = "ACTION";
            }

            System.out.println("FLD_CASE_ID:" + FLD_CASE_ID);

            insertStmt.setLong(1, Long.parseLong(FLD_CASE_ID));
            insertStmt.setString(2, USERNAME);
            insertStmt.setDate(3, FLD_CREATE_DATE);
            insertStmt.setString(4, FLD_DESC);
            insertStmt.setString(5, FLD_CASE_TYPE_ID);

            insertStmt.setString(6, "ROOT");
            insertStmt.setLong(7, Long.parseLong(FLD_REG_CITY_ID));
            insertStmt.setLong(8, Long.parseLong(FLD_PROVINCE_ID));


            insertStmt.executeQuery();

            connectionFactory.closeStatement(insertStmt);
//            connectionFactory.closeResultSet(resultSet1);
//            connectionFactory.closeResultSet(resultSet2);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }


}
