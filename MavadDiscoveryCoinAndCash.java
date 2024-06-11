package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadDiscoveryCoinAndCash {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadDiscoveryCoinAndCash(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_OTHER_FINDING_ID,FLD_OTHER_FINDING_TYPE_ID,\n" +
                "       FLD_CASE_ID,FLD_DATE,FLD_NO,FLD_DESC,FLD_PROVINCE_ID,FLD_PRICE_PROXIMATE from " +sourceSchema+".TBL_OTHER_FINDINGS" ;


        String insertQuery = "Insert into " + targetSchema +".NAP_DISCOVERY_CASH_AND_COIN(DISCOVERY_CASH_AND_COIN_ID," +
                "CREATED_BY,CREATION_TIME,DELETED,DESCRIPTION,ESTIMATE_PRICE,LAST_UPDATE_TIME," +
                "LAST_UPDATED_BY,NUMBER_OR_AMOUNT,SIM_NUMBER,TYPE,CURRENCY,INSPECTION_MEETING_NOTE_ID,MEASURE_UNIT_ID,STATUS)\n" +
                "values (?,'root',?,0,?,?,sysdate,'root',?,null,?,null,?,null,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_OTHER_FINDING_ID = resultSet.getString("FLD_OTHER_FINDING_ID");
            System.out.println(FLD_OTHER_FINDING_ID);
            int FLD_OTHER_FINDING_TYPE_ID = resultSet.getInt("FLD_OTHER_FINDING_TYPE_ID");
            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            Date FLD_DATE = resultSet.getDate("FLD_DATE");
            int FLD_NO = resultSet.getInt("FLD_NO");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
            String FLD_PRICE_PROXIMATE = resultSet.getString("FLD_PRICE_PROXIMATE");

            if(FLD_DATE == null){
                String str="2019-04-19";
                Date date = Date.valueOf(str);
                FLD_DATE=date;
            }

            String selectQuery1 = "select INSPECTION_MEETING_NOTE_ID from " +targetSchema+".NAP_INSPECTION_MEETING_NOTE WHERE  FOLDER_ID =" + FLD_CASE_ID ;
            PreparedStatement selectStmt4 = connectiontarget.prepareStatement(selectQuery1);
            ResultSet resultSet4 = selectStmt4.executeQuery();
            String INSPECTION_MEETING_NOTE_ID = null;
            while(resultSet4.next()){
                INSPECTION_MEETING_NOTE_ID = resultSet4.getString("INSPECTION_MEETING_NOTE_ID");
            }

            connectionFactory.closeResultSet(resultSet4);
            connectionFactory.closeStatement(selectStmt4);


            String type = null;

            if (FLD_OTHER_FINDING_TYPE_ID == 3 || FLD_OTHER_FINDING_TYPE_ID ==17723381) {
                type="CASH";
            }
            if (FLD_OTHER_FINDING_TYPE_ID == 12) {
                type="ETC";
            }
            if (FLD_OTHER_FINDING_TYPE_ID == 7 || FLD_OTHER_FINDING_TYPE_ID == 17723361) {
                type="JEWELLERY";
            }
            if (FLD_OTHER_FINDING_TYPE_ID == 1012748) {
                type="SIM_CARD";
            }

            insertStmt.setLong(1, Long.parseLong(FLD_OTHER_FINDING_ID));
            insertStmt.setDate(2, FLD_DATE);
            insertStmt.setString(3,FLD_DESC);
            insertStmt.setString(4,FLD_PRICE_PROXIMATE);
            insertStmt.setInt(5,FLD_NO);
            insertStmt.setString(6,type);
            insertStmt.setString(7,INSPECTION_MEETING_NOTE_ID);

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
