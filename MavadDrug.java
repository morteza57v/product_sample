package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadDrug {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadDrug(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_NARCOTIC_TYPE_ID,FLD_DESC from " +sourceSchema+".TBL_NARCOTIC_TYPE" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_DRUGS_TYPE(DRUGS_TYPE_ID, TITLE)\n" +
                "values ( ?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_NARCOTIC_TYPE_ID = resultSet.getString("FLD_NARCOTIC_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");

//            if(FLD_DESC == null)
//                FLD_DESC = "نامشخص";
//
//            long newId = idGenerator.getNextId();
//            long newId1 = newId-2500000;
//






//            List<String> aa= Arrays.asList("3873827", "32", "28", "33", "40", "22", "41", "2", "31", "37", "45");
//            List<String> bb= Arrays.asList(  "100025", "100020", "100021", "100022", "100023", "100024", "100026", "100027", "100028", "100029", "100030");
//
//            for (int i = 0; i <aa.size() ; i++) {
//                if (FLD_PROVINCE_ID.equals(aa.get(i))){
//                    FLD_PROVINCE_ID=bb.get(i);
//                };
//            }



            insertStmt.setLong(1, Long.parseLong(FLD_NARCOTIC_TYPE_ID));

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
