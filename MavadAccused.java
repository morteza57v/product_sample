package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;




public class MavadAccused {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadAccused(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PERSON_POLICE_ID,FLD_ADDRESS_DESC,FLD_BIRTH_DATE,FLD_REG_DATE," +
                "FLD_DESC,FLD_FATHER_NAME,FLD_NAME,FLD_NATIONAL_CODE,FLD_ALIAS_NAME,FLD_CERTIF_NO," +
                "FLD_BIRTH_LOCATION2,FLD_CITY_ID,FLD_FAMILY,FLD_CITIZENSHIP_TYPE_ID,FLD_ADDRESS_CITY_ID," +
                "FLD_ADDRESS_PROVINCE_ID " +
                "from " +sourceSchema+".TBL_PERSON" ;


        String insertQuery = "Insert into " + targetSchema +".NAP_ACCUSED( " +
                "    ACCUSED_ID             ,\n" +
                "    ARREST_ADDRESS           ,\n" +
                "    ARREST_DATE              ,\n" +
                "    BANDIT                   ,\n" +
                "    BIRTH_DATE               ,\n" +
                "    CREATED_BY               ,\n" +
                "    CREATION_TIME            ,\n" +
                "    DELETED                  ,\n" +
                "    DESCRIPTION              ,\n" +
                "    FATHER_NAME              ,\n" +
                "    FIRST_NAME               ,\n" +
                "    FOREIGNER_CODE           ,\n" +
                "    FURTHER_INFORMATION      ,\n" +
                "    HOME_ADDRESS             ,\n" +
                "    ID_NO                    ,\n" +
                "    LAST_NAME                ,\n" +
                "    LAST_UPDATE_TIME         ,\n" +
                "    LAST_UPDATED_BY          ,\n" +
                "    NATIONAL_CODE            ,\n" +
                "    NICKNAME                 ,\n" +
                "    WORK_ADDRESS             ,\n" +
                "    ARREST_CITY              ,\n" +
                "    ARREST_PROVINCE          ,\n" +
                "    BIRTH_CITY_ID            ,\n" +
                "    CRIMINAL_ROLE_ID         ,\n" +
                "    INSPECTION_MEETING_NOTE_ID,\n" +
                "    ISSUED_CITY_ID         ,\n" +
                "    NATIONALITY           ,\n" +
                "    STATUS             ,\n" +
                "    LATITUDE         ,\n" +
                "    LONGITUDE,PICTURE)\n" +
                "values (?,?,?"+
                ",0,?,'root'," +
                "SYSDATE,0,?,?,?," +
                "null,null,null,?,?,SYSDATE," +
                "'root',?,?,null,?,?,null,?,?,?,?,'ACTIVE',null,null,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_PERSON_POLICE_ID = resultSet.getString("FLD_PERSON_POLICE_ID");
            String FLD_ADDRESS_DESC = resultSet.getString("FLD_ADDRESS_DESC");
            Date FLD_BIRTH_DATE = resultSet.getDate("FLD_BIRTH_DATE");
            Date FLD_REG_DATE = resultSet.getDate("FLD_REG_DATE");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_FATHER_NAME = resultSet.getString("FLD_FATHER_NAME");
            String FLD_NAME = resultSet.getString("FLD_NAME");
            String FLD_CERTIF_NO = resultSet.getString("FLD_CERTIF_NO");
            String FLD_FAMILY = resultSet.getString("FLD_FAMILY");
            String FLD_NATIONAL_CODE = resultSet.getString("FLD_NATIONAL_CODE");
            String FLD_ALIAS_NAME = resultSet.getString("FLD_ALIAS_NAME");
            String FLD_CITIZENSHIP_TYPE_ID = resultSet.getString("FLD_CITIZENSHIP_TYPE_ID");

//            String selectCases ="select FLD_CASE_ID from "+sourceSchema+".TBL_CASES where FLD_PERSON_POLICE_ID = "+FLD_PERSON_POLICE_ID;
//
//            PreparedStatement preparedStatement = connection.prepareStatement(selectCases);
//            ResultSet resultCases = preparedStatement.executeQuery();
//
//            String FLD_CASE_ID = null;
//
//            while(resultCases.next()){
//                FLD_CASE_ID =resultCases.getString("FLD_CASE_ID");
//            }


            String selectForCaseArrest = "select FLD_CITY_ID,FLD_CASE_ID,FLD_ARREST_DATE,FLD_ARRESTED_ROLE_ID," +
                    "FLD_ARREST_IMAGE,FLD_PROVINCE_ID  "+
                    "from " +sourceSchema+".TBL_CASE_ARREST where FLD_PERSON_POLICE_ID = "+ FLD_PERSON_POLICE_ID ;

            PreparedStatement selectStatement = connection.prepareStatement(selectForCaseArrest);
            ResultSet resultSet1 = selectStatement.executeQuery();

            String FLD_ARRESTED_ROLE_ID = null;
            String FLD_PROVINCE_ID= null;
            Blob FLD_ARREST_IMAGE = null;
            String  FLD_CITY_ID = null;
            String FLD_CASE_ID = null;

            while(resultSet1.next()){
                FLD_PROVINCE_ID = resultSet1.getString("FLD_PROVINCE_ID");
                FLD_REG_DATE = resultSet1.getDate("FLD_ARREST_DATE");
                FLD_ARRESTED_ROLE_ID = resultSet1.getString("FLD_ARRESTED_ROLE_ID");
                FLD_ARREST_IMAGE = resultSet1.getBlob("FLD_ARREST_IMAGE");
                FLD_CITY_ID  = resultSet1.getString("FLD_CITY_ID");
                FLD_CASE_ID  = resultSet1.getString("FLD_CASE_ID");
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

            insertStmt.setLong(1, Long.parseLong(FLD_PERSON_POLICE_ID));
            insertStmt.setString(2,FLD_ADDRESS_DESC);
            insertStmt.setDate(3, FLD_REG_DATE);
            insertStmt.setDate(4, FLD_BIRTH_DATE);
            insertStmt.setString(5, FLD_DESC);
            insertStmt.setString(6,FLD_FATHER_NAME);
            insertStmt.setString(7, FLD_NAME);
            insertStmt.setString(8, FLD_CERTIF_NO);
            insertStmt.setString(9, FLD_FAMILY);
            insertStmt.setString(10, FLD_NATIONAL_CODE);
            insertStmt.setString(11, FLD_ALIAS_NAME);
            insertStmt.setString(12, FLD_CITY_ID);
            insertStmt.setString(13, FLD_PROVINCE_ID);
            insertStmt.setString(14, FLD_ARRESTED_ROLE_ID);
            insertStmt.setString(15, INSPECTION_MEETING_NOTE_ID);
            insertStmt.setString(16, FLD_CITY_ID);
            insertStmt.setString(17, FLD_CITIZENSHIP_TYPE_ID);
            insertStmt.setBlob(18, FLD_ARREST_IMAGE);

            connectionFactory.closeResultSet(resultSet1);
            connectionFactory.closeStatement(selectStatement);

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
