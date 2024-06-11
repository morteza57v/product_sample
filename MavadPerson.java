package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadPerson {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadPerson(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
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


        String insertQuery = "Insert into "+targetSchema+".NAP_PERSON (PERSON_ID, BIRTH_DATE, CREATED_BY, CREATION_TIME, DELETED, DESCRIPTION," +
                " FATHER_NAME, FIRST_NAME, GENDER, HEIGHT, ID_NUMBER,\n" +
                "LAST_NAME, LAST_UPDATE_TIME, LAST_UPDATED_BY, LEGAL_NAME, LEGAL_PERSON_TYPE," +
                " NATIONAL_CODE, PERSON_TYPE, REGISTER_DATE,\n" +
                "REGISTER_NO, WEIGHT, BIRTH_CITY_ID, EYE_COLOR_ID, REGISTER_CITY_ID, SKIN_COLOR_ID, STATUS, CITY_ID)\n" +
                "values (?,?, 'root',\n" +
                "sysdate,0,?,?,?,null,null,null,?,\n" +
                "sysdate, 'root', null, null,?,'REAL',?, null,null,?, null,?, null,\n" +
                "'ACTIVE',?)";


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


            String selectForCaseArrest = "select FLD_CITY_ID,FLD_CASE_ID,FLD_ARREST_DATE,FLD_ARRESTED_ROLE_ID," +
                    "FLD_ARREST_IMAGE,FLD_PROVINCE_ID  "+
                    "from " +sourceSchema+".TBL_CASE_ARREST where FLD_PERSON_POLICE_ID = "+ FLD_PERSON_POLICE_ID ;

            PreparedStatement selectStatement = connection.prepareStatement(selectForCaseArrest);
            ResultSet resultSet1 = selectStatement.executeQuery();

            String FLD_ARRESTED_ROLE_ID = null;
            String FLD_PROVINCE_ID= null;
            Blob FLD_ARREST_IMAGE = null;
            String  FLD_CITY_ID = null;

            while(resultSet1.next()){
                FLD_PROVINCE_ID = resultSet1.getString("FLD_PROVINCE_ID");
                FLD_REG_DATE = resultSet1.getDate("FLD_ARREST_DATE");
                FLD_ARRESTED_ROLE_ID = resultSet1.getString("FLD_ARRESTED_ROLE_ID");
                FLD_ARREST_IMAGE = resultSet1.getBlob("FLD_ARREST_IMAGE");
                FLD_CITY_ID  = resultSet1.getString("FLD_CITY_ID");
            }


            insertStmt.setLong(1, Long.parseLong(FLD_PERSON_POLICE_ID));
            insertStmt.setDate(2, FLD_BIRTH_DATE);
            insertStmt.setString(3, FLD_DESC);
            insertStmt.setString(4,FLD_FATHER_NAME);
            insertStmt.setString(5, FLD_NAME);
            insertStmt.setString(6, FLD_FAMILY);
            insertStmt.setString(7, FLD_NATIONAL_CODE);
            insertStmt.setDate(8, FLD_REG_DATE);
            insertStmt.setString(9, FLD_CITY_ID);
            insertStmt.setString(10, FLD_CITY_ID);
            insertStmt.setString(11, FLD_CITY_ID);

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
