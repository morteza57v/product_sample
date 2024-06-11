package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadAccused2 {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadAccused2(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select fld_operation_smuggler_id, fld_operation_id, fld_person_id,\n" +
                "       fld_personel_op_effect_type_id, fld_desc \n"+
                "from " +sourceSchema+".TBL_OPERATION_SMUGGLER" ;


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
                "values (?,null,null"+
                ",0,null,'root'," +
                "SYSDATE,0,?,?,?," +
                "null,null,null,null,?,SYSDATE," +
                "'root',?,null,null,null,null,null,null,?,null,null,'ACTIVE',null,null,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String fld_operation_smuggler_id = resultSet.getString("fld_operation_smuggler_id");
            String fld_operation_id = resultSet.getString("fld_operation_id");
            String fld_person_id = resultSet.getString("fld_person_id");
            String fld_desc = resultSet.getString("fld_desc");

            String selectPerson = "select FLD_PERSON_POLICE_ID,FLD_ADDRESS_DESC,FLD_BIRTH_DATE,FLD_REG_DATE," +
                    "FLD_DESC,FLD_FATHER_NAME,FLD_NAME,FLD_NATIONAL_CODE,FLD_ALIAS_NAME,FLD_CERTIF_NO," +
                    "FLD_BIRTH_LOCATION2,FLD_CITY_ID,FLD_FAMILY,FLD_CITIZENSHIP_TYPE_ID,FLD_ADDRESS_CITY_ID," +
                    "FLD_ADDRESS_PROVINCE_ID " +
                    "from " +sourceSchema+".TBL_PERSON where FLD_PERSON_POLICE_ID = "+fld_person_id ;

            PreparedStatement selectStmt1 = connection.prepareStatement(selectPerson);
            ResultSet resultSet1 = selectStmt1.executeQuery();

            String FLD_FATHER_NAME =null;
            String FLD_NAME = null;
            String FLD_FAMILY = null;
            String FLD_NATIONAL_CODE = null;

            while(resultSet1.next()){
               FLD_FATHER_NAME = resultSet1.getString("FLD_FATHER_NAME");
               FLD_NAME = resultSet1.getString("FLD_NAME");
               FLD_FAMILY = resultSet1.getString("FLD_FAMILY");
               FLD_NATIONAL_CODE = resultSet1.getString("FLD_NATIONAL_CODE");
            }

            connectionFactory.closeResultSet(resultSet1);
            connectionFactory.closeStatement(selectStmt1);

            insertStmt.setLong(1, Long.parseLong(fld_operation_smuggler_id));
            insertStmt.setString(2,fld_desc);
            insertStmt.setString(3, FLD_FATHER_NAME);
            insertStmt.setString(4, FLD_NAME);
            insertStmt.setString(5, FLD_FAMILY);
            insertStmt.setString(6, FLD_NATIONAL_CODE);
            insertStmt.setString(7,fld_operation_id);

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
