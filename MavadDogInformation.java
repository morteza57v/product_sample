package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadDogInformation {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadDogInformation(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select " +
                "FLD_DOG_ID," +
                "FLD_BIRTH_DATE," +
                "FLD_DESC," +
                "FLD_SELLER_DATE," +
                "FLD_SEX_ID," +
                "FLD_DOG_STATUS_TYPE_ID," +
                "FLD_SPECIAL_CODE," +
                "FLD_MICRO_CODE," +
                "FLD_NAME," +
                "FLD_DOG_RACE_TYPE_ID," +
                "FLD_SELLER_COUNTRY_ID from " + sourceSchema + ".TBL_DOG";


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema + ".NAP_DOG_INFORMATION(" +
                "    DOG_INFORMATION_ID,\n" +
                "    BIRTH_DATE,\n" +
                "    CAGE_NUMBER                  ,\n" +
                "    CAUSE_DESCRIPTION            ,\n" +
                "    COMMENTS                     ,\n" +
                "    CREATED_BY                   ,\n" +
                "    CREATION_TIME                ,\n" +
                "    DELETED  ," +
                "    DESCRIPTION                  ,\n" +
                "    DOG_TYPE                     ,\n" +
                "    ENTRY_DATE                   ,\n" +
                "    GENDER                       ,\n" +
                "    HEALTH_CONDITION             ,\n" +
                "    IDENTITY_CARD  ," +
                "    LAST_UPDATE_TIME             ,\n" +
                "    LAST_UPDATED_BY              ,\n" +
                "    LOSS_DATE                    ,\n" +
                "    LOSS_PLACE  ," +
                "    MICROCHIP_CODE ," +
                "    MICROCHIP_INJECTION_DATE     ,\n" +
                "    NAME                         ,\n" +
                "    ORGANIZATION_ID              ,\n" +
                "    VET_IDEA ," +
                "    BREED_ID    ," +
                "    FATHER_INFORMATION_ID        ,\n" +
                "    LOSS_CAUSE_ID                ,\n" +
                "    LOSS_CONFIRMATION_EMPLOYEE_ID,\n" +
                "    MOTHER_INFORMATION_ID        ,\n" +
                "    SOURCE_COUNTRY_ID,\n" +
                "    STATUS     ,\n" +
                "    VET_EMPLOYEE_ID )\n" +
                "values (?,?,null,null,null,'root',sysdate" +
                ",0,?,?,?,?,?,?,sysdate" +
                ",'root',null" +
                ",null,?,null,?,null ,null,?,null,null,null,null,?,?,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_DOG_ID = resultSet.getString("FLD_DOG_ID");
            System.out.println(FLD_DOG_ID);
            Date FLD_BIRTH_DATE = resultSet.getDate("FLD_BIRTH_DATE");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_SELLER_COUNTRY_ID = resultSet.getString("FLD_SELLER_COUNTRY_ID");
            String FLD_SELLER_COUNTRY_ID2 = resultSet.getString("FLD_SELLER_COUNTRY_ID");
            Date FLD_SELLER_DATE = resultSet.getDate("FLD_SELLER_DATE");
            String FLD_SEX_ID = resultSet.getString("FLD_SEX_ID");
            String FLD_DOG_STATUS_TYPE_ID = resultSet.getString("FLD_DOG_STATUS_TYPE_ID");
            String FLD_SPECIAL_CODE = resultSet.getString("FLD_SPECIAL_CODE");
            String FLD_MICRO_CODE = resultSet.getString("FLD_MICRO_CODE");
            String FLD_NAME = resultSet.getString("FLD_NAME");
            String FLD_DOG_RACE_TYPE_ID = resultSet.getString("FLD_DOG_RACE_TYPE_ID");


            String ACTIVE = "ACTIVE";


            if (FLD_SEX_ID.equals("5")) {
                FLD_SEX_ID = "MALE";
            } else if (FLD_SEX_ID.equals("6")) {
                FLD_SEX_ID = "FEMALE";
            }

            if (FLD_SELLER_COUNTRY_ID.equals("1")) {
                FLD_SELLER_COUNTRY_ID = "NON_IMPORTED";
            } else {
                FLD_SELLER_COUNTRY_ID = "IMPORTED";
            }


            if (FLD_DOG_STATUS_TYPE_ID.equals("1")) {
                FLD_DOG_STATUS_TYPE_ID = "HEALTHY";
            } else if (FLD_DOG_STATUS_TYPE_ID.equals("4") || FLD_DOG_STATUS_TYPE_ID.equals("2")) {
                FLD_DOG_STATUS_TYPE_ID = "DEAD";
                ACTIVE = "INACTIVE";
            } else if (FLD_DOG_STATUS_TYPE_ID.equals("3")) {
                FLD_DOG_STATUS_TYPE_ID = "UNHEALTHY";
            }

            insertStmt.setLong(1, Long.parseLong(FLD_DOG_ID));
            insertStmt.setDate(2, FLD_BIRTH_DATE);
            insertStmt.setString(3, FLD_DESC);
            insertStmt.setString(4, FLD_SELLER_COUNTRY_ID);
            insertStmt.setDate(5, FLD_SELLER_DATE);
            insertStmt.setString(6, FLD_SEX_ID);
            insertStmt.setString(7, FLD_DOG_STATUS_TYPE_ID);
            insertStmt.setString(8, FLD_SPECIAL_CODE);
            insertStmt.setString(9, FLD_MICRO_CODE);
            insertStmt.setString(10, FLD_NAME);
            insertStmt.setString(11, FLD_DOG_RACE_TYPE_ID);
            insertStmt.setString(12, FLD_SELLER_COUNTRY_ID2);
            insertStmt.setString(13, ACTIVE);

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
