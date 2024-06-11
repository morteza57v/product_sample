package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

public class MavadUser {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;


    public MavadUser(String targetSchema, String sourceSchema, IdGenerator idGenerator,String Camunda, Connection connection , Connection connectiontarget,Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FIRST_NAME,LAST_NAME,USERNAME,ENABLED from " +sourceSchema+".SEC_USER" ;
        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery =
                "Insert into " + targetSchema + ".PORTAL_USER " +
                        "(USER_ID,ACTIVATION_HOUR_END,ACTIVATION_HOUR_START," +
                        "ACTIVATION_TIME_FROM,ACTIVATION_TIME_TO,BROWSER_LOGIN_AUTHORITY," +
                        "CREATED_BY,CREATION_TIME,ENABLED,ENABLING_REASON," +
                        "FINGERPRINT_CONTENT_TYPE,FINGERPRINT_FILE_NAME,FIRST_NAME," +
                        "FORCE_PASSWORD_CHANGE,INCORRECT_LOGIN_DISABLED_TIME,INCORRECT_LOGINS_COUNT," +
                        "IS_SUPERVISOR,LAST_NAME,LAST_PASSWORD_CHANGE_TIME," +
                        "LAST_UPDATE_TIME,LAST_UPDATED_BY,MOBILE_NUMBER," +
                        "NATIONAL_CODE,IS_ONLINE,PASSWORD," +
                        "PERSONNEL_CODE,PERSONNEL_ID,PICTURE_CONTENT_TYPE," +
                        "PICTURE_FILE_NAME,SERVICE_LOGIN_AUTHORITY,SIGNATURE_CONTENT_TYPE," +
                        "SIGNATURE_FILE_NAME,USERNAME) " +
                        "values (?,null,null,null," +
                        "null,1,'root'," +
                        "SYSDATE,1" +
                        ",null,null,null," +
                        "?,0,null,null," +
                        "0,?,SYSDATE," +
                        "SYSDATE," +
                        "'root',null,null,0,'9eb3b98423a0d474e936270a734eb83d4565de81',null,null,null,null,0,null,null,?)";


//        String insertCamunda = "Insert into " + Camunda +".ACT_ID_USER" +
//                " (ID_,REV_,FIRST_,LAST_,EMAIL_,PWD_,SALT_,LOCK_EXP_TIME_,ATTEMPTS_,PICTURE_ID_) " +
//                "values (?,1,?,?,null,'{SHA-512}wlPvkXOeileQBcx02hwsDgOvk4gdAKy74KrkdX0GMFsI+UN6mvly8BSU6gER4nwPrQc8zrGH3Ik3QfPlys7paQ==','M1pqmBHd3VfP0jnj4AoyxQ==',null,null,null)";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            String loginName = resultSet.getString("USERNAME");
//            if(loginName.contains(".")){
//                String[] split = loginName.split("[.]");
//                if(split.length > 2){
//                    loginName = split[0]+split[1]+split[2];
//                }else{
//                    loginName = split[0]+split[1];
//                }
//            }
//            long newId = idGenerator.getNextId();
            randomStringUUID = new RandomStringUUID();
            String uuid = randomStringUUID.getUUID();
            map.put(loginName, uuid);

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);
//            PreparedStatement insertStmt1 = connectionCamunda.prepareStatement(insertCamunda);
            insertStmt.setString(1, uuid);
            String firstName = resultSet.getString("FIRST_NAME");
            String lastName = resultSet.getString("LAST_NAME");
            if(firstName == null)
                firstName = "نامشخص";
            if(loginName.equalsIgnoreCase("root"))
                loginName = "root1";
//            insertStmt.setString(13, userFullName);
            insertStmt.setString(1, uuid);
            insertStmt.setString(2, firstName);
            insertStmt.setString(3, lastName);
            insertStmt.setString(4, loginName);

            System.out.println("LoginName:"+loginName);

//            insertStmt1.setString(1, loginName);
//            insertStmt1.setString(2, firstName);
//            insertStmt1.setString(3, lastName);
//            insertStmt1.executeQuery();
            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);
//            connectionFactory.closeStatement(insertStmt1);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }
}
