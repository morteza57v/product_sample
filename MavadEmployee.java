package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

public class MavadEmployee {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadEmployee(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, Connection connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PERSONEL_ID,FLD_PERSONEL_JOB_TITLE_ID ,FLD_PERSONEL_NAME,FLD_PERSONEL_FAMILY,FLD_PERSONEL_NO from " +sourceSchema+".TBL_PERSONEL" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_EMPLOYEE (EMPLOYEE_ID, CREATED_BY, CREATION_TIME, DELETED, DESCRIPTION, FIRST_NAME, LAST_NAME,\n" +
                "                                LAST_UPDATE_TIME, LAST_UPDATED_BY, NATIONAL_CODE, PERSONNEL_CODE, USER_ID, USERNAME,\n" +
                "                                VETERINARY_NUMBER, POLICE_GRADE, STATUS)\n" +
                "values (?, 'root',SYSDATE, 0, null,?,?,sysdate, 'root', '123',?, null, 'root', null, null, 'ACTIVE')\n";



        String insertQueryjonshentable = "Insert into " + targetSchema + ".NAP_EMPLOYEE_JOB " +
                "(EMPLOYEE_ID,JOB_ID)" +
                " values (?,?)";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){


            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            PreparedStatement insertStmthonshen = connectiontarget.prepareStatement(insertQueryjonshentable);
            String FLD_PERSONEL_ID = resultSet.getString("FLD_PERSONEL_ID");
            String PERSONEL_JOB_TITLE_ID = resultSet.getString("FLD_PERSONEL_JOB_TITLE_ID");
            String PERSONEL_NAME = resultSet.getString("FLD_PERSONEL_NAME");
            String PERSONEL_FAMILY = resultSet.getString("FLD_PERSONEL_FAMILY");
            String PERSONEL_NO = resultSet.getString("FLD_PERSONEL_NO");

            if(PERSONEL_NAME == null)
                PERSONEL_NAME = "نامشخص";

            if(PERSONEL_NO == null)
                PERSONEL_NO = null;

            if(PERSONEL_FAMILY == null)
                PERSONEL_FAMILY = "نامشخص";


            insertStmt.setLong(1, Long.parseLong(FLD_PERSONEL_ID));
            insertStmt.setString(2, PERSONEL_NAME);
            insertStmt.setString(3, PERSONEL_FAMILY);
            insertStmt.setString(4, PERSONEL_NO);

            if(PERSONEL_JOB_TITLE_ID == null){
                PERSONEL_JOB_TITLE_ID = "23";
            }

            insertStmthonshen.setLong(1, Long.parseLong(FLD_PERSONEL_ID));
            insertStmthonshen.setString(2, PERSONEL_JOB_TITLE_ID);


            insertStmt.executeQuery();
            insertStmthonshen.executeQuery();
            connectionFactory.closeStatement(insertStmt);
            connectionFactory.closeStatement(  insertStmthonshen);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
