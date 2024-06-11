package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadAppointment {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadAppointment(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_DOG_CHECK_ID,FLD_DATE,FLD_DOCTOR_ID,FLD_CHECK_DESC,FLD_DOG_ID from " +sourceSchema+".TBL_DOG_CHECK_INFO" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_APPOINTMENT" +
                "(APPOINTMENT_ID,APPOINTMENT_DATE,CITY_SCAN,DELETED,DESCRIPTION" +
                ",DIET_PROCESS_STATUS,DOG_CONDITION_DESCRIPTION,DRUG_PROCESS_STATUS," +
                "DRUG_STATUS,HEALTH_CONDITION,M_R_I,PRESCRIPTION_ISSUE_TYPE,RADIOLOGY," +
                "TEST_ORDER,THERAPEUTIC_ACTION_DESCRIPTION,ULTRASOUND,VISIT_DURATION," +
                "VISIT_NUMBER,VISIT_RESULT,DOG_DELIVERY,VET_EMPLOYEE_ID,VISIT_LOCATION_ID)\n" +
                "values (?,?,0,0,?,null,null,null,null,null,0,null,null,0,?,0,15,?,null,null,?,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_DOG_CHECK_ID = resultSet.getString("FLD_DOG_CHECK_ID");
            System.out.println(FLD_DOG_CHECK_ID);
            Date FLD_DATE = resultSet.getDate("FLD_DATE");
            String FLD_DOCTOR_ID = resultSet.getString("FLD_DOCTOR_ID");
            String FLD_CHECK_DESC = resultSet.getString("FLD_CHECK_DESC");
            String FLD_DOG_ID = resultSet.getString("FLD_DOG_ID");


            insertStmt.setLong(1, Long.parseLong(FLD_DOG_CHECK_ID));
            insertStmt.setDate(2, FLD_DATE);
            insertStmt.setString(3, FLD_CHECK_DESC);
            insertStmt.setString(4, FLD_CHECK_DESC);
            insertStmt.setLong(5, Long.parseLong(FLD_DOG_CHECK_ID));
            insertStmt.setString(6, FLD_DOCTOR_ID);

            insertStmt.executeQuery();

            String insert = "Insert into " +targetSchema+".NAP_DOG_INFORMATION_APPOINTMENT " +
                    "(APPOINTMENT_ID,DOG_INFORMATION_ID) values (?,?)" ;

            PreparedStatement preparedStatement = connectiontarget.prepareStatement(insert);
            preparedStatement.setString(1,FLD_DOG_CHECK_ID);
            preparedStatement.setString(2,FLD_DOG_ID);
            preparedStatement.executeQuery();


            connectionFactory.closeStatement(insertStmt);
            connectionFactory.closeStatement(preparedStatement);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
