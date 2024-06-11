package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadCoachTraining {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadCoachTraining(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select fld_dog_coach_course_id,\n" +
                "       fld_from_date,fld_todate,\n" +
                "       fld_desc from " +sourceSchema+".TBL_DOG_COACH_COURSE_INFO" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_COACH_TRAINING(" +
                "    COACH_TRAINING_ID ,\n" +
                "    AGREE      ,\n" +
                "    END_DATE      ,\n" +
                "    INSTRUCTION_DESCRIPTION ,\n" +
                "    PROCESS_STATUS ,\n" +
                "    REQUIRED_HOURS ,\n" +
                "    START_DATE ,\n" +
                "    COACH_HEAD_EMPLOYEE_ID     )\n" +
                "values (?,null,?,?,null,null,?,null)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String fld_dog_coach_course_id = resultSet.getString("fld_dog_coach_course_id");
            Date fld_from_date = resultSet.getDate("fld_from_date");
            Date fld_to_date = resultSet.getDate("fld_todate");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            insertStmt.setLong(1, Long.parseLong(fld_dog_coach_course_id));
            insertStmt.setDate(2, fld_to_date);
            insertStmt.setString(3, FLD_DESC);
            insertStmt.setDate(4, fld_from_date);

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
