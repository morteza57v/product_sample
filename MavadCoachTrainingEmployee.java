package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;

public class MavadCoachTrainingEmployee {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadCoachTrainingEmployee(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_COURSE_ID,FLD_PERSONEL_ID \n" +
                "from " +sourceSchema+".TBL_DOG_COACH_CORS_PROFESOR" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_COACH_TRAINING_COACH_EMPLOYEE(" +
                "    COACH_TRAINING_ID ,\n" +
                "    COACH_EMPLOYEE_ID )\n" +
                "values (?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_COURSE_ID = resultSet.getString("FLD_COURSE_ID");
            String FLD_PERSONEL_ID = resultSet.getString("FLD_PERSONEL_ID");


            insertStmt.setLong(1, Long.parseLong(FLD_COURSE_ID));
            insertStmt.setLong(2, Long.parseLong(FLD_PERSONEL_ID));


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
