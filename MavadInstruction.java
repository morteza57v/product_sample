package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadInstruction {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadInstruction(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select fld_course_student_id, fld_student_id, fld_course_id, fld_dog_id,\n" +
                "       fld_professor_idea from " +sourceSchema+".TBL_DOG_COACH_CORS_STUDENT" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_INSTRUCTION(" +
                "    INSTRUCTION_ID        ,\n" +
                "    AGREE      ,\n" +
                "    DOG_SPECIALTY_CATEGORIES ,\n" +
                "    END_DATE ,\n" +
                "    INSTRUCTION_DESCRIPTION ,\n" +
                "    INSTRUCTION_TYPE ,\n" +
                "    PROCESS_STATUS ,\n" +
                "    REQUIRED_HOURS ,\n" +
                "    START_DATE ,\n" +
                "    TALENT_DESCRIPTION ,\n" +
                "    TALENT_TYPE ,\n" +
                "    ASSESSOR_EXPERT_NAME_EMPLOYEE_ID ,\n" +
                "    DOG_INFORMATION_ID ,\n" +
                "    SECONDER_NAME_EMPLOYEE_ID     )\n" +
                "values (?,null,NULL,sysdate,?,null,null,null,sysdate,null,null,?,?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String fld_course_student_id = resultSet.getString("fld_course_student_id");
            String fld_professor_idea = resultSet.getString("fld_professor_idea");
            String fld_student_id = resultSet.getString("fld_student_id");
            String fld_dog_id = resultSet.getString("fld_dog_id");
            String fld_course_id = resultSet.getString("fld_course_id");

            insertStmt.setLong(1, Long.parseLong(fld_course_student_id));
            insertStmt.setString(2, fld_professor_idea);
            insertStmt.setString(3, fld_student_id);
            insertStmt.setString(4, fld_dog_id);
            insertStmt.setString(5, fld_student_id);

            System.out.println("fld_student_id:"+fld_student_id);
            System.out.println("fld_course_student_id:"+fld_course_student_id);

            insertStmt.executeQuery();


            String insertInstructionDogTrainingCourse = "Insert into " + targetSchema +".NAP_INSTRUCTION_DOG_TRAINING_COURSE(" +
                    "    DOG_TRAINING_COURSE_ID  ,\n" +
                    "    INSTRUCTION_ID     )\n" +
                    "values (?,?)\n";

            String insertInstructionEmployee = "Insert into " + targetSchema +".NAP_EMPLOYEE_INSTRUCTION(" +
                    "    DOG_TRAINER_EMPLOYEE_ID  ,\n" +
                    "    EMPLOYEE_ID     )\n" +
                    "values (?,?)\n";

            PreparedStatement preparedStatement = connectiontarget.prepareStatement(insertInstructionDogTrainingCourse);
            PreparedStatement preparedSt = connectiontarget.prepareStatement(insertInstructionEmployee);

            preparedStatement.setString(1,fld_course_student_id);
            preparedStatement.setString(2,fld_course_id);

            preparedSt.setString(1,fld_course_student_id);
            preparedSt.setString(2,fld_student_id);

            preparedStatement.executeQuery();
            preparedSt.executeQuery();

            connectionFactory.closeStatement(insertStmt);
            connectionFactory.closeStatement(preparedStatement);
            connectionFactory.closeStatement(preparedSt);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
