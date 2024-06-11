package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Morteza
 */

public class MavadPresentMember {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;

    public MavadPresentMember(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection, Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectFindingPersonal = "select FLD_FINDING_PERSONEL_ID,FLD_OFFICER_ROLE_ID," +
                "FLD_OFFICER_TYPE_ID,FLD_PERSONEL_ID,FLD_CASE_ID," +
                "FLD_OPERATION_NUMBER,FLD_DESC,OCPI_ID,FLD_FIRST_NAME," +
                "FLD_LAST_NAME,FLD_REWARD " +
                "from " + sourceSchema + ".TBL_FINDING_PERSONEL ";


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema + ".NAP_PRESENT_MEMBER(" +
                "    PRESENT_MEMBER_ID ,\n" +
                "    CREATED_BY                    ,\n" +
                "    CREATION_TIME                  ,\n" +
                "    DELETED              ,\n" +
                "    DESCRIPTION                  ,\n" +
                "    DUTY               ,\n" +
                "    FIRST_NAME                     ,\n" +
                "    LAST_NAME                 ,\n" +
                "    LAST_UPDATE_TIME                  ,\n" +
                "    LAST_UPDATED_BY                 ,\n" +
                "    MEMBER_TYPE                  ,\n" +
                "    SAPKA_MEMBER                ,\n" +
                "    SHARE_AMOUNT                ,\n" +
                "    INSPECTION_MEETING_NOTE_ID                ,\n" +
                "    EMPLOYEE_ID          ,\n" +
                "    OCPI_ID          ,\n" +
                "    STATUS        )\n" +
                "values (?,'root',sysdate,0," +
                "?,null,?,?,sysdate,'root',?,null," +
                "?,?,?,?,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement preparedStatement = connection.prepareStatement(selectFindingPersonal);
        ResultSet resultFindingPersonal = preparedStatement.executeQuery();


        while (resultFindingPersonal.next()) {
            String FLD_FINDING_PERSONEL_ID = resultFindingPersonal.getString("FLD_FINDING_PERSONEL_ID");
            String FLD_OFFICER_TYPE_ID = resultFindingPersonal.getString("FLD_OFFICER_TYPE_ID");
            String FLD_PERSONEL_ID = resultFindingPersonal.getString("FLD_PERSONEL_ID");
            String FLD_CASE_ID = resultFindingPersonal.getString("FLD_CASE_ID");
            String FLD_DESC = resultFindingPersonal.getString("FLD_DESC");
            String OCPI_ID = resultFindingPersonal.getString("OCPI_ID");
            String FLD_FIRST_NAME = resultFindingPersonal.getString("FLD_FIRST_NAME");
            String FLD_LAST_NAME = resultFindingPersonal.getString("FLD_LAST_NAME");
//            String FLD_PER_CODE = resultFindingPersonal.getString("FLD_PER_CODE");
//            String FLD_PERCENT = resultFindingPersonal.getString("FLD_PERCENT");
            String FLD_REWARD = resultFindingPersonal.getString("FLD_REWARD");

            if(FLD_OFFICER_TYPE_ID !=null){
                if(FLD_OFFICER_TYPE_ID.equalsIgnoreCase("1")){
                    FLD_OFFICER_TYPE_ID = "DISCOVERER";
                }
                if(FLD_OFFICER_TYPE_ID.equalsIgnoreCase("2")){
                    FLD_OFFICER_TYPE_ID = "DISCIPLINARY";
                }
            }

            String selectInspection = "select INSPECTION_MEETING_NOTE_ID " +
                    "from " +targetSchema+".NAP_INSPECTION_MEETING_NOTE where FOLDER_ID ="+FLD_CASE_ID ;
            PreparedStatement preparedInspection = connectionTarget.prepareStatement(selectInspection);
            ResultSet resultInspection = preparedInspection.executeQuery();
            String INSPECTION_MEETING_NOTE_ID = null;
            while (resultInspection.next()){
                INSPECTION_MEETING_NOTE_ID = resultInspection.getString("INSPECTION_MEETING_NOTE_ID");
            }

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);
            insertStmt.setString(1, FLD_FINDING_PERSONEL_ID);
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setString(3, FLD_FIRST_NAME);
            insertStmt.setString(4, FLD_LAST_NAME);
            insertStmt.setString(5, FLD_OFFICER_TYPE_ID);
            insertStmt.setString(6, FLD_REWARD);
            insertStmt.setString(7, INSPECTION_MEETING_NOTE_ID);
            insertStmt.setString(8, FLD_PERSONEL_ID);
            insertStmt.setString(9, OCPI_ID);

            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);

            connectionFactory.closeResultSet(resultInspection);
            connectionFactory.closeStatement(preparedInspection);
        }

        connectionFactory.closeResultSet(resultFindingPersonal);
        connectionFactory.closeStatement(preparedStatement);
    }

    public HashMap<String, String> getMap() {
        return map;
    }
}
