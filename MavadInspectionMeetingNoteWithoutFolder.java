package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadInspectionMeetingNoteWithoutFolder {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadInspectionMeetingNoteWithoutFolder(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectFolder = "select FLD_CASE_ID,FLD_PROVINCE_ID,FLD_REG_CITY_ID " +
                "from " + sourceSchema + ".TBL_CASES where FLD_REG_CITY_ID is not null ";


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema + ".NAP_INSPECTION_MEETING_NOTE(" +
                "    INSPECTION_MEETING_NOTE_ID ,\n" +
                "    ACCEPTED                    ,\n" +
                "    BIRTH_DATE                  ,\n" +
                "    CHASSIS_NUMBER              ,\n" +
                "    CREATED_BY                  ,\n" +
                "    CREATION_TIME               ,\n" +
                "    DELETED                     ,\n" +
                "    DESCRIPTION                 ,\n" +
                "    DISCOVERED                  ,\n" +
                "    FATHER_NAME                 ,\n" +
                "    FIRST_NAME                  ,\n" +
                "    HOME_ADDRESS                ,\n" +
                "    INSPECT_DATE                ,\n" +
                "    INSPECT_TYPE                ,\n" +
                "    JUDGE_COMMAND_DATE          ,\n" +
                "    JUDGE_COMMAND_METHOD        ,\n" +
                "    JUDGE_COMMAND_NO            ,\n" +
                "    JUDGE_ISSUED_FROM           ,\n" +
                "    LAST_NAME                   ,\n" +
                "    LAST_UPDATE_TIME            ,\n" +
                "    LAST_UPDATED_BY             ,\n" +
                "    LIFE_DAMAGE                 ,\n" +
                "    NATIONAL_CODE               ,\n" +
                "    NO_OWNER                    ,\n" +
                "    PLAQUE                      ,\n" +
                "    PROPERTY_DAMAGE             ,\n" +
                "    TEAM_NAME                   ,\n" +
                "    VEHICLE_PRODUCTION_YEAR     ,\n" +
                "    WEIGHING_PROCESS_STATUS     ,\n" +
                "    WORK_ADDRESS                ,\n" +
                "    WORKSHOP_DESTRUCTION        ,\n" +
                "    FOLDER_ID                           ,\n" +
                "    ISSUED_CITY_ID                      ,\n" +
                "    MANAGEMENT_ENFORCEMENT_PLAN_ID      ,\n" +
                "    STATUS                              ,\n" +
                "    TEAM_MANGER                         ,\n" +
                "    VEHICLE_BRAND_ID                    ,\n" +
                "    VEHICLE_CLASS_ID                    ,\n" +
                "    VEHICLE_COLOR_ID                    ,\n" +
                "    VEHICLE_TYPE_ID                     ,\n" +
                "    RESULT_COMBAT_ACTION_ID             ,\n" +
                "    RESULT_DISCIPLINARY_ACTION_ID       ,\n" +
                "    RESULT_COMBAT_ACTIONS_OR_EVENTS     ,\n" +
                "    RESULT_ROUTINE_POLICE_ACTIONS       ,\n" +
                "    MEETING_TYPE                        ,\n" +
                "    LATITUDE                            ,\n" +
                "    LONGITUDE                           ,\n" +
                "    UNITS_ID                            ,\n" +
                "    CITY_ID                            ,\n" +
                "    PROVINCE_ID              )\n" +
                "values (?,1,null,null,'root',sysdate,0," +
                "null,1,null,null,null,null,null,null," +
                "null,null,'convert',null,sysdate,'root',null,null,0,null,null," +
                "null,null,null,null,null,?,?,null,'ACTIVE',null,null,null,null," +
                "null,null,null,null,null,'FOLDER',null,null,null,?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement preparedStatement = connection.prepareStatement(selectFolder);
        ResultSet resultFolder = preparedStatement.executeQuery();

        String selectInspection = null;

        while (resultFolder.next()) {
            String FLD_CASE_ID = resultFolder.getString("FLD_CASE_ID");
            String FLD_REG_CITY_ID = resultFolder.getString("FLD_REG_CITY_ID");
            String FLD_PROVINCE_ID = resultFolder.getString("FLD_PROVINCE_ID");
            selectInspection = "select FLD_OPERATION_ID,FLD_OPERATION_TYPE_ID,FLD_CASE_ID,FLD_DESC,FLD_AUTHORITY_NO,FLD_AUTHORITY_DATE," +
//                    "FLD_DOING_DATE,FLD_PROVINCE_ID from " +sourceSchema+".TBL_OPERATION where FLD_CASE_ID ="+FLD_CASE_ID ;
                    "FLD_DOING_DATE,FLD_PROVINCE_ID from " + sourceSchema + ".TBL_OPERATION";
            PreparedStatement preparedInspection = connection.prepareStatement(selectInspection);
            ResultSet resultInspection = preparedInspection.executeQuery();
            while (!resultInspection.next()) {
                PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);
                long newId = idGenerator.getNextId();
                insertStmt.setLong(1, newId);
                insertStmt.setString(2, FLD_CASE_ID);
                insertStmt.setString(3, FLD_REG_CITY_ID);
                insertStmt.setString(4, FLD_REG_CITY_ID);
                insertStmt.setString(5, FLD_PROVINCE_ID);

                insertStmt.executeQuery();

                connectionFactory.closeStatement(insertStmt);
                break;
            }
            connectionFactory.closeResultSet(resultInspection);
            connectionFactory.closeStatement(preparedInspection);
        }

        connectionFactory.closeResultSet(resultFolder);
        connectionFactory.closeStatement(preparedStatement);
    }

    public HashMap<String, String> getMap() {
        return map;
    }

}
