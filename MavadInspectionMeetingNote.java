package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;



public class MavadInspectionMeetingNote {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadInspectionMeetingNote(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_OPERATION_ID,FLD_OPERATION_TYPE_ID,FLD_CASE_ID," +
                "FLD_DESC,FLD_AUTHORITY_NO,FLD_AUTHORITY_DATE,FLD_DOING_DATE,FLD_PROVINCE_ID " +
                "from " +sourceSchema+".TBL_OPERATION where FLD_PROVINCE_ID is not null " ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_INSPECTION_MEETING_NOTE(" +
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
                "    PROVINCE_ID              )\n" +
                "values ( ?,1,null,null,'root',sysdate,0," +
                "?,1,null,null,null,?,null,?," +
                "null,?,'convert',null,sysdate,'root',null,null,0,null,null," +
                "null,null,null,null,null,?,null,?,'ACTIVE',null,null,null,null," +
                "null,null,null,null,null,'FOLDER',null,null,null,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();


        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);


            String FLD_OPERATION_ID = resultSet.getString("FLD_OPERATION_ID");
            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_AUTHORITY_NO = resultSet.getString("FLD_AUTHORITY_NO");
//            String FLD_OPERATION_TYPE_ID = resultSet.getString("FLD_OPERATION_TYPE_ID");
            String FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
//            if(FLD_OPERATION_TYPE_ID != null){
//                if(FLD_OPERATION_TYPE_ID.equalsIgnoreCase("1")){
//                    FLD_OPERATION_TYPE_ID = "گشت و کیمن";
//                }
//                if(FLD_OPERATION_TYPE_ID.equalsIgnoreCase("2")){
//                    FLD_OPERATION_TYPE_ID = "عملیات";
//                }
//                if(FLD_OPERATION_TYPE_ID.equalsIgnoreCase("3")){
//                    FLD_OPERATION_TYPE_ID = "درگیری";
//                }
//            }
            Date FLD_AUTHORITY_DATE = resultSet.getDate("FLD_AUTHORITY_DATE");
            Date FLD_DOING_DATE = resultSet.getDate("FLD_DOING_DATE");

            String selectSocial = "select FLD_CASE_ID,FLD_SOCIAL_SEC_IMPROV_PLAN_ID " +
                    "from " +sourceSchema+".TBL_CASES where FLD_CASE_ID =  "+FLD_CASE_ID ;

            PreparedStatement preparedStatement = connection.prepareStatement(selectSocial);
            ResultSet resultSocial = preparedStatement.executeQuery();

            String FLD_SOCIAL_SEC_IMPROV_PLAN_ID = null;

            while(resultSocial.next()){
                FLD_SOCIAL_SEC_IMPROV_PLAN_ID = resultSocial.getString("FLD_SOCIAL_SEC_IMPROV_PLAN_ID");
            }


            insertStmt.setLong(1, Long.parseLong(FLD_OPERATION_ID));
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setDate(3, FLD_DOING_DATE);
//            insertStmt.setString(4, FLD_OPERATION_TYPE_ID);
            insertStmt.setDate(4, FLD_AUTHORITY_DATE);
            insertStmt.setString(5, FLD_AUTHORITY_NO);
            insertStmt.setLong(6, Long.parseLong(FLD_CASE_ID));
            insertStmt.setString(7, FLD_SOCIAL_SEC_IMPROV_PLAN_ID);
            insertStmt.setLong(8, Long.parseLong(FLD_PROVINCE_ID));


            insertStmt.executeQuery();

            connectionFactory.closeStatement(insertStmt);

            connectionFactory.closeResultSet(resultSocial);
            connectionFactory.closeStatement(preparedStatement);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }


}
