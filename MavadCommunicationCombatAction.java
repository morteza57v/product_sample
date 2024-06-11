package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;



public class MavadCommunicationCombatAction {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadCommunicationCombatAction(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection , Connection connectiontarget, String connectionCamunda) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
        // this.connectionCamunda = connectionCamunda;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select" +
                " FLD_INTERACTION_ID," +
                "FLD_RELATION_OPERATION_ID," +
                "FLD_REGISTER_DATE," +
                "from " +sourceSchema+".TBL_INTERACTION" ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_COMMUNICATION_COMBAT_ACTION(" +
                "    COMMUNICATION_COMBAT_ACTION_ID ,\n" +
                "    PROCESS_STATUS                ,\n" +
                "    ACTION_TYPE                   ,\n" +
                "    CREATED_BY                    ,\n" +
                "    CREATION_TIME                 ,\n" +
                "    DATE_PROPOSE_ACTION           ,\n" +
                "    DELETED                       ,\n" +
                "    DESCRIPTION                   ,\n" +
                "    DESCRIPTION_NECESSARY_ACTION  ,\n" +
                "    DESCRIPTION_NECESSARY_RESOURCE,\n" +
                "    END_TIME                      ,\n" +
                "    LAST_UPDATE_TIME              ,\n" +
                "    LAST_UPDATED_BY               ,\n" +
                "    LATITUDE                      ,\n" +
                "    LONGITUDE                     ,\n" +
                "    MARFOK_CODE                   ,\n" +
                "    PROPOSE_COMBAT_ACTION         ,\n" +
                "    START_TIME                    ,\n" +
                "    URGENCY_ACTION                ,\n" +
                "    CLUE_ID                       ,\n" +
                "    STATUS                        ,\n" +
                "    AGREE                         ,\n" +
                "    CHIEF_AGREE                   ,\n" +
                "    CHIEF_COMMENT                 ,\n" +
                "    MANAGER_COMMENT               ,\n" +
                "    GEO_LOCATION                  ,\n" +
                "    RESULT_DESCRIPTION            ,\n" +
                "    INSPECTION_MEETING_NOTE_ID)\n" +
                "values (?, 'CHIEF_ACCEPTED','CURRENT_DISCIPLINARY','root',?," +
                "to_timestamp('25-DEC-21 12.55.42.739000000 PM','DD-MON-RR HH.MI.SSXFF AM'),0,'convert',null,null,to_timestamp('25-DEC-21 12.55.42.739000000 PM','DD-MON-RR HH.MI.SSXFF AM')" +
                ",to_timestamp('25-DEC-21 12.55.42.739000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'root',null,null,null,'MOBILE_PATROL',to_timestamp('25-DEC-21 12.55.42.739000000 PM','DD-MON-RR HH.MI.SSXFF AM')" +
                ",'NORMAL',null,'ACTIVE','NO','NO',null,null,null,null,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);



            String selectQuery2 = "select FLD_OPERATION_ID from " +sourceSchema+".TBL_OPERATION" ;

            PreparedStatement selectStmt2 = connection.prepareStatement(selectQuery2);
            ResultSet resultSet2 = selectStmt2.executeQuery();
            resultSet2.next();
            String FLD_OPERATION_ID = resultSet2.getString("FLD_OPERATION_ID");



            String FLD_INTERACTION_ID = resultSet.getString("FLD_INTERACTION_ID");
            String FLD_RELATION_OPERATION_ID = resultSet.getString("FLD_RELATION_OPERATION_ID");

            Date FLD_REGISTER_DATE = resultSet.getDate("FLD_REGISTER_DATE");


            insertStmt.setLong(1, Long.parseLong(FLD_INTERACTION_ID));
            insertStmt.setDate(2, FLD_REGISTER_DATE);

            insertStmt.setLong(3, Long.parseLong(FLD_OPERATION_ID));



            if (FLD_RELATION_OPERATION_ID.equals("19374164")){
                insertStmt.executeQuery();
            }

            connectionFactory.closeStatement(insertStmt);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);

    }

    public HashMap<String, String> getMap() {
        return map;
    }

}
