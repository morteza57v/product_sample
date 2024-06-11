package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author Morteza
 */

public class MavadAnotherPresentMember {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;

    public MavadAnotherPresentMember(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection, Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectOperationPersonal = "select FLD_OPERATION_PERSONEL_ID,\n" +
                "       FLD_OPERATION_ID,FLD_DESC,FLD_PERSONEL_ID,FLD_PERSONEL_OP_EFFECT_TYPE_ID\n" +
//                "       ,FLD_MARTYR_DATE,FLD_MARTYR_LOCATION " +
                "from " + sourceSchema + ".TBL_OPERATION_PERSONEL ";


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
                "?,null,?,?,sysdate,'root',null,null," +
                "null,?,?,null,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement preparedStatement = connection.prepareStatement(selectOperationPersonal);
        ResultSet resultFindingPersonal = preparedStatement.executeQuery();

        int id = 0;

        while (resultFindingPersonal.next()) {
            String FLD_OPERATION_PERSONEL_ID = resultFindingPersonal.getString("FLD_OPERATION_PERSONEL_ID");
            System.out.println(FLD_OPERATION_PERSONEL_ID);
            String FLD_OPERATION_ID = resultFindingPersonal.getString("FLD_OPERATION_ID");
            String FLD_PERSONEL_ID = resultFindingPersonal.getString("FLD_PERSONEL_ID");
            String FLD_PERSONEL_OP_EFFECT_TYPE_ID = resultFindingPersonal.getString("FLD_PERSONEL_OP_EFFECT_TYPE_ID");
            String FLD_DESC = resultFindingPersonal.getString("FLD_DESC");

            if(FLD_PERSONEL_OP_EFFECT_TYPE_ID != null){
                if (FLD_PERSONEL_OP_EFFECT_TYPE_ID.equalsIgnoreCase("1")){
                    FLD_PERSONEL_OP_EFFECT_TYPE_ID = "WOUND";
                }
                if (FLD_PERSONEL_OP_EFFECT_TYPE_ID.equalsIgnoreCase("2")){
                    FLD_PERSONEL_OP_EFFECT_TYPE_ID = "DEATH";
                }
            }

            String selectPersonal = "select FLD_PERSONEL_NAME,FLD_PERSONEL_FAMILY " +
                    "from " +sourceSchema+".TBL_PERSONEL where FLD_PERSONEL_ID ="+FLD_PERSONEL_ID ;
            PreparedStatement preparedPersonal = connection.prepareStatement(selectPersonal);
            ResultSet resultInspection = preparedPersonal.executeQuery();
            String FLD_PERSONEL_NAME = null;
            String FLD_PERSONEL_FAMILY = null;
            while (resultInspection.next()){
                FLD_PERSONEL_NAME = resultInspection.getString("FLD_PERSONEL_NAME");
                FLD_PERSONEL_FAMILY = resultInspection.getString("FLD_PERSONEL_FAMILY");
            }

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);
            insertStmt.setString(1, FLD_OPERATION_PERSONEL_ID);
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setString(3, FLD_PERSONEL_NAME);
            insertStmt.setString(4, FLD_PERSONEL_FAMILY);
//            insertStmt.setString(5, FLD_OFFICER_TYPE_ID);
//            insertStmt.setString(6, FLD_REWARD);
            insertStmt.setString(5, FLD_OPERATION_ID);
            insertStmt.setString(6, FLD_PERSONEL_ID);
//            insertStmt.setString(9, OCPI_ID);


            String insertMemberLifeDamage = "Insert into " + targetSchema + ".NAP_MEMBER_LIFE_DAMAGE(" +
                   "MEMBER_LIFE_DAMAGE_ID, COMMENTS, CREATED_BY, CREATION_TIME, DELETED,\n" +
                    "       DESCRIPTION, INCIDENT_ADDRESS, INCIDENT_DATE, INCIDENT_TYPE, LAST_UPDATE_TIME,\n" +
                    "       LAST_UPDATED_BY, LATITUDE, LONGITUDE, REASON, WOUND_COMMENT,\n" +
                    "       INSPECTION_MEETING_NOTE_ID, PRESENT_MEMBER_ID, STATUS) "+
                    "values (?,?,'root',sysdate,0," +
                    "?,null,null,?,sysdate,'root',null,null," +
                    "null,null,?,?,'ACTIVE')\n";

            id ++;
            System.out.println(id);
            PreparedStatement insertStmtMemberLifeDamage = connectionTarget.prepareStatement(insertMemberLifeDamage);
            insertStmtMemberLifeDamage.setInt(1, id);
            insertStmtMemberLifeDamage.setString(2, FLD_DESC);
            insertStmtMemberLifeDamage.setString(3, FLD_DESC);
            insertStmtMemberLifeDamage.setString(4, FLD_PERSONEL_OP_EFFECT_TYPE_ID);
//            insertStmt.setString(4, FLD_PERSONEL_FAMILY);
//            insertStmt.setString(5, FLD_OFFICER_TYPE_ID);
//            insertStmt.setString(6, FLD_REWARD);
            insertStmtMemberLifeDamage.setString(5, FLD_OPERATION_ID);
            insertStmtMemberLifeDamage.setString(6, FLD_OPERATION_PERSONEL_ID);
//            insertStmt.setString(9, OCPI_ID);


            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);

            insertStmtMemberLifeDamage.executeQuery();
            connectionFactory.closeStatement(insertStmtMemberLifeDamage);

            connectionFactory.closeResultSet(resultInspection);
            connectionFactory.closeStatement(preparedPersonal);
        }

        connectionFactory.closeResultSet(resultFindingPersonal);
        connectionFactory.closeStatement(preparedStatement);
    }

    public HashMap<String, String> getMap() {
        return map;
    }
}
