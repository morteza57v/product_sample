package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MavadRegistryAndDiscoveryBonus {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadRegistryAndDiscoveryBonus(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_FINDING_REWARD_BASE_ID, FLD_NARCOTIC_TYPE_ID, FLD_MEASURMENT_UNIT_TYPE_ID,\n" +
                "       FLD_ORG_GROUP_TYPE_ID, FLD_AMOUNT_FROM, FLD_AMOUNT_TO, FLD_PRICE,\n" +
                "       FLD_DESC from " +sourceSchema+".TBL_FINDING_REWARD_BASE where FLD_AMOUNT_FROM is not null  and FLD_AMOUNT_TO is not null" ;

        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_REGISTRATION_AND_ACTIVATION_DISCOVERY_BONUS_FORMULA(" +
                "REGISTRATION_AND_ACTIVATION_DISCOVERY_BONUS_FORMULA_ID," +
                "CREATION_TIME," +
                "CREATED_BY," +
                "DELETED," +
                "DESCRIPTION," +
                "EQUAL," +
                "LAST_UPDATE_TIME," +
                "LAST_UPDATED_BY," +
                "MAXIMUM," +
                "MINIMUM," +
                "PARAMETER_ABBREVIATION," +
                "PRICE," +
                "PRICE_UNITE," +
                "DRUGS_TYPE_ID," +
                "MEASURE_UNIT_ID," +
                "RECORD_RULES_AND_DISCOVERY_REWARDS_ID," +
                "STATUS," +
                "UNITS_ID)\n" +
                "values (?,sysdate,'root',0,?,null,sysdate,'root',?,?,null,?,'RIAL',?,?,21,'ACTIVE',18595113)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_FINDING_REWARD_BASE_ID = resultSet.getString("FLD_FINDING_REWARD_BASE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_AMOUNT_FROM = resultSet.getString("FLD_AMOUNT_FROM");
            String FLD_AMOUNT_TO = resultSet.getString("FLD_AMOUNT_TO");
            String FLD_PRICE = resultSet.getString("FLD_PRICE");
            String FLD_NARCOTIC_TYPE_ID = resultSet.getString("FLD_NARCOTIC_TYPE_ID");
            String FLD_MEASURMENT_UNIT_TYPE_ID = resultSet.getString("FLD_MEASURMENT_UNIT_TYPE_ID");

            insertStmt.setLong(1, Long.parseLong(FLD_FINDING_REWARD_BASE_ID));
            insertStmt.setString(2, FLD_DESC);
            insertStmt.setString(3, FLD_AMOUNT_TO);
            insertStmt.setString(4, FLD_AMOUNT_FROM);
            insertStmt.setString(5, FLD_PRICE);
            insertStmt.setString(6, FLD_NARCOTIC_TYPE_ID);
            insertStmt.setString(7, FLD_MEASURMENT_UNIT_TYPE_ID);

            System.out.println("1:"+FLD_FINDING_REWARD_BASE_ID);
            System.out.println("2:"+FLD_DESC);
            System.out.println("3:"+FLD_AMOUNT_FROM);
            System.out.println("4:"+FLD_AMOUNT_TO);
            System.out.println("5:"+FLD_PRICE);
            System.out.println("6:"+FLD_NARCOTIC_TYPE_ID);
            System.out.println("7:"+FLD_MEASURMENT_UNIT_TYPE_ID);

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
