package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadBankAccount {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadBankAccount(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_PERSON_BANK_ID,FLD_PERSON_POLICE_ID,FLD_BANK_TYPE_ID,\n" +
                "       FLD_ACCOUNT_NO,FLD_DATE,FLD_DESC,FLD_BRANCH_NO from " +sourceSchema+".TBL_PERSON_BANK" ;

        String insertQuery =
                "Insert into " + targetSchema + ".NAP_BANK_ACCOUNT " +
                        "(BANK_ACCOUNT_ID,ACCOUNT_NO,\n" +
                        "       BRANCH_CODE,BRANCH_NAME,CREATED_BY,\n" +
                        "       CREATION_TIME,DELETED,DESCRIPTION,\n" +
                        "       LAST_UPDATE_TIME,LAST_UPDATED_BY,SHEBA_NO,BANK_ID,STATUS) " +
                        "values (?,?,?,null,'root',sysdate,0,?,sysdate,'root',null,?,'ACTIVE')";

        String insertBankAccountPerson =
                "Insert into " + targetSchema + ".NAP_BANK_ACCOUNT_PERSON " +
                        "(BANK_ACCOUNT_ID,PERSON_ID) " +
                        "values (?,?)";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);
            String FLD_PERSON_BANK_ID = resultSet.getString("FLD_PERSON_BANK_ID");
            String FLD_PERSON_POLICE_ID = resultSet.getString("FLD_PERSON_POLICE_ID");
            String FLD_BANK_TYPE_ID = resultSet.getString("FLD_BANK_TYPE_ID");
            String FLD_ACCOUNT_NO = resultSet.getString("FLD_ACCOUNT_NO");
            Date FLD_DATE = resultSet.getDate("FLD_DATE");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_BRANCH_NO = resultSet.getString("FLD_BRANCH_NO");

            insertStmt.setLong(1, Long.parseLong(FLD_PERSON_BANK_ID));
            insertStmt.setString(2, FLD_ACCOUNT_NO);
            insertStmt.setString(3, FLD_BRANCH_NO);
            insertStmt.setString(4,FLD_DESC);
            insertStmt.setString(5,FLD_BANK_TYPE_ID);

            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);

            String selectAccused = "select ACCUSED_ID \n" +
                    " from " +targetSchema+".NAP_ACCUSED where ACCUSED_ID ="+FLD_PERSON_POLICE_ID ;

            PreparedStatement preparedStatement = connectiontarget.prepareStatement(selectAccused);
            ResultSet accusedResultSet = preparedStatement.executeQuery();

            while(accusedResultSet.next()){
                PreparedStatement preparedStatement1 = connectiontarget.prepareStatement(insertBankAccountPerson);
                preparedStatement1.setString(1,FLD_PERSON_BANK_ID);
                preparedStatement1.setString(2,FLD_PERSON_POLICE_ID);

                preparedStatement1.executeQuery();
                connectionFactory.closeStatement(preparedStatement1);
            }

            connectionFactory.closeResultSet(accusedResultSet);
            connectionFactory.closeStatement(preparedStatement);

        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }


    public HashMap<String, String> getMap() {
        return map;
    }



}
