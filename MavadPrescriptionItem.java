package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MavadPrescriptionItem {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;
    private Connection connectionCamunda;

    public MavadPrescriptionItem(String targetSchema, String sourceSchema, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select fld_drugs_out_doc_id, fld_drug_id, fld_no, fld_desc,\n" +
                "       fld_dog_id,fld_request_date,\n" +
                "       fld_deliver_to_id, fld_deliver_from_id,\n" +
                "       fld_exit_date from " +sourceSchema+".TBL_DOG_OUT_DOC_DRUGS" ;

        String insertQuery = "Insert into " + targetSchema +".NAP_PRESCRIPTION_ITEM(" +
                "    PRESCRIPTION_ITEM_ID ,\n" +
                "    DELIVERY      ,\n" +
                "    DOCTOR_ORDERS ,\n " +
                "    NUMBER_ITEM ,\n " +
                "    APPOINTMENT_ID ,\n " +
                "    )\n" +
                "values (?,?,?,?)\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while(resultSet.next()){

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_COURSE_TYPE_ID = resultSet.getString("FLD_COURSE_TYPE_ID");
            String FLD_DESC = resultSet.getString("FLD_DESC");

            insertStmt.setLong(1, Long.parseLong(FLD_COURSE_TYPE_ID));
            insertStmt.setString(2, FLD_DESC);

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
