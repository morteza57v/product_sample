package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadDiscoveryNarcotic {

    private String targetSchema;
    private String Camunda;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectiontarget;

    public MavadDiscoveryNarcotic(String targetSchema, String sourceSchema, IdGenerator idGenerator, String Camunda, Connection connection, Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.Camunda = Camunda;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_NARCOTICS_FINDING_ID,FLD_NARCOTIC_TYPE_ID,FLD_CASE_ID,FLD_DESC,FLD_MEASURMENT_UNIT_TYPE_ID,\n" +
                "       FLD_FINDING_AMOUNT,FLD_FINDIND_DATE,FLD_FINDING_AMOUNT2,FLD_MEASURMENT_UNIT_TYPE_ID2,\n" +
                "       FLD_PROVINCE_ID,FLD_EMBED_TYPE_ID,FLD_EMBED_METHOD_ID from " + sourceSchema + ".TBL_NARCOTICS_FINDING";


        String insertQuery =
                "Insert into " + targetSchema + ".NAP_INVENTORY_ITEM " +
                        "(INVENTORY_ITEM_ID ,\n" +
                        "    ACTUAL_WEIGHT ,\n" +
                        "    ACTUAL_WEIGHT_WITHOUT_COVER ,\n" +
                        "    APPEARANCE_WEIGHT ,\n" +
                        "    COVER_TYPE  ,\n" +
                        "    CREATED_BY ,\n" +
                        "    CREATION_TIME,\n" +
                        "    CURRENT_QUANTITY ,\n" +
                        "    DELETED,\n" +
                        "    DESCRIPTION,\n" +
                        "    DESTROYED ,\n" +
                        "    DISCOVERED ,\n" +
                        "    DRUG_PURE_WEIGHT,\n" +
                        "    DRUG_PURITY,\n" +
                        "    EXIT_COUNT,\n" +
                        "    EXIT_DATE,\n" +
                        "    EXPIRE_DATE,\n" +
                        "    INPUT_QUANTITY,\n" +
                        "    INVENTORY_PROCESS_STATUS,\n" +
                        "    INVENTORY_SERIAL_NO,\n" +
                        "    ITEM_TYPE,\n" +
                        "    LAB_DELIVERED,\n" +
                        "    LAB_PROCESS_STATUS,\n" +
                        "    LAB_RECEIVE_DATE ,\n" +
                        "    LAB_SERIAL_NO,\n" +
                        "    LAST_UPDATE_TIME ,\n" +
                        "    LAST_UPDATED_BY,\n" +
                        "    OLD_WEIGHT,\n" +
                        "    OTHER_DRUGS_NAME,\n" +
                        "    PACKAGE_DATE,\n" +
                        "    PROCESS_TYPE ,\n" +
                        "    RECEIVER_NAME,\n" +
                        "    STOCK_PROCESS_STATUS,\n" +
                        "    STOCK_RECEIVE_DATE,\n" +
                        "    WEIGHING_WEIGHT,\n" +
                        "    ACTUAL_DRUGS_TYPE_ID,\n" +
                        "    APPEARANCE_DRUGS_TYPE_ID,\n" +
                        "    BARCODE_ID,\n" +
                        "    DEFAULT_RECEIVER,\n" +
                        "    DELIVERY_TO_INVENTORY,\n" +
                        "    DELIVERY_TO_LABORATORY,\n" +
                        "    DISCOVERY_CITY_ID,\n" +
                        "    DISCOVERY_PROVINCE_ID,\n" +
                        "    DISCOVERY_REGION_ID,\n" +
                        "    FIRST_KEEPING_PLACE,\n" +
                        "    INSPECTION_MEETING_NOTE_ID,\n" +
                        "    INVENTORY_ID,\n" +
                        "    LAB_RECEIVER_EMPLOYEE,\n" +
                        "    LABORATORY_ID,\n" +
                        "    PRODUCT_ID,\n" +
                        "    EMPLOYEE_ID,\n" +
                        "    SAMPLE_KEEPING_PLACE,\n" +
                        "    STATUS,\n" +
                        "    ZONE_PARTITION_ID,\n" +
                        "    MEASURE_UNIT_ID,\n" +
                        "    LAB_TESTER_ID,\n" +
                        "    ACTUAL_MEASURE_UNIT_ID,\n" +
                        "    EMBED_TYPE_ID,\n" +
                        "    EMBED_METHOD_ID\n" +
                        "    ) values (?,?,?,?,null,'root',sysdate,null,0,null,null,1 ," +
                        "null ,null,null ,null ,null ,null ,null ,null ,null ,null,null ,null ,null" +
                        ",sysdate,'root',null,null,?,null,null ,null ,null ,null,?,?,null ,null " +
                        ",null ,null,null,null ,null,null " +
                        ",?,null,null,null ,null,null,null ,'ACTIVE',null,?,null,null,?,?)";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            PreparedStatement insertStmt = connectiontarget.prepareStatement(insertQuery);

            String FLD_NARCOTICS_FINDING_ID = resultSet.getString("FLD_NARCOTICS_FINDING_ID");
            String FLD_NARCOTIC_TYPE_ID = resultSet.getString("FLD_NARCOTIC_TYPE_ID");
            String FLD_MEASURMENT_UNIT_TYPE_ID2 = resultSet.getString("FLD_MEASURMENT_UNIT_TYPE_ID2");
//            String FLD_WAREHOUSE_ENTER_ID = resultSet.getString("FLD_WAREHOUSE_ENTER_ID");
            Double FLD_FINDING_AMOUNT2 = resultSet.getDouble("FLD_FINDING_AMOUNT2");
//            Double FLD_AMOUNT_LAFAFEH = resultSet.getDouble("FLD_AMOUNT_LAFAFEH");
            String FLD_EMBED_TYPE_ID = resultSet.getString("FLD_EMBED_TYPE_ID");
            String FLD_EMBED_METHOD_ID = resultSet.getString("FLD_EMBED_METHOD_ID");
            String FLD_CASE_ID = resultSet.getString("FLD_CASE_ID");
            Date FLD_FINDIND_DATE = resultSet.getDate("FLD_FINDIND_DATE");


//            if (FLD_AMOUNT_LAFAFEH == null) {
//                FLD_AMOUNT_LAFAFEH = 0.0;
//            }

            if (FLD_FINDING_AMOUNT2 == null) {
                FLD_FINDING_AMOUNT2 = 0.0;
            }

//            String selectQuery1 = "select FLD_CASE_ID,FLD_WAREHOUSE_ID from " + sourceSchema + ".TBL_WAREHOUSE_ENTER where FLD_WAREHOUSE_ENTER_ID =" + FLD_WAREHOUSE_ENTER_ID;
//
//            PreparedStatement selectStmt2 = connection.prepareStatement(selectQuery1);
//            ResultSet resultSet2 = selectStmt2.executeQuery();

//            String FLD_CASE_ID = null;
//            String FLD_WAREHOUSE_ID = null;
            String INSPECTION_MEETING_NOTE_ID = null;

            String selectQuery2 = "select INSPECTION_MEETING_NOTE_ID from " + targetSchema + ".NAP_INSPECTION_MEETING_NOTE where FOLDER_ID =" + FLD_CASE_ID;
            PreparedStatement selectStmt3 = connectiontarget.prepareStatement(selectQuery2);
            ResultSet resultSet3 = selectStmt3.executeQuery();

            while (resultSet3.next()) {
                INSPECTION_MEETING_NOTE_ID = resultSet3.getString("INSPECTION_MEETING_NOTE_ID");
            }
            connectionFactory.closeResultSet(resultSet3);
            connectionFactory.closeStatement(selectStmt3);
//            while (resultSet2.next()) {
//                FLD_CASE_ID = resultSet2.getString("FLD_CASE_ID");
//                FLD_WAREHOUSE_ID = resultSet2.getString("FLD_WAREHOUSE_ID");
//                if (FLD_CASE_ID != null) {
//
//
//        }
//
//    }

//            connectionFactory.closeResultSet(resultSet2);
//            connectionFactory.closeStatement(selectStmt2);


//            double FLD_AMOUNTconvert = 0;
//            double FLD_AMOUNT_LAFAFEHconvert = 0;

//            if (FLD_MEASURMENT_UNIT_TYPE_ID != null) {
//
//                switch (FLD_MEASURMENT_UNIT_TYPE_ID) {
//                    case "1":
//                        FLD_AMOUNTconvert = FLD_AMOUNT * 1000;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH * 1000;
//                        FLD_MEASURMENT_UNIT_TYPE_ID = "2";
//                        System.out.println("تبدیل تن به کیلو گرم");
//                        break;
//                    case "2":
//                        FLD_AMOUNTconvert = FLD_AMOUNT;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH;
//                        System.out.println("کیلوگرم");
//                        break;
//                    case "3":
//                        FLD_AMOUNTconvert = FLD_AMOUNT / 1000;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH / 1000;
//                        FLD_MEASURMENT_UNIT_TYPE_ID = "2";
//                        System.out.println("گرم به کیلو گرم");
//                        break;
//                    case "4":
//                        FLD_AMOUNTconvert = FLD_AMOUNT;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH;
//                        FLD_MEASURMENT_UNIT_TYPE_ID = "4";
//                        System.out.println("لیتر");
//                        break;
//                    case "5":
//                        FLD_AMOUNTconvert = FLD_AMOUNT / 100000;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH / 100000;
//                        FLD_MEASURMENT_UNIT_TYPE_ID = "2";
//                        System.out.println("سانتی گرم به کیلو گرم");
//                        break;
//                    case "6":
//                        FLD_AMOUNTconvert = FLD_AMOUNT;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH;
//                        System.out.println("عدد");
//                        break;
//                    case "7":
//                        FLD_AMOUNTconvert = FLD_AMOUNT / 1000;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH / 1000;
//                        FLD_MEASURMENT_UNIT_TYPE_ID = "4";
//                        System.out.println("سی سی");
//                        break;
//                    case "8":
//                        FLD_AMOUNTconvert = FLD_AMOUNT;
//                        FLD_AMOUNT_LAFAFEHconvert = FLD_AMOUNT_LAFAFEH;
//                        System.out.println("بوته");
//                        break;
//                    default:
//                        System.out.println("Error");
//                        break;
//
//                }
//
//            }

            System.out.println(FLD_NARCOTICS_FINDING_ID);

            insertStmt.setString(1, FLD_NARCOTICS_FINDING_ID);
            insertStmt.setDouble(2, FLD_FINDING_AMOUNT2);
            insertStmt.setDouble(3, FLD_FINDING_AMOUNT2);
            insertStmt.setDouble(4, FLD_FINDING_AMOUNT2);
            insertStmt.setDate(5, FLD_FINDIND_DATE);
            insertStmt.setString(6, FLD_NARCOTIC_TYPE_ID);
            insertStmt.setString(7, FLD_NARCOTIC_TYPE_ID);
            insertStmt.setString(8, INSPECTION_MEETING_NOTE_ID);
//            insertStmt.setString(7, FLD_WAREHOUSE_ID);
            insertStmt.setString(9, FLD_MEASURMENT_UNIT_TYPE_ID2);
            insertStmt.setString(10, FLD_EMBED_TYPE_ID);
            insertStmt.setString(11, FLD_EMBED_METHOD_ID);


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
