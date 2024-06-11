package net.navoshgaran.mavad.cnv;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

import java.sql.*;
import java.util.HashMap;


public class MavadReporterPerson {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<String, String> map = new HashMap<String, String>();
    private IdGenerator idGenerator;
    private RandomStringUUID randomStringUUID;
    private Connection connection;
    private Connection connectionTarget;

    public MavadReporterPerson(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection , Connection connectionTarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectionTarget = connectionTarget;

    }


    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select FLD_REPORTER_PERSON_ID,FLD_RELIGION_TYPE_ID,FLD_HAIR_COLOR_TYPE_ID\n" +
                ",FLD_DENOMINATION_TYPE_ID,FLD_LIFE_STATUS_TYPE_ID,FLD_CLAN_TYPE_ID,FLD_EYE_COLOR_TYPE_ID\n" +
                ",FLD_SEX_ID,FLD_MARRIAGE_TYPE_ID,FLD_CITIZENSHIP_TYPE_ID\n" +
                ",FLD_PROVINCE_ID,FLD_CITY_ID,FLD_EDUCATION_CERTIF_TYPE_ID\n" +
                ",TBL_EDUCATION_FIELD_TYPE_ID,FLD_JOB_ADDRESS,FLD_JOB_TELEPHONE\n" +
                ",FLD_ADDRESS_TELEPHONE,FLD_ADDRESS_DESC,FLD_REG_DATE,FLD_NAME\n" +
                ",FLD_FATHER_NAME,FLD_FAMILY,FLD_CERTIF_NO,FLD_CERTIF_SER_NO\n" +
                ",FLD_BIRTH_DATE,FLD_BIRTH_LOCATION,FLD_ALIAS_NAME,FLD_NATIONAL_CODE\n" +
                ",FLD_FLD_WEIGH,FLD_HEIGHT,FLD_SPECIAL_VISIUAL_SIGN\n" +
                ",FLD_DESC,FLD_MOBILE,FLD_PASS,FLD_JOB_TITLE from " +sourceSchema+".TBL_REPORTER_PERSON where FLD_PROVINCE_ID is not null and FLD_CITY_ID is not null  " ;


        //TODO: Organ code should be filled with personel code or smth like that!!!
        String insertQuery = "Insert into " + targetSchema +".NAP_REPORTER_PERSON(" +
                "    REPORTER_PERSON_ID ,\n" +
                "    ACCENT ,\n" +
                "    ACCESS_LEVEL ,\n" +
                "    ACCESS_LEVEL_PERSON ,\n" +
                "    ACCOUNT_NUMBER ,\n" +
                "    ADDRESS_LOCATION ,\n" +
                "    ALIAS_NAME ,\n" +
                "    ARMED_STATUS ,\n" +
                "    ASSESSMENT ,\n" +
                "    BACKGROUND_SUMMARY ,\n" +
                "    BIRTH_DATE ,\n" +
                "    CAUSE_COLLABORATION_MOTIVATE ,\n" +
                "    CERTIFICATION_NO ,\n" +
                "    CERTIFICATION_SER_NO ,\n" +
                "    CLASS_NUMBER ,\n" +
                "    CODE_CONDEMNED_SORRY ,\n" +
                "    CODE_REPORTER ,\n" +
                "    CONNECT_TO_CONDUCTOR ,\n" +
                "    COUNTRY_DEPARTURE ,\n" +
                "    CREATED_BY ,\n" +
                "    CREATION_TIME ,\n" +
                "    CRIMINAL_RECORD ,\n" +
                "    DELETED ,\n" +
                "    DESCRIPTION ,\n" +
                "    FAMILY ,\n" +
                "    FATHER_NAME ,\n" +
                "    GENDER ,\n" +
                "    HEIGHT ,\n" +
                "    HOME_ADDRESS ,\n" +
                "    HOME_TELEPHONE ,\n" +
                "    JOB ,\n" +
                "    JOB_ADDRESS ,\n" +
                "    JOB_TELEPHONE ,\n" +
                "    KNOW_OTHER_PERSON ,\n" +
                "    LAST_UPDATE_TIME ,\n" +
                "    LAST_UPDATED_BY ,\n" +
                "    LEVEL_CONDEMNED_SORRY ,\n" +
                "    LOG_CLASS_NUMBER ,\n" +
                "    MARRIAGE ,\n" +
                "    MASS_CONDEMNED_SORRY ,\n" +
                "    MOBILE ,\n" +
                "    NAME ,\n" +
                "    NATIONAL_CODE ,\n" +
                "    OTHER_COLLABORATION ,\n" +
                "    PASSPORT_N_O ,\n" +
                "    POLICE_CHIEF ,\n" +
                "    PROPERTY_CONDEMNED_SORRY ,\n" +
                "    REPENTANT_CONVICT ,\n" +
                "    SHEBA_NUMBER ,\n" +
                "    SPECIAL_VISUAL_SIGN ,\n" +
                "    TARGET_CONDEMNED_SORRY ,\n" +
                "    TYPE_COOPERATION ,\n" +
                "    WEIGHT ,\n" +
                "    CITIZENSHIP_ID," +
                "    CITY_ID," +
                "    CLAN_ID," +
                "    DENOMINATION_ID," +
                "    EDUCATION_ID," +
                "    EDUCATION_FIELD_ID," +
                "    EYE_COLOR_ID," +
                "    HAIR_COLOR_ID," +
                "    PROVINCE_ID," +
                "    RELIGION_ID," +
                "    REPORT_COLLABORATION_METHOD_ID," +
                " STATUS)\n" +
                "values (?,null,'CONFIDENTIAL','ندارد',null,?,?," +
                "null,'ندارد','ندارد',?,'ندارد',?,?,'ندارد'," +
                "1,?,'NO RESULT',null,'root',sysdate,null,0,?,?," +
                "?,?,?,?,?,?,?,?,'NO',sysdate,'root','ندارد','ندارد'," +
                "?,'No Result',?,?,?,'no',?,'ندارد','ندارد',null,null,?,'ندارد','ندارد',?,?,?,?," +
                "?,?,?,?,?,?,?,41,'ACTIVE')\n";


        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connection.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();


        while(resultSet.next()){

            PreparedStatement insertStmt = connectionTarget.prepareStatement(insertQuery);

            String FLD_REPORTER_PERSON_ID = resultSet.getString("FLD_REPORTER_PERSON_ID");
            String FLD_ADDRESS_DESC = resultSet.getString("FLD_ADDRESS_DESC");
            String FLD_ALIAS_NAME = resultSet.getString("FLD_ALIAS_NAME");
            Date FLD_BIRTH_DATE = resultSet.getDate("FLD_BIRTH_DATE");
            String FLD_CERTIF_NO = resultSet.getString("FLD_CERTIF_NO");
            String FLD_CERTIF_SER_NO = resultSet.getString("FLD_CERTIF_SER_NO");
            String FLD_DESC = resultSet.getString("FLD_DESC");
            String FLD_FAMILY = resultSet.getString("FLD_FAMILY");
            String FLD_FATHER_NAME = resultSet.getString("FLD_FATHER_NAME");
            String FLD_SEX_ID = resultSet.getString("FLD_SEX_ID");
            String FLD_HEIGHT = resultSet.getString("FLD_HEIGHT");
            String  FLD_ADDRESS_TELEPHONE = resultSet.getString("FLD_ADDRESS_TELEPHONE");
            String  FLD_JOB_TITLE = resultSet.getString("FLD_JOB_TITLE");
            String  FLD_JOB_ADDRESS = resultSet.getString("FLD_JOB_ADDRESS");
            String  FLD_JOB_TELEPHONE = resultSet.getString("FLD_JOB_TELEPHONE");
            String  FLD_MARRIAGE_TYPE_ID = resultSet.getString("FLD_MARRIAGE_TYPE_ID");
            String  FLD_MOBILE = resultSet.getString("FLD_MOBILE");
            String  FLD_NAME = resultSet.getString("FLD_NAME");
            String  FLD_NATIONAL_CODE = resultSet.getString("FLD_NATIONAL_CODE");
            String  FLD_PASS = resultSet.getString("FLD_PASS");
            String  FLD_SPECIAL_VISIUAL_SIGN = resultSet.getString("FLD_SPECIAL_VISIUAL_SIGN");
            String  FLD_FLD_WEIGH = resultSet.getString("FLD_FLD_WEIGH");
            String  FLD_CITIZENSHIP_TYPE_ID = resultSet.getString("FLD_CITIZENSHIP_TYPE_ID");
            String  FLD_CITY_ID = resultSet.getString("FLD_CITY_ID");
            String  FLD_CLAN_TYPE_ID = resultSet.getString("FLD_CLAN_TYPE_ID");
            String  FLD_DENOMINATION_TYPE_ID = resultSet.getString("FLD_DENOMINATION_TYPE_ID");
            String  FLD_EDUCATION_CERTIF_TYPE_ID = resultSet.getString("FLD_EDUCATION_CERTIF_TYPE_ID");
            String  TBL_EDUCATION_FIELD_TYPE_ID = resultSet.getString("TBL_EDUCATION_FIELD_TYPE_ID");
            String  FLD_EYE_COLOR_TYPE_ID = resultSet.getString("FLD_EYE_COLOR_TYPE_ID");
            String  FLD_HAIR_COLOR_TYPE_ID = resultSet.getString("FLD_HAIR_COLOR_TYPE_ID");
            String  FLD_PROVINCE_ID = resultSet.getString("FLD_PROVINCE_ID");
            String  FLD_RELIGION_TYPE_ID = resultSet.getString("FLD_RELIGION_TYPE_ID");


            String GENDER = null;
            String Marriage = null;

            if( FLD_SEX_ID != null && FLD_SEX_ID.equals("2")){
                GENDER ="MALE";
            }else{
                GENDER ="FEMALE";
            }

            if(FLD_HEIGHT == null){
                FLD_HEIGHT = "100";
            }

            if(FLD_MARRIAGE_TYPE_ID != null && FLD_MARRIAGE_TYPE_ID.equalsIgnoreCase("1")){
                Marriage = "SINGLE";
            }
            if(FLD_MARRIAGE_TYPE_ID != null && FLD_MARRIAGE_TYPE_ID.equalsIgnoreCase("2")){
                Marriage = "MARRIED";
            }
            if(FLD_MARRIAGE_TYPE_ID != null && FLD_MARRIAGE_TYPE_ID.equalsIgnoreCase("3")){
                Marriage = "DIVORCED";
            }

            if(FLD_ADDRESS_DESC == null){
                FLD_ADDRESS_DESC="ندارد";
            }

            if(FLD_DESC == null){
                FLD_DESC="ندارد";
            }

            if(FLD_NATIONAL_CODE == null){
                FLD_NATIONAL_CODE="ندارد";
            }

            if(FLD_FLD_WEIGH == null){
                FLD_FLD_WEIGH="55";
            }
            if(FLD_EYE_COLOR_TYPE_ID == null){
                FLD_EYE_COLOR_TYPE_ID = "2";
            }
            if(FLD_HAIR_COLOR_TYPE_ID == null){
                FLD_HAIR_COLOR_TYPE_ID = "1";
            }



            insertStmt.setLong(1, Long.parseLong(FLD_REPORTER_PERSON_ID));
            insertStmt.setString(2, FLD_ADDRESS_DESC);
            insertStmt.setString(3, FLD_ALIAS_NAME);
            insertStmt.setDate(4, FLD_BIRTH_DATE);
            insertStmt.setString(5, FLD_CERTIF_NO);
            insertStmt.setString(6, FLD_CERTIF_SER_NO);
            insertStmt.setLong(7, Long.parseLong(FLD_REPORTER_PERSON_ID));
            insertStmt.setString(8, FLD_DESC);
            insertStmt.setString(9, FLD_FAMILY);
            insertStmt.setString(10, FLD_FATHER_NAME);
            insertStmt.setString(11, GENDER);
            insertStmt.setString(12, FLD_HEIGHT);
            insertStmt.setString(13, FLD_ADDRESS_DESC);
            insertStmt.setString(14, FLD_ADDRESS_TELEPHONE);
            insertStmt.setString(15, FLD_JOB_TITLE);
            insertStmt.setString(16, FLD_JOB_ADDRESS);
            insertStmt.setString(17, FLD_JOB_TELEPHONE);
            insertStmt.setString(18, Marriage);
            insertStmt.setString(19, FLD_MOBILE);
            insertStmt.setString(20, FLD_NAME);
            insertStmt.setString(21, FLD_NATIONAL_CODE);
            insertStmt.setString(22, FLD_PASS);
            insertStmt.setString(23, FLD_SPECIAL_VISIUAL_SIGN);
            insertStmt.setString(24, FLD_FLD_WEIGH);
            insertStmt.setString(25, FLD_CITIZENSHIP_TYPE_ID);
            insertStmt.setString(26, FLD_CITY_ID);
            insertStmt.setString(27, FLD_CLAN_TYPE_ID);
            insertStmt.setString(28, FLD_DENOMINATION_TYPE_ID);
            insertStmt.setString(29, FLD_EDUCATION_CERTIF_TYPE_ID);
            insertStmt.setString(30, TBL_EDUCATION_FIELD_TYPE_ID);
            insertStmt.setString(31, FLD_EYE_COLOR_TYPE_ID);
            insertStmt.setString(32, FLD_HAIR_COLOR_TYPE_ID);
            insertStmt.setString(33, FLD_PROVINCE_ID);
            insertStmt.setString(34, FLD_RELIGION_TYPE_ID);


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
