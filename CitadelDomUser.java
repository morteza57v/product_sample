package net.navoshgaran.mavad.cnv;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.MavadConverter;

public class CitadelDomUser {

	private String targetSchema;
	private String sourceSchema;
	private HashMap<String, String> userMap;
	private HashMap<Long, Long> map = new HashMap<Long, Long>();
	private Connection connection;
	private Connection connectiontarget;

	private IdGenerator idGenerator;

	public CitadelDomUser(String targetSchema, String sourceSchema, HashMap<String, String> userMap,
			IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
		this.targetSchema = targetSchema;
		this.sourceSchema = sourceSchema;
		this.userMap = userMap;
		this.idGenerator = idGenerator;
		this.connection = connection;
		this.connectiontarget= connectiontarget;
	}

	public HashMap<Long, Long> getMap() {
		return map;
	}

	public void run() throws Exception{

		String selectQuery = "select * from " + sourceSchema + ".tbl_user";
		String insertQuery = "insert into " + targetSchema + ".tb_domusers "
				+ "(domu_id, domu_usr_ref, domu_dom_ref, domu_access_level, domu_isactive,"
				+ " domu_passduration, domu_hascertificate, domu_logon_machines, domu_logon_hours,"
				+ " domu_can_change_password, domu_must_change_password, domu_logon_name,"
				+ " domu_startdate, domu_password)" + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate, ?)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
		PreparedStatement selectStmt = connectiontarget.prepareStatement(selectQuery);
		ResultSet resultSet = selectStmt.executeQuery();

		while (resultSet.next()) {

			String password = "";
			
//			String md5Pass = null;
//			boolean noMD5 = false;
//			try{
//				md5Pass = resultSet.getString("MD5PASS");
//			}catch(Exception ex){
//				noMD5 = true;
//			}
//			if (noMD5){
//				System.out.println("No MD5Pass Field is available!!");
//			}
//			if(md5Pass == null) {
//				password = resultSet.getString("userpass");
//				password = md5Hash(password);
//			}
//			else {
//				password = md5Pass;
//			}
			password = resultSet.getString("userpass");
			Long userSeq = resultSet.getLong("pk_user_id");
			String loginName = resultSet.getString("USERNAME");
			int active= (int)resultSet.getInt("Account_Disable");
			long newId = idGenerator.getNextId();
			map.put(userSeq, newId);

			PreparedStatement insertStmt = connection.prepareStatement(insertQuery);
			insertStmt.setLong(1, newId);
			insertStmt.setString(2, userMap.get(loginName));
			insertStmt.setLong(3, MavadConverter.domainIdx);
			insertStmt.setInt(4, 1);
			insertStmt.setString(5, active==1?"T":"F");
			insertStmt.setInt(6, 30);
			insertStmt.setString(7, "T");
			insertStmt.setString(8, MavadConverter.MACHINES);
			insertStmt.setString(9, MavadConverter.HOURS);
			insertStmt.setString(10, "T");
			insertStmt.setString(11, "F");
			insertStmt.setString(12, loginName);
			insertStmt.setString(13, password);
			insertStmt.executeQuery();
			connectionFactory.closeStatement(insertStmt);

		}
		connectionFactory.closeResultSet(resultSet);
		connectionFactory.closeStatement(selectStmt);
	}

//	private String md5Hash(String data) throws Exception {
//
//		MessageDigest md;
//		String result;
//		md = java.security.MessageDigest.getInstance("MD5");
//		md.update(data.getBytes(), 0, data.length());
//		byte[] hash = md.digest();
//		result = new BigInteger(1, hash).toString(16);
//		result = fitPassword(result);
//
//		return result;
//	}
//   private String fitPassword(String str) throws Exception {
//
//      if(str.length() > 32)   throw new Exception("Password length is invalid, password is " + str);
//
//      for(int i = 0; i < 32 - str.length(); i++)
//          str = "0" + str;
//      return str;
//  }


}
