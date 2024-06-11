package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

public class CitadelRoleAccess {

	private String targetSchema;
	private String sourceSchema;
	private HashMap<Long, Long> map = new HashMap<Long, Long>();
	private HashMap<Long, Long> roleMap;
	private HashMap<Long, Long> secobjectMap;
	private IdGenerator idGenerator;
	private Connection connection;
	private Connection connectiontarget;

	public CitadelRoleAccess(String targetSchema, String sourceSchema,
			HashMap<Long, Long> roleMap, HashMap<Long, Long> secobjectMap,
			IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
		this.targetSchema = targetSchema;
		this.sourceSchema = sourceSchema;
		this.roleMap = roleMap;
		this.secobjectMap = secobjectMap;
		this.idGenerator = idGenerator;
		this.connection = connection;
		this.connectiontarget = connectiontarget;

	}

	public HashMap<Long, Long> getMap() {
		return map;
	}

	public void run() throws SQLException, ClassNotFoundException {

		String selectQuery = "select * from " + sourceSchema
				+ ".tbl_module_group";
		String insertQuery = "insert into "
				+ targetSchema
				+ ".tb_roleaccesses "
				+ "(racc_id, racc_rol_ref, racc_sobj_ref, racc_word, racc_access_level, racc_subtractor) "
				+ "values (?, ?, ?, ?, ?, ?)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
		PreparedStatement selectStmt = connectiontarget.prepareStatement(selectQuery);
		ResultSet resultSet = selectStmt.executeQuery();

		while (resultSet.next()) {

			long id = resultSet.getLong("pk_module_group_id");
			long newId = idGenerator.getNextId();
			map.put(id, newId);

			Long roleRef = roleMap.get(resultSet.getLong("fk_group_id"));
			Long secRef = secobjectMap.get(resultSet.getLong("fk_module_id"));
			PreparedStatement insertStmt = connection.prepareStatement(insertQuery);
			insertStmt.setLong(1, newId);
			insertStmt.setLong(2, roleRef);
			insertStmt.setLong(3, secRef);

			String accessWord = "";
			if (resultSet.getString("has_delete") != null
					&& resultSet.getString("has_delete").trim().equals("1")) {
				accessWord += "D";
			}
			if (resultSet.getString("has_update") != null
					&& resultSet.getString("has_update").trim().equals("1")) {
				accessWord += "U";
			}
			if (resultSet.getString("has_new") != null
					&& resultSet.getString("has_new").trim().equals("1")) {
				accessWord += "I";
			}
			if (resultSet.getString("has_view") != null
					&& resultSet.getString("has_view").trim().equals("1")) {
				accessWord += "S";
			}

			insertStmt.setString(4, accessWord);
			insertStmt.setString(5, "1");
			insertStmt.setString(6, "F");
			if (!accessWord.trim().equals(""))
				insertStmt.executeQuery();
			connectionFactory.closeStatement(insertStmt);
		}
		connectionFactory.closeResultSet(resultSet);
		connectionFactory.closeStatement(selectStmt);
	}
}
