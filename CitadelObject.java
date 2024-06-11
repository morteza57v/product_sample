package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.MavadConverter;

public class CitadelObject {

	private String targetSchema;
	private String sourceSchema;
	private HashMap<Long, Long> map = new HashMap<Long, Long>();
	private IdGenerator idGenerator;
	private Connection connection;
	private Connection connectiontarget;

	public CitadelObject(String targetSchema, String sourceSchema,
			IdGenerator idGenerator, Connection connection,
			Connection connectiontarget) {
		this.targetSchema = targetSchema;
		this.sourceSchema = sourceSchema;
		this.idGenerator = idGenerator;
		this.connection = connection;
		this.connectiontarget = connectiontarget;
	}

	public HashMap<Long, Long> getMap() {
		return map;
	}

	public void run() throws SQLException, ClassNotFoundException {

		String selectQuery = "select pk_module_id, module_name, module_title from "
				+ sourceSchema + ".tbl_module";

		String insertAllURLRecordQuery = "insert into " + targetSchema
				+ ".tb_secobjects "
				+ "(sobj_id, sobj_name, sobj_caption, sobj_sys_ref, "
				+ "sobj_type, sobj_access_level, sobj_alias, sobj_fixed) "
				+ "values (" + MavadConverter.AllURLsSOBJIdx + ", 'AllUrls', 'همه مسيرها', " + MavadConverter.systemIdx + ", 'URL', 1, '/*', 'F')";

		String insertQuery = "insert into " + targetSchema + ".tb_secobjects "
				+ "(sobj_id, sobj_name, sobj_caption, sobj_sys_ref, "
				+ "sobj_type, sobj_access_level, sobj_alias, sobj_fixed) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
		PreparedStatement statement1 = connectiontarget
				.prepareStatement(selectQuery);
		ResultSet resultSet = statement1.executeQuery();

		while (resultSet.next()) {

			long id = resultSet.getLong("pk_module_id");
			long newId = idGenerator.getNextId();
			map.put(id, newId);
		}
		connectionFactory.closeResultSet(resultSet);

		PreparedStatement stmt1 = connection.prepareStatement(insertAllURLRecordQuery);
		stmt1.execute();
		connectionFactory.closeStatement(stmt1);

		resultSet = statement1.executeQuery();

//		int idx = 1;
		while (resultSet.next()) {

			PreparedStatement stmt = connection.prepareStatement(insertQuery);
			stmt.setLong(1, map.get(resultSet.getLong("pk_module_id")));
			stmt.setString(2, resultSet.getString("module_name"));
			String caption = resultSet.getString("module_title");
			if (caption != null)
				caption = caption.replaceAll("'", "");
			stmt.setString(3, caption);

			stmt.setLong(4, MavadConverter.systemIdx);

			stmt.setString(5, "ENT");
			stmt.setInt(6, 1);
			String alias = resultSet.getString("module_name");
			stmt.setString(7, alias);
			stmt.setString(8, "F");
			stmt.executeQuery();
			connectionFactory.closeStatement(stmt);
		}

		connectionFactory.closeResultSet(resultSet);
		connectionFactory.closeStatement(statement1);
	}

}
