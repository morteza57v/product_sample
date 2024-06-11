package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.MavadConverter;

public class CitadelRoleprivilege {

	private String targetSchema;
	private HashMap<Long, Long> map = new HashMap<Long, Long>();
	private HashMap<Long, Long> roleMap;
	private IdGenerator idGenerator;
	private Connection connection;

	public CitadelRoleprivilege(String targetSchema,
			HashMap<Long, Long> roleMap, IdGenerator idGenerator,
			Connection connection) {
		this.targetSchema = targetSchema;
		this.roleMap = roleMap;
		this.idGenerator = idGenerator;
		this.connection = connection;
	}

	public HashMap<Long, Long> getMap() {
		return map;
	}

	public void run() throws SQLException, ClassNotFoundException {

		String insertNrmRoleQuery = "insert into "
				+ targetSchema
				+ ".tb_roles "
				+ "(rol_id, rol_sys_ref, rol_codename, rol_isbase, rol_fixed, "
				+ "rol_type, rol_name, rol_subtractor) "
				+ "values ("
				+ MavadConverter.LICRoleIdx
				+ ", "
				+ MavadConverter.systemIdx
				+ ",'mngConnect', 'F', 'F', 'G', 'ورود به سیستم اجرائیات', 'F')";

		String insertQuery = "insert into " + targetSchema
				+ ".tb_roleprivileges "
				+ "(rprv_id, rprv_rol_ref_1, rprv_rol_ref_2, rprv_fixed) "
				+ "values (?, ?, ?, ?)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();

		PreparedStatement insertNrmRoleStmt = connection
				.prepareStatement(insertNrmRoleQuery);
		insertNrmRoleStmt.execute();
		connectionFactory.closeStatement(insertNrmRoleStmt);

		for (Long roleId : roleMap.keySet()) {
			Long newRoleId = roleMap.get(roleId);
			long newId = idGenerator.getNextId();

			Long ref1 = newRoleId;
			if (ref1 == null)
				continue;
			Long ref2 = MavadConverter.LICRoleIdx;

			PreparedStatement insertStmt = connection.prepareStatement(insertQuery);

			insertStmt.setLong(1, newId);
			insertStmt.setLong(2, ref1);
			insertStmt.setLong(3, ref2);
			insertStmt.setString(4, "F");
			insertStmt.executeQuery();
			connectionFactory.closeStatement(insertStmt);
		}

		String insertAllUrlsRoleAccessQuery = "insert into "
				+ targetSchema
				+ ".tb_roleaccesses "
				+ "(racc_id, racc_rol_ref, racc_sobj_ref, racc_word, racc_access_level, racc_subtractor) "
				+ "values (" + MavadConverter.AllURLsRoleAccessIdx + ","
				+ MavadConverter.LICRoleIdx + ", "
				+ MavadConverter.AllURLsSOBJIdx + ", 'V', 1, 'F')";

		insertNrmRoleStmt = connection
				.prepareStatement(insertAllUrlsRoleAccessQuery);
		insertNrmRoleStmt.execute();
		connectionFactory.closeStatement(insertNrmRoleStmt);

	}
}
