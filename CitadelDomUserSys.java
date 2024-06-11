package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.MavadConverter;
;

public class CitadelDomUserSys {

	private String targetSchema;
	private HashMap<Long, Long> domUserMap = new HashMap<Long, Long>();
	private IdGenerator idGenerator;
	private Connection connection;
//	private Connection connectiontarget;

	public CitadelDomUserSys(String targetSchema, HashMap<Long, Long> domUserMap, IdGenerator idGenerator,
			Connection connection) {
		this.targetSchema = targetSchema;
		this.domUserMap = domUserMap;
		this.idGenerator = idGenerator;
		this.connection = connection;
		//this.connectiontarget = connectiontarget;
	}

	public void run() throws SQLException, ClassNotFoundException {
		String insertQuery = "insert into " + targetSchema + ".tb_domusersys "
				+ "(dmus_id, dmus_domu_ref, dmus_doms_ref, dmus_logon_machines, dmus_logon_hours) "
				+ "values (?, ?, ?, ?, ?)";
		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();

		for (Long userSeq : domUserMap.keySet()) {
			Long domUserId = domUserMap.get(userSeq);

			PreparedStatement statement2 = connection.prepareStatement(insertQuery);
			long newId = idGenerator.getNextId();
			statement2.setLong(1, newId);
			statement2.setLong(2, domUserId);
			statement2.setLong(3, MavadConverter.domSystemIdx);
			statement2.setString(4, MavadConverter.MACHINES);
			statement2.setString(5, MavadConverter.HOURS);
			statement2.executeQuery();
			connectionFactory.closeStatement(statement2);
		}

	}
}
