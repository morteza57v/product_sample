package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.MavadConverter;


public class CitadelDomSystem {
	private String targetSchema;
	private Connection connection;

	public String getTargetSchema() {
		return targetSchema;
	}

	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public CitadelDomSystem(String targetSchema, Connection connection) {
		this.targetSchema = targetSchema;
		this.connection = connection;
	}

	public void run() throws SQLException, ClassNotFoundException {

		String insertQuery = "insert into " + targetSchema + ".tb_domsystems "
				+ "(doms_id, doms_dom_ref, doms_sys_ref, doms_startdate) " + "values (?, ?, ?, sysdate)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();

		PreparedStatement stmt = connection.prepareStatement(insertQuery);
		stmt.setLong(1, MavadConverter.domSystemIdx);
		stmt.setLong(2,6584 );
		stmt.setLong(3, MavadConverter.systemIdx);
		stmt.executeQuery();
		connectionFactory.closeStatement(stmt);

	}

}
