package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.MavadConverter;


public class CitadelSystem {
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

	public CitadelSystem(String targetSchema, Connection connection) {
		this.targetSchema = targetSchema;
		this.connection = connection;
	}

	public void run() throws SQLException, ClassNotFoundException {

		String insertQuery = "insert into " + targetSchema + ".tb_systems "
				+ "(sys_id, sys_name, sys_code, sys_deployable, sys_path,SYS_PUBLIC_RESOURCES)" + "values (?, ?, ?, ?, ?, ?)";

		ConnectionFactory connectionFactory = ConnectionFactory.getInstance();

		PreparedStatement stmt = connection.prepareStatement(insertQuery);
		stmt.setLong(1, MavadConverter.systemIdx);
		stmt.setString(2, "سيستم گواهينامه");
		stmt.setString(3, "WebModule");
		stmt.setString(4, "T");
		stmt.setString(5, "/");
		stmt.setString(6, "/scripts/*;/images/*;/xml/*;/pages/login/loginPage.jsp;/pages/login/error.jsp;/pages/images/*");
		stmt.executeQuery();
		connectionFactory.closeStatement(stmt);

	}

}
