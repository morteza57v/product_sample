package net.navoshgaran.mavad.cnv;

import java.util.HashMap;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;
import net.navoshgaran.mavad.MavadConverter;

public class CitadelRole {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<Long, Long> map = new HashMap<Long, Long>();
    private IdGenerator idGenerator;
    private Connection connection;
    private Connection connectiontarget;
    


    public CitadelRole(String targetSchema, String sourceSchema, IdGenerator idGenerator,Connection connection,Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select * from " + sourceSchema + ".tbl_group ";

        String insertQuery =
                "insert into " + targetSchema + ".tb_roles " +
                        "(rol_id, rol_sys_ref, rol_codename, rol_desc, rol_isbase, rol_fixed, " +
                        "rol_type, rol_name, rol_hours, rol_subtractor) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'F')";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connectiontarget.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        //TODO: check shavad ke rol ba codename yeksan dar system mojood nabashad
//        "select * from tb_roles aa, tb_roles bb where aa.ROL_CODENAME = bb.rol_codename and aa.rol_id <> bb.rol_id";
        while (resultSet.next()) {

            long id = resultSet.getLong("pk_group_id");
            long newId = idGenerator.getNextId();
            map.put(id, newId);
            PreparedStatement insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setLong(1, newId);
            insertStmt.setLong(2, MavadConverter.systemIdx);
            insertStmt.setString(3, "mng" + resultSet.getString("pk_group_id"));
            insertStmt.setString(4, resultSet.getString("description"));
            insertStmt.setString(5, "T");
            insertStmt.setString(6, "F");
            insertStmt.setString(7, "N");
            insertStmt.setString(8, resultSet.getString("group_name"));
            insertStmt.setString(9, MavadConverter.HOURS);
            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);

    }

    public HashMap<Long, Long> getMap() {
        return map;
    }


}
