package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import net.navoshgaran.mavad.ConnectionFactory;
import net.navoshgaran.mavad.IdGenerator;

public class CitadelUserRole {

    private String targetSchema;
    private String sourceSchema;
    private HashMap<Long, Long> domuserMap;
    private HashMap<Long, Long> roleMap;
    private IdGenerator idGenerator;
    private Connection connection;
    private Connection connectiontarget;
    

    public CitadelUserRole(String targetSchema, String sourceSchema, HashMap<Long, Long> domuserMap,
                    HashMap<Long, Long> roleMap, IdGenerator idGenerator, Connection connection , Connection connectiontarget) {
        this.targetSchema = targetSchema;
        this.sourceSchema = sourceSchema;
        this.roleMap = roleMap;
        this.domuserMap = domuserMap;
        this.idGenerator = idGenerator;
        this.connection = connection;
        this.connectiontarget = connectiontarget;
    }

    public void run() throws SQLException, ClassNotFoundException {

        String selectQuery = "select * from " + sourceSchema + ".TBL_USER_GROUP";

        String insertQuery =
                "insert into " + targetSchema + ".tb_userroles " +
                        "(urol_id, urol_domu_ref, urol_rol_ref, urol_isactive, urol_startdate, urol_fixed) " +
                        "values (?, ?, ?, ?, sysdate, ?)";

        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        PreparedStatement selectStmt = connectiontarget.prepareStatement(selectQuery);
        ResultSet resultSet = selectStmt.executeQuery();

        while (resultSet.next()) {

            long newId = idGenerator.getNextId();

            Long domUserId = domuserMap.get(resultSet.getLong("fk_user_id"));
            if(domUserId == null)
                continue;

            PreparedStatement insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setLong(1, newId);

            insertStmt.setLong(2, domUserId);
            insertStmt.setLong(3, roleMap.get(resultSet.getLong("fk_group_id")));
            insertStmt.setString(4, "T");
            insertStmt.setString(5, "F");
           // System.out.println(resultSet.getLong("fk_user_id") +"-->"+  resultSet.getLong("fk_group_id"));
            insertStmt.executeQuery();
            connectionFactory.closeStatement(insertStmt);
        }

        connectionFactory.closeResultSet(resultSet);
        connectionFactory.closeStatement(selectStmt);
    }

}
