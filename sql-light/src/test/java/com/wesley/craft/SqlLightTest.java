package com.wesley.craft;

import com.wesley.craft.util.DbUtil;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SqlLightTest
 *
 * @author WangPanYong
 * @since 2024/09/29
 */
class SqlLightTest {

    String url = "jdbc:mysql://192.168.2.219:3306/lims042?useOldAliasMetadataBehavior=true&useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2b8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=false";
    String driverClassName = "com.mysql.cj.jdbc.Driver";
    String userName = "app";
    String password = "rrl_9YfC5qH(";

    @org.junit.jupiter.api.Test
    void query() {
        DefaultSqlLight sqlLight = new DefaultSqlLight();

        Connection connection = DbUtil.openSQLConnection(url, driverClassName, userName, password);
        sqlLight.loadConfig(connection);
        sqlLight.query(connection, 0, "test", "10000");
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}