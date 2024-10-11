package com.wesley.craft.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * DbUtil
 *
 * @author WangPanYong
 * @since 2024/09/29
 */
public class DbUtil {

    /**
     * 构造SQL Connection
     *
     * @param url
     * @param jdbcDriver
     * @param userName
     * @param password
     * @return Connection
     */
    public static Connection openSQLConnection(String url, String jdbcDriver, String userName, String password) {
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1.getMessage());
        }

        try {
            return DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(url + " is invalidate");
        }
    }

}
