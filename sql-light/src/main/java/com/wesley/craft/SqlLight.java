package com.wesley.craft;

import java.sql.Connection;

/**
 * SqlLight
 *
 * @author WangPanYong
 * @since 2024/09/29
 */
public interface SqlLight {

    /**
     * query
     *
     * @param conn 数据库连接
     * @param groupId 分组Id
     * @param mainDsName 主数据集名称
     * @param primaryValue 主键值
     * @author WesleyWong
     * @since 2024/10/11
     **/
    void query(Connection conn, int groupId, String mainDsName, Object primaryValue);
}
