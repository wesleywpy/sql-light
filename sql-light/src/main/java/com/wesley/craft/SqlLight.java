package com.wesley.craft;

import com.wesley.craft.model.SqlDatasetResult;

import java.sql.Connection;
import java.util.Map;

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
    Map<String,SqlDatasetResult> query(Connection conn, int groupId, String mainDsName, Object primaryValue);

    Map<String,SqlDatasetResult> query(Connection conn, int groupId, String mainDsName, Object primaryValue, Map<String,Object> params);

}
