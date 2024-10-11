package com.wesley.craft;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.wesley.craft.model.SqlDataset;
import com.wesley.craft.model.SqlDatasetField;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DefaultSqlLight
 *
 * @author WangPanYong
 * @since 2024/09/29
 */
public class DefaultSqlLight implements SqlLight {
    private Map<Integer, List<SqlDataset>> datasetMap = new HashMap<>();

    public void loadConfig(Connection conn) {
        try {
            List<Entity> entityList = SqlExecutor.query(conn, "select * from " + SqlDataset.TABLE_NAME + " order by group_id,sort", new EntityListHandler());

            // 加载数据集字段
            List<Entity> fieldEntityList = SqlExecutor.query(conn, "select * from " + SqlDatasetField.TABLE_NAME, new EntityListHandler());
            Map<Long, List<SqlDatasetField>> fieldMap = fieldEntityList.stream().map(entity -> entity.toBean(SqlDatasetField.class))
                                                                       .collect(Collectors.groupingBy(SqlDatasetField::getDatasetId));


            // 根据group_id分组
            datasetMap = entityList.stream().map(entity -> {
                                       SqlDataset dataset = entity.toBean(SqlDataset.class);
                                       dataset.setFields(fieldMap.getOrDefault(dataset.getId(), Collections.emptyList()));
                                       return dataset;
                                   })
                                   .collect(Collectors.groupingBy(SqlDataset::getGroupId));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void query(Connection conn, int groupId, String mainDsName, Object primaryValue) {
        List<SqlDataset> sqlDatasets = datasetMap.get(groupId);
        if (sqlDatasets == null || sqlDatasets.isEmpty()) {
            // todo 抛出异常
            return;
        }

        // 主数据集
        SqlDataset mainDataset = sqlDatasets.stream().filter(e -> e.getName().equals(mainDsName)).findFirst().orElse(sqlDatasets.get(0));
        Set<String> dsNames = sqlDatasets.stream().map(SqlDataset::getName).collect(Collectors.toSet());
        List<SqlDataset> untidyDataset = new ArrayList<>();
        for (SqlDataset sqlDataset : sqlDatasets) {
            String dependOn = sqlDataset.getDependOn();
            // 依赖数据集必须存在 && 不允许依赖自身
            if (mainDataset != sqlDataset && dsNames.contains(dependOn) && !sqlDataset.getName().equals(dependOn)) {
                if (mainDataset.getName().equals(dependOn)) {
                    mainDataset.addSubDataset(sqlDataset);
                }else {
                    untidyDataset.add(sqlDataset);
                }
            }
        }
        buildSubDataset(mainDataset, untidyDataset);

        // TODO: 2024/10/11 处理返回值
        query(conn, mainDataset, primaryValue);

    }

    private void buildSubDataset(SqlDataset parentDataset, List<SqlDataset> sqlDatasets) {
        List<SqlDataset> subDatasets = parentDataset.getSubDatasets();
        if (CollUtil.isEmpty(subDatasets) || CollUtil.isEmpty(sqlDatasets)) {
            return;
        }

        for (SqlDataset subDataset : subDatasets) {
            for (SqlDataset sqlDataset : sqlDatasets) {
                if (subDataset != sqlDataset) {
                    if (subDataset.getName().equals(sqlDataset.getDependOn())) {
                        subDataset.addSubDataset(sqlDataset);
                    }
                }
            }
        }
        for (SqlDataset subDataset : subDatasets) {
            buildSubDataset(subDataset, sqlDatasets);
        }
    }

    protected void query(Connection coon, SqlDataset parentDataset, Object primaryValue) {
        String mainSql = parentDataset.getMainSql();
        try {
            List<Entity> dsList = SqlExecutor.query(coon, mainSql, new EntityListHandler(), primaryValue);
            System.out.println(dsList.size());

            List<SqlDataset> subDatasets = parentDataset.getSubDatasets();
            if (CollUtil.isEmpty(subDatasets) || CollUtil.isEmpty(dsList)) {
                return;
            }
            Entity entity = dsList.get(0);
            for (SqlDataset subDataset : subDatasets) {
                Object dependValue = entity.get(subDataset.getDependKey());
                if (Objects.nonNull(dependValue)){
                    query(coon, subDataset, dependValue);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
