package com.wesley.craft;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.wesley.craft.model.SqlDataset;
import com.wesley.craft.model.SqlDatasetField;
import com.wesley.craft.model.SqlDatasetResult;
import com.wesley.craft.support.SqlNamedParam;

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

    @Override
    public Map<String,SqlDatasetResult> query(Connection conn, int groupId, String mainDsName, Object primaryValue) {
        return this.query(conn, groupId, mainDsName, primaryValue, new HashMap<>());
    }

    @Override
    public Map<String, SqlDatasetResult> query(Connection conn, int groupId, String mainDsName, Object primaryValue, Map<String, Object> params) {
        List<SqlDataset> sqlDatasets = datasetMap.get(groupId);
        if (sqlDatasets == null || sqlDatasets.isEmpty()) {
            throw new IllegalArgumentException("groupId 未找到数据集");
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
        DatasetQueryParam queryParam = new DatasetQueryParam(conn, mainDataset, primaryValue);
        queryParam.paramMap = params;
        Map<String,SqlDatasetResult> resultMap = new HashMap<>();
        this.query(queryParam, resultMap);
        return resultMap;
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

    protected void query(DatasetQueryParam queryParam, Map<String, SqlDatasetResult> resultMap) {
        SqlDataset parentDataset = queryParam.parentDataset;
        String mainSql = queryParam.getSql();
        try {
            List<Entity> dsList = SqlExecutor.query(queryParam.coon, mainSql, new EntityListHandler(), queryParam.paramList.toArray());
            System.out.println(dsList.size());
            SqlDatasetResult result = new SqlDatasetResult();
            result.setName(parentDataset.getName());
            if (CollUtil.isNotEmpty(dsList)) {
                // TODO: 2024/10/15 字段配置参数处理
                List<Map<String, Object>> values = new ArrayList<>(dsList.size());
                values.addAll(dsList);
                result.setValues(values);
                resultMap.put(parentDataset.getName(), result);
            }

            List<SqlDataset> subDatasets = parentDataset.getSubDatasets();
            if (CollUtil.isEmpty(subDatasets) || CollUtil.isEmpty(dsList)) {
                return;
            }
            Entity entity = dsList.get(0);
            for (SqlDataset subDataset : subDatasets) {
                List<Object> dependValues = dsList.stream().map(e -> e.get(subDataset.getDependKey()))
                                                  .filter(Objects::nonNull).distinct().toList();
                if (!dependValues.isEmpty()){
                    DatasetQueryParam subParam = new DatasetQueryParam(queryParam.coon, subDataset, dependValues.get(0));
                    subParam.paramMap = queryParam.paramMap;
                    query(subParam, resultMap);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DatasetQueryParam {
        final Connection coon;
        final SqlDataset parentDataset;
        final List<Object> paramList = new ArrayList<>();
        Map<String, Object> paramMap;


        public DatasetQueryParam(Connection coon, SqlDataset parentDataset, Object primaryValue) {
            this.coon = coon;
            this.parentDataset = parentDataset;
            this.paramList.add(primaryValue);
        }

        String getSql() {
            String mainSql = parentDataset.getMainSql();
            if (Objects.isNull(paramMap) || paramMap.isEmpty()) {
                return mainSql;
            }
            // TODO: 2024/10/16 处理paramList多个值情况
            final SqlNamedParam namedSql = new SqlNamedParam(mainSql, paramMap);
            return namedSql.getSql();
        }
    }
}
