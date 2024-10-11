package com.wesley.craft.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * SqlDataset
 *
 * @author WangPanYong
 * @since 2024/09/29
 */
@Data
@EqualsAndHashCode(of = {"groupId","name"})
public class SqlDataset {
    public static final String TABLE_NAME = "sql_dataset";

    private Long id;
    private Integer groupId;
    private String name;
    private String mainSql;
    private String primaryKey;
    private String remark;

    private String dependOn;
    private String dependKey;

    private List<SqlDatasetField> fields;

    private List<SqlDataset> subDatasets;

    public void addSubDataset(SqlDataset subDataset) {
        if (subDatasets == null) {
            subDatasets = new ArrayList<>();
        }
        subDatasets.add(subDataset);
    }

}
