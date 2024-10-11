package com.wesley.craft.model;

import lombok.Data;

/**
 * SqlDatasetField
 *
 * @author WangPanYong
 * @since 2024/10/08
 */
@Data
public class SqlDatasetField {
    public static final String TABLE_NAME = "sql_dataset_field";

    private Long id;
    private Long datasetId;

    private String fieldName;
}
