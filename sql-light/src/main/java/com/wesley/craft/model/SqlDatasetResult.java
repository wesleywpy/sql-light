package com.wesley.craft.model;

import lombok.Data;

import java.util.*;

/**
 * SqlDatasetResult
 *
 * @author WangPanYong
 * @since 2024/10/15
 */
@Data
public class SqlDatasetResult {
    private String name;

    private List<Map<String, Object>> values;

    public void addValue(Map<String, Object> value) {
        if (this.values == null) {
            this.values = new ArrayList<>();
        }
        this.values.add(value);
    }

    public Map<String, Object> findFirst() {
        return Objects.isNull(values) || values.isEmpty() ? new HashMap<>() : values.get(0);
    }


}
