package org.ptm.kvservice.db.models;

import com.google.gson.JsonPrimitive;
import java.util.List;

public class KeyValuesObj {

    private String key;
    private List<JsonPrimitive> values;
    private String type;

    public KeyValuesObj(String key, List<JsonPrimitive> values, String type) {
        this.key = key;
        this.values = values;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<JsonPrimitive> getValues() {
        return values;
    }

    public void setValues(List<JsonPrimitive> values) {
        this.values = values;
    }


}
