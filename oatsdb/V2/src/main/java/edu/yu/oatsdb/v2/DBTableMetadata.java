package edu.yu.oatsdb.v2;

import java.io.Serializable;
import java.util.Map;

public class DBTableMetadata<K, V> implements Serializable {
    String tableName;
    Class<K> keyClass;
    Class<V> valueClass;
    Map<K, V> map;
    public DBTableMetadata(String tableName, Class<K> keyClass, Class<V> valueClass, Map<K, V> map) {
        this.tableName = tableName;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.map = map;
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Name: ").append(tableName).append("\n");
        sb.append("keyClass: ").append(keyClass).append("\n");
        sb.append("valueClass: ").append(valueClass).append("\n");
        sb.append("Map: ").append(map).append("\n");
        return sb.toString();
    }
}
