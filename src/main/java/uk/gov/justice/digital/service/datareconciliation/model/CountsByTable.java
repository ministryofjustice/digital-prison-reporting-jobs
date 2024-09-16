package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@EqualsAndHashCode
@ToString
public class CountsByTable<V> {

    private final Map<String, V> underlyingMap = new HashMap<>();

    public void put(String fullTableName, V tableCount) {
        underlyingMap.put(fullTableName, tableCount);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public V get(String fullTableName) {
        return underlyingMap.get(fullTableName);
    }

    public Set<Map.Entry<String, V>> entrySet() {
        return underlyingMap.entrySet();
    }

    public Set<String> keySet() {
        return underlyingMap.keySet();
    }
}
