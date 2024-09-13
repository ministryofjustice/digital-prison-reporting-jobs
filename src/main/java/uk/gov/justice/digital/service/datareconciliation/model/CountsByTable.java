package uk.gov.justice.digital.service.datareconciliation.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountsByTable<V> {

    private final Map<String, V> countsByTable = new HashMap<>();

    public void put(String fullTableName, V tableCount) {
        countsByTable.put(fullTableName, tableCount);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public V get(String fullTableName) {
        return countsByTable.get(fullTableName);
    }

    public Set<Map.Entry<String, V>> entrySet() {
        return countsByTable.entrySet();
    }

    public Set<String> keySet() {
        return countsByTable.keySet();
    }
}
