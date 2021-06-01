package edu.yu.oatsdb.base;

/** Interface for DB Management System. 
 *
 * Specifies a client model that interacts with "named" Maps of a given key and
 * value class.
 * 
 * @author Avraham Leff
 */

import java.util.*;

public interface DBMS {

  /** Retrieves the named map.  The map is "typed" by its key and value classes
   * and this API provides a type-safe way for the map retrieval.
   *
   * Note: a map reference acquired by tx1 need not be reacquired during tx2.
   * That is, map references persist between transactions.  This semantics will
   * have to change if we allow Maps to be removed, but in the absence of such
   * an API there is no reason not to allow the reference to be reused.
   *
   * @param name name associated with the map, cannot be empty
   * @return the named map
   * @param keyClass the type of the map's keys, cannot be null
   * @param valueClass the type of the map's values, cannot be null
   * @see #createMap
   * @throws ClassCastException if specified key or value class doesn't match
   * the corresponding classes of the actual map
   * @throws NoSuchElementException if no map is associated with the specified
   * name
   * @throws ClientNotInTxException if client is not associated with a
   * transaction.
   */
  public <K, V> Map<K, V> getMap(final String name,
                                 Class<K> keyClass, Class<V> valueClass);

  /** Creates (and returns) a map, associates it with the specified name for
   * subsequent retrieval.  The map is parameterized by the type of the key
   * class and the type of the value class
   *
   * @param name names the map (for subsequent retrieval), cannot be empty
   * @param keyClass the type of the map's keys, cannot be null
   * @param valueClass the type of the map's values, cannot be null
   * @return a parameterized map of the specified key and value types
   * @throws IllegalArgumentException if name is already bound to another map
   * @throws ClientNotInTxException if client is not associated with a
   * transaction.
   */
  public <K, V> Map<K, V>
    createMap(String name, Class<K> keyClass, Class<V> valueClass);
}
