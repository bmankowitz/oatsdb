package edu.yu.oatsdb.base;

/** Extends the "Configurable DB Management System" interface with an API that
 * allows for configuration of a persistent DBMS.
 * 
 * Design note: In a production system these APIs would be available only to
 * administrators.

 * @author Avraham Leff
 */

import java.util.*;

public interface ConfigurablePersistentDBMS extends ConfigurableDBMS {

  /** Returns the disk usage in MB of this DBMS instance.
   *
   * @return disk usage in MB for this DBMS
   */
  double getDiskUsageInMB();

  /** Delete all files and directories associated with this DBMS instance from
   * both disk and from main-memory.  Effectively resets the database.
   *
   * IMPORTANT: the effects of this API on existing transactions is
   * undefined.  This method should be invoked only when the system is
   * quiescent (i.e., outside a transaction).
   */
  void clear();
}

