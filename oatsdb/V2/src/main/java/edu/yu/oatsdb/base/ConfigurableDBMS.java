package edu.yu.oatsdb.base;

/** Extends the "DB Management System" interface with an API that allows for
 * configuration of the DBMS.
 * 
 * @author Avraham Leff
 */

import java.util.*;

public interface ConfigurableDBMS extends DBMS {

  /** Sets the duration of the "transaction timeout".  A client whose
   * transaction's duration exceeds the DBMS's timeout will be automatically
   * rolled back by the DBMS.
   *
   * @param ms the timeout duration in ms, must be greater than 0
   */
  void setTxTimeoutInMillis(int ms);

  /** Returns the current DBMS transaction timeout duration.
   *
   * @return duration in milliseconds
   */
  int getTxTimeoutInMillis();
}

