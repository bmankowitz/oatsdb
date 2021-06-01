package edu.yu.oatsdb.base;

/** This (Runtime) exception is thrown to inform clients that their transaction
 * has been rolled back unilaterally by the DBMS e.g., when the client thread
 * has timed out trying to get resource locks.  Upon receiving this exception,
 * the client knows that the tx will have to restart from the beginning because
 * any locks that may have been acquired by the the client have been released
 * by the DBMS.  Also, the tx is marked as bring rolled back and is no longer
 * active.
 * 
 * @author Avraham Leff
 */

public class ClientTxRolledBackException extends RuntimeException {
  public ClientTxRolledBackException(String msg) {
    super(msg);
  }

  public ClientTxRolledBackException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
  
}
