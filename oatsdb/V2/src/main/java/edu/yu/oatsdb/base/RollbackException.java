package edu.yu.oatsdb.base;

/** RollbackException thrown when a client's transactional operation
 * (implicitly) references a transaction that has been marked for rollback only
 * or the transaction has been rolled back instead of committed.
 *
 * Design note: API loosely modeled on javax.transaction APIs
 * https://docs.oracle.com/javaee/5/api/javax/transaction/package-summary.html
 * 
 * @author Avraham Leff
 */

public class RollbackException extends Exception {
  public RollbackException(String msg) {
    super(msg);
  }
}
