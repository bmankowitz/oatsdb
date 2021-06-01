package edu.yu.oatsdb.base;

/** NotSupportedException is thrown to indicate that the client's request
 * cannot be executed because the operation is not a supported feature. For
 * example, because nested transactions are not supported, the Transaction
 * Manager throws this exception when a calling thread that is already
 * associated with a transaction attempts to start a new transaction. (A nested
 * transaction occurs when a thread is already associated with one transaction
 * and attempts to start a second transaction.)
 *
 * Design note: API loosely modeled on javax.transaction APIs
 * https://docs.oracle.com/javaee/5/api/javax/transaction/package-summary.html
 * 
 * @author Avraham Leff
 */

public class NotSupportedException extends Exception {
  public NotSupportedException(String msg) {
    super(msg);
  }
}
