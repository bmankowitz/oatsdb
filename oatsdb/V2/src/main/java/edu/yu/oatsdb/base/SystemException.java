package edu.yu.oatsdb.base;

/** SystemException is thrown by the transaction manager to indicate that it
 * has encountered an unexpected error condition that prevents future
 * transaction services from proceeding.
 *
 * Design note: API loosely modeled on javax.transaction APIs
 * https://docs.oracle.com/javaee/5/api/javax/transaction/package-summary.html
 * 
 * @author Avraham Leff
 */

public class SystemException extends Exception {
  public SystemException(String msg) {
    super(msg);
  }
  public SystemException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
}
