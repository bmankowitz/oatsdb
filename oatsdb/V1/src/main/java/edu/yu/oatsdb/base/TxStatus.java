package edu.yu.oatsdb.base;

/** The TxStatus enum defines the set of transaction status codes.  
 *
 * Note: these values represent a "point in time" window into a transaction's
 * state, whose state may change in the next moment.
 *
 * Design note: API loosely modeled on javax.transaction APIs
 * https://docs.oracle.com/javaee/5/api/javax/transaction/package-summary.html
 * 
 * @author Avraham Leff
 */

public enum TxStatus {
  UNKNOWN, ACTIVE, COMMITTED, COMMITTING, ROLLEDBACK, ROLLING_BACK, NO_TRANSACTION
}
