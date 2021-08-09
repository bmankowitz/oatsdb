package edu.yu.oatsdb.base;

/** The TxMgr interface defines the methods that allow the DBMS to manage
 * transaction boundaries.
 *
 * Design note: API loosely modeled on javax.transaction APIs
 * https://docs.oracle.com/javaee/5/api/javax/transaction/package-summary.html
 * 
 * @author Avraham Leff
 */

public interface TxMgr {

  /** Create a new transaction and associate it with the current thread.
   *
   * @throws NotSupportedException Thrown if the thread is already associated
   * with a transaction and the Transaction Manager implementation does not
   * support nested transactions.
   * @throws SystemException Thrown if the transaction manager encounters an
   * unexpected error condition.
   */
  void begin() throws NotSupportedException, SystemException;
      
  /** Complete the transaction associated with the current thread. When this
   * method completes, the thread is no longer associated with a transaction.
   *
   * @throws RollbackException Thrown to indicate that the transaction has been
   * rolled back rather than committed.
   * @throws IllegalStateException Thrown if the current thread is not
   * associated with a transaction.
   * @throws SystemException Thrown if the transaction manager encounters an
   * unexpected error condition.
   */
  void commit() throws RollbackException, IllegalStateException, SystemException;

  /** Roll back the transaction associated with the current thread. When this
   * method completes, the thread is no longer associated with a transaction.
   *
   * @throws IllegalStateException Thrown if the current thread is not
   * associated with a transaction.
   * @throws SystemException Thrown if the transaction manager encounters an
   * unexpected error condition.
   */
  void rollback() throws IllegalStateException, SystemException;

  /** Return the transaction object that represents the transaction context of the
   * calling thread.  If no transaction is associated with the current thread,
   * returns a Transaction whose getStatus() equals TxStatus.NO_TRANSACTION.
   *
   * @throws SystemException Thrown if the transaction manager encounters an
   * unexpected error condition.
   */
  Tx getTx() throws SystemException;

  /** Obtain the status of the transaction associated with the current thread.
   *
   * @returns The transaction status. If no transaction is associated with the
   * current thread, this method returns the TxStatus.NO_TRANSACTION value
   * @throws SystemException Thrown if the transaction manager encounters an
   * unexpected error condition.
  */
  TxStatus getStatus() throws SystemException;

}
