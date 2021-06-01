package edu.yu.oatsdb.base;

/** Defines set of "completion" states that a transaction may have ended up in.
 * Unlike the TxStatus (which defines a set of states that a tx may be in at a
 * given moment), this enum focuses on success/failure states that a tx
 * ultimately gets to.
 * 
 * @see TxStatus 
 * 
 * @author Avraham Leff
 */

public enum TxCompletionStatus {
  NOT_COMPLETED, /* Hasn't completed yet */
  COMMITTED, /* Completed successfully */
  ROLLEDBACK /* Completed unsuccessfully */
  ;          // needed because methods will follow

  /** Implements state-transitions for a given TxCompletionStatus, returning an
   * "updated" enum value that reflects the change implied by the associated
   * transaction's new TxStatus value.  This method does not change the value
   * of the currentStatus parameter: it returns a new TxCompletionStatus value.
   *
   * NOTE: Does not do validity checking on the current TxCompletionStatus
   * relative to the incoming TxStatus
   *
   * @param currentStatus current TxCompletionStatus value
   * @param txStatus current TxStatus value
   */
  public static TxCompletionStatus updateTxStatus
  (final TxCompletionStatus currentStatus, final TxStatus txStatus)
  {
    final TxCompletionStatus newStatus =  txStatusEquivalent(txStatus);
    TxCompletionStatus retval;

    // no corresponding value to TxStatus: leave unchanged
    if (newStatus == null) { 
      retval = currentStatus;   // do nothing
    }
    else {
      retval = newStatus;
    }

    return retval;
  }

  /** Note: may return null depending on whether the TxStatus state is
   * meaningful to the TxCompletionStatus lifecycle.
   */
  private static TxCompletionStatus
  txStatusEquivalent(final TxStatus txStatus)
  {
    TxCompletionStatus retval;  // otherwise "break is unreachable"
    switch(txStatus) {
    case UNKNOWN:
      retval = null;
      break;
    case ACTIVE:
      retval = NOT_COMPLETED;
      break;
    case COMMITTED:
      retval = COMMITTED;
      break;
    case COMMITTING:
      retval = null;
      break;
    case ROLLEDBACK:
      retval = ROLLEDBACK;
      break;
    case ROLLING_BACK:
      retval = null;
      break;
    case NO_TRANSACTION:
      retval = null;
      break;
    default:
      retval = null;
      break;
    }

    return retval;
  } // fromTxStatus

}
