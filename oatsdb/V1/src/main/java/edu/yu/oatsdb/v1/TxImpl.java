package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;

import java.io.Serializable;

public class TxImpl implements Tx, Serializable {
    Thread txThread;
    TxStatus status = TxStatus.NO_TRANSACTION; //default value
    //A transaction is a series of DB commands.
    public TxImpl(Thread thread){
        setStatus(TxStatus.NO_TRANSACTION);
        txThread = thread;
    }


    public TxStatus getStatus() throws SystemException {
        return status;
    }

    public TxCompletionStatus getCompletionStatus() {
        switch (status){
            case COMMITTED:
                return TxCompletionStatus.COMMITTED;
            case ROLLEDBACK:
                return TxCompletionStatus.ROLLEDBACK;
            case ACTIVE:
            case COMMITTING:
            case ROLLING_BACK:
                return TxCompletionStatus.NOT_COMPLETED;
            default:
                //noTx or UNKNOWN
                return null;
        }
    }

    protected void setStatus(TxStatus status){
        this.status = status;
    }

}
