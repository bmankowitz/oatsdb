package edu.yu.oatsdb.v2;


import edu.yu.oatsdb.base.SystemException;
import edu.yu.oatsdb.base.Tx;
import edu.yu.oatsdb.base.TxCompletionStatus;
import edu.yu.oatsdb.base.TxStatus;

import java.io.Serializable;

public class TxImpl implements Tx, Serializable {
    final Thread txThread;
    TxStatus status = TxStatus.NO_TRANSACTION; //default value
    final int id;
    //A transaction is a series of DB commands.
    public TxImpl(Thread thread, int id){
        setStatus(TxStatus.NO_TRANSACTION);
        txThread = thread;
        this.id = id;
    }



    public TxStatus getStatus() throws SystemException{
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
