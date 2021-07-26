package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;

public enum TxMgrImpl implements TxMgr {
    Instance;
    private final static Logger logger = LogManager.getLogger(TxMgrImpl.class);

    public synchronized void begin() throws NotSupportedException, SystemException {
        Thread currentThread = Thread.currentThread();
        //logger.info("Starting begin from {}", currentThread);
        if(Globals.threadTxMap.containsKey(currentThread)){
            throw new NotSupportedException("This thread ( "+currentThread.getName()+" ) " +
                    "is already in a different transaction. No nested transactions!");
        }
        TxImpl newTx = new TxImpl(currentThread);
        Globals.threadTxMap.put(currentThread, newTx);

        newTx.setStatus(TxStatus.ACTIVE);
        Globals.threadTxMap.put(currentThread, newTx);

    }

    public synchronized void commit() throws RollbackException, IllegalStateException, SystemException {
        //ensure name is valid
        Thread currentThread = Thread.currentThread();
        TxImpl currentTx = Globals.threadTxMap.get(currentThread);

        //logger.info("Starting commit {}, id= {}", currentThread, currentThread.getId());
        if(!Globals.threadTxMap.containsKey(currentThread)) {
            throw new IllegalStateException("This thread is not in a transaction");
        }
        //set tx status
        currentTx.setStatus(TxStatus.COMMITTING);
        Globals.threadTxMap.replace(currentThread, currentTx);

        //commit
        Globals.commitThreadTables(currentThread);

        //after successfully transacting the transaction, allow this thread to add a new tx
        currentTx = Globals.threadTxMap.get(currentThread);
        currentTx.setStatus(TxStatus.COMMITTED);
        Globals.threadTxMap.replace(currentThread, currentTx);
        Globals.threadTxMap.remove(currentThread);
    }

    public synchronized void rollback() throws IllegalStateException, SystemException {
        Thread currentThread = Thread.currentThread();
        TxImpl tx = Globals.threadTxMap.get(currentThread);

        //logger.info("Starting rollback");

        if(!Globals.threadTxMap.containsKey(currentThread)) {
            //if the thread is not already in the thread table
            throw new IllegalStateException("This thread is not in a transaction");
        }
        tx.setStatus(TxStatus.ROLLING_BACK);
        Globals.threadTxMap.replace(currentThread, tx);

        Globals.revertThreadTables(currentThread);

        //after successfully transacting the transaction, allow this thread to add a new tx
        tx.setStatus(TxStatus.ROLLEDBACK);
        Globals.threadTxMap.replace(currentThread, tx);
        Globals.threadTxMap.remove(currentThread);
    }

    public Tx getTx() throws SystemException {
        if(Globals.threadTxMap.get(Thread.currentThread()) == null){
            TxImpl fakeTx = new TxImpl(null );
            fakeTx.setStatus(TxStatus.NO_TRANSACTION);
            return fakeTx;
        }
        return Globals.threadTxMap.get(Thread.currentThread());
    }

    public TxStatus getStatus() throws SystemException {
        if(Globals.threadTxMap.get(Thread.currentThread()) == null ||
                Globals.threadTxMap.get(Thread.currentThread()).getStatus() == TxStatus.COMMITTED ||
                Globals.threadTxMap.get(Thread.currentThread()).getStatus() == TxStatus.ROLLEDBACK)
            return TxStatus.NO_TRANSACTION;
        else
            return Globals.threadTxMap.get(Thread.currentThread()).getStatus();

    }
}
