package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;
import java.util.HashMap;
import java.util.HashSet;

public enum TxMgrImpl implements TxMgr {
    Instance;

    public synchronized void begin() throws NotSupportedException, SystemException {
        //incoming thread: TODO: FIX THIS FOR MULTITHREADING
        Thread incoming = Thread.currentThread();
        if(Globals.threadTxMap.containsKey(incoming)){
            throw new NotSupportedException("This thread ( "+incoming.getName()+" ) " +
                    "is already in a different transaction. No nested transactions!");
        }
        TxImpl newTx = new TxImpl();
        newTx.setStatus(TxStatus.NO_TRANSACTION);
        Globals.threadTxMap.put(incoming, newTx);

        newTx.setThread(incoming);
        newTx.setStatus(TxStatus.ACTIVE);
        Globals.threadTxMap.put(incoming, newTx);

        //TODO: This is a hack for v0. Must change this to be table specific; not every Tx needs every table
        Globals.startAll();

    }

    public synchronized void commit() throws RollbackException, IllegalStateException, SystemException {
        //ensure name is valid
        Thread t = Thread.currentThread();//TODO: this will eventually be declared elsewhere for parallelization
        TxImpl tx = Globals.threadTxMap.get(t);

        if(!Globals.threadTxMap.containsKey(t)) {
            throw new IllegalStateException("This thread is not in a transaction");
        }
        tx.setStatus(TxStatus.COMMITTING);
        Globals.threadTxMap.replace(t, tx);

        //after successfully transacting the transaction, allow this thread to add a new tx
        Globals.commitAll();

        tx = Globals.threadTxMap.get(t);
        tx.setStatus(TxStatus.COMMITTED);
        Globals.threadTxMap.replace(t, tx);
        Globals.threadTxMap.remove(t);
    }

    public synchronized void rollback() throws IllegalStateException, SystemException {
        Thread t = Thread.currentThread();//TODO: this will eventually be declared elsewhere for parallelization
        //NEW:
        TxImpl tx = Globals.threadTxMap.get(t);

        if(!Globals.threadTxMap.containsKey(t)) {
            //if the thread is not already in the thread table
            throw new IllegalStateException("This thread is not in a transaction");
        }
        tx.setStatus(TxStatus.ROLLING_BACK);
        Globals.threadTxMap.replace(t, tx);
        //after successfully transacting the transaction, allow this thread to add a new tx
        Globals.revertAll();
        tx = Globals.threadTxMap.get(t);
        tx.setStatus(TxStatus.ROLLEDBACK);
        Globals.threadTxMap.replace(t, tx);
        Globals.threadTxMap.remove(t);
    }

    public Tx getTx() throws SystemException {
        if(Globals.threadTxMap.get(Thread.currentThread()) == null){
            TxImpl fakeTx = new TxImpl();
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
