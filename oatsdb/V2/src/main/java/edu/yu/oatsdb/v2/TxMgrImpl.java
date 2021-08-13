package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.NotSupportedException;
import edu.yu.oatsdb.base.RollbackException;
import edu.yu.oatsdb.base.SystemException;
import edu.yu.oatsdb.base.Tx;
import edu.yu.oatsdb.base.TxMgr;
import edu.yu.oatsdb.base.TxStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public enum TxMgrImpl implements TxMgr {
    Instance;
    private final static Logger logger = LogManager.getLogger(TxMgrImpl.class);

    public void begin() throws NotSupportedException, SystemException{

        if (Globals.log) logger.info("Starting begin from {}", Thread.currentThread());
        if(Globals.threadTxMap.containsKey(Thread.currentThread())){
            NotSupportedException e = new NotSupportedException("This thread ( "+Thread.currentThread().getName()+" ) " +
                    "is already in a different transaction. No nested transactions!");
            e.printStackTrace();
            throw e;
        }
        final TxImpl newTx = new TxImpl(Globals.txIdGenerator.getAndIncrement());
        Globals.threadTxMap.put(Thread.currentThread(), newTx);

        newTx.setStatus(TxStatus.ACTIVE);
        Globals.threadTxMap.put(Thread.currentThread(), newTx);

    }

    public void commit() throws RollbackException, IllegalStateException, SystemException {
        //ensure name is valid

        final TxImpl currentTx = Globals.threadTxMap.get(Thread.currentThread());

        if (Globals.log) logger.info("Starting commit");
        if(!Globals.threadTxMap.containsKey(Thread.currentThread())) {
            throw new IllegalStateException("This thread is not in a transaction");
        }
        if(Globals.threadTxMap.get(Thread.currentThread()).rollingBack()) throw new RollbackException("This thread" +
                "is not currently in a transaction" + Thread.currentThread());
        //set tx status
        currentTx.setStatus(TxStatus.COMMITTING);
        Globals.threadTxMap.replace(Thread.currentThread(), currentTx);

        //commit
        Globals.commitThreadTables();

        //after successfully transacting the transaction, allow this thread to add a new tx
        currentTx.setStatus(TxStatus.COMMITTED);
        Globals.threadTxMap.replace(Thread.currentThread(), currentTx);
        Globals.threadTxMap.remove(Thread.currentThread());
        if (Globals.log) logger.info("Finished commit {}, id= {}", Thread.currentThread(), Thread.currentThread().getId());
    }

    public void rollback() throws IllegalStateException, SystemException {

        final TxImpl tx = Globals.threadTxMap.get(Thread.currentThread());

        if (Globals.log) logger.info("Starting rollback");

        if(!Globals.threadTxMap.containsKey(Thread.currentThread())) {
            //if the thread is not already in the thread table
            throw new IllegalStateException("This thread is not in a transaction");
        }
        tx.setStatus(TxStatus.ROLLING_BACK);
        Globals.threadTxMap.replace(Thread.currentThread(), tx);

        Globals.revertThreadTables();

        //after successfully transacting the transaction, allow this thread to add a new tx
        tx.setStatus(TxStatus.ROLLEDBACK);
        Globals.threadTxMap.replace(Thread.currentThread(), tx);
        Globals.threadTxMap.remove(Thread.currentThread());
    }

    public Tx getTx() throws SystemException {
        if(Globals.threadTxMap.get(Thread.currentThread()) == null){
            final TxImpl fakeTx = new TxImpl(-1 );
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
