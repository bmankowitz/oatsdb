package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import java.util.Map;

import static org.junit.Assert.*;
class UnserializableObject{
    transient int i = 3;
}
public class DBMSImplTest {
    ConfigurablePersistentDBMS db;
    TxMgr txMgr;
    int i;

    @Before
    public void before() throws InstantiationException {
        db = (ConfigurablePersistentDBMS) OATSDBType.dbmsFactory(OATSDBType.V2);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V2);
    }
    @After
    public void after() throws InstantiationException {
        db.clear();
        db = null;
        txMgr = null;
    }


    @Test(expected = ClientNotInTxException.class)
    public void createOutsideTx() throws InstantiationException, SystemException, NotSupportedException {
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
    }

    @Test(expected = ClientNotInTxException.class)
    public void getOutsideTx() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.commit();
        db.getMap("grades",Character.class, String.class);
    }

    @Test(expected = ClientNotInTxException.class)
    public void createOutsideTxAfterRollback() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.rollback();
        db.createMap("grades",Character.class, String.class);
    }

    @Test(expected = ClientNotInTxException.class)
    public void getOutsideTxAfterRollback() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.rollback();
        db.getMap("grades",Character.class, String.class);
    }

    @Test
    public void previousReferencesStayValidAfterCommit() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.commit();
        txMgr.begin();
        gradeDetail.put('A', "Success!");
        txMgr.commit();
        txMgr.begin();
        assertEquals(db.getMap("grades", Character.class, String.class).get('A'), "Success!");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMapValueClassNotSerializable() throws InstantiationException, SystemException,
            NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, UnserializableObject> gradeDetail = db.createMap("asfddd",Character.class,
                UnserializableObject.class);
        gradeDetail.put('s', new UnserializableObject());
        txMgr.rollback();
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMapNameBlank() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("",Character.class, String.class);
        txMgr.rollback();
    }
    @Test(expected = IllegalArgumentException.class)
    public void createMapNameSpaces() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap(" ",Character.class, String.class);
        txMgr.rollback();
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMapNameExists() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        Map<Character, String> gradeDetail2 = db.createMap("a",Character.class, String.class);
        txMgr.rollback();
    }
    @Test(expected = IllegalArgumentException.class)
    public void createMapNameExistsButDifferentClasses() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        Map<String, String> gradeDetail2 = db.createMap("a",String.class, String.class);
        txMgr.rollback();
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMapNameExistsSeparateCommit() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        txMgr.commit();
        txMgr.begin();
        Map<Character, String> gradeDetail2 = db.createMap("a",Character.class, String.class);
        txMgr.rollback();
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMapNameExistsOkayAfterRollback() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        txMgr.rollback();
        txMgr.begin();
        Map<Character, String> gradeDetail2 = db.createMap("a",Character.class, String.class);
        txMgr.rollback();
    }

    @Test(expected = java.util.NoSuchElementException.class)
    public void getMapNotFound() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.getMap("nonexistent",Character.class, String.class);
        txMgr.rollback();
    }

    @Test(expected = ClassCastException.class)
    public void getMapMismatchedKeyAndValueClasses() throws InstantiationException, SystemException,
            NotSupportedException,
            RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        db.getMap("a", Integer.class, Integer.class);
        txMgr.rollback();
    }
    @Test(expected = ClassCastException.class)
    public void getMapMismatchedKeyClass() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        db.getMap("a", Integer.class, String.class);
        txMgr.rollback();
    }
    @Test(expected = ClassCastException.class)
    public void getMapMismatchedValueClass() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("a",Character.class, String.class);
        db.getMap("a", Character.class, Integer.class);
        txMgr.rollback();
    }
    @Test(expected = IllegalArgumentException.class)
    public void getMapNameBlank() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        db.getMap("", Integer.class, Integer.class);
        txMgr.rollback();
    }
    @Test(expected = IllegalArgumentException.class)
    public void getMapNameSpaces() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        db.getMap(" ", Integer.class, Integer.class);
        txMgr.rollback();
    }

    @Test
    public void createAndRetrieveMap() throws InstantiationException, SystemException, NotSupportedException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        Map<Character, String> retrievedMap = db.getMap("grades",Character.class, String.class);
        assertEquals(retrievedMap, gradeDetail);
        txMgr.rollback();
    }


    @Test(expected = ClientNotInTxException.class)
    public void getUsingReferenceOutsideTx() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.commit();
        System.out.println(gradeDetail);
    }
}