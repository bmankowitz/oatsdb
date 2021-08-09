package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PersistenceTests {
    ConfigurablePersistentDBMS db;
    TxMgr txMgr;
    Map<Character, String> gradeDetail;
    int i;


    @Before
    public void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException{
        db = (ConfigurablePersistentDBMS) OATSDBType.dbmsFactory(OATSDBType.V2);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V2);
    }

    @After
    public void after() throws InstantiationException {
        db = null;
        txMgr = null;
    }
    @Test
    public void simpleMasterTest() throws SystemException{
        HashMap<String, Long> map = new HashMap<>();
        map.put("tet", 23L);
        Globals.setMasterValues(map);
    }
    @Test
    public void simpleWriteTempTest() throws SystemException{
        HashMap<String, Integer> map = new HashMap<>();
        map.put("tet", 1);
        map.put("ftet", 3);
        DBTableMetadata<String, Integer> metaMap =
                new DBTableMetadata<>("name", String.class, Integer.class, map);
        Globals.writeTempMap(metaMap);
    }
    @Test
    public void simpleGetStorageTest() throws SystemException{
        System.out.println(db.getDiskUsageInMB());
    }
    @Test
    public void simpleClearStorageTest() throws SystemException{
        db.clear();
        Assert.assertEquals(0, Globals.getStorageSize());
    }
    @Test
    public void simpleCommitTest() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        gradeDetail.put('s', "this should persist");
        txMgr.commit();
    }
    @Test
    public void simpleReadStorageTest() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        Globals.readStorage();
        txMgr.begin();
        Assert.assertNotNull(db.getMap("grades", Character.class,String.class));
        txMgr.commit();
    }
}
