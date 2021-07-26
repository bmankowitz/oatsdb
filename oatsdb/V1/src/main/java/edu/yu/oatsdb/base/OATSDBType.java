package edu.yu.oatsdb.base;

/** Enum defining the valid set of OATSdb versions.
 *
 * @author Avraham Leff
 */

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum OATSDBType {
  V0, V1, V2;


  /** Use reflection to create appropriate instance of DBMS
   *
   * @param OATSDB_Type enum value specifies the DBMS instance you want
   * @throws InstantiationException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static DBMS dbmsFactory(OATSDBType oatsdbType)
  throws InstantiationException
  {
    logger.info("Creating DBMS implementation for {}", oatsdbType);

    DBMS dbms = null;

    try {
      if (oatsdbType == null) {
        final String msg = "Null 'oatsdbType' parameter";
        logger.error(msg);
        throw new ClassNotFoundException(msg);
      }

      Class<?> clazz = null;
      Object instance = null;

      switch(oatsdbType) {
      case V0:
        clazz = Class.forName("edu.yu.oatsdb.v0.DBMSImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        dbms = DBMS.class.cast(instance);
        break;
      case V1:
        clazz = Class.forName("edu.yu.oatsdb.v1.DBMSImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        dbms = DBMS.class.cast(instance);
        break;
      case V2:
        clazz = Class.forName("edu.yu.oatsdb.v2.DBMSImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        dbms = DBMS.class.cast(instance);
        break;
      default:
        logger.error("Unknown 'oatsdbType' parameter: {}", oatsdbType);
        throw new ClassNotFoundException
          ("Unknown 'oatsdbType' parameter: "+oatsdbType);
      } // switch
    }   // try
    catch(Exception e) {
      final String msg = "Problem creating instance of "+oatsdbType+" ["+e+"]";
      logger.error(msg);
      throw new InstantiationException(msg); // no version which takes "cause"
    }

    return dbms;
  }

  /** Use reflection to create appropriate instance of TxMgr
   *
   * @param OATSDB_Type enum value specifies the TxMgr instance you want
   * @throws InstantiationException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static TxMgr txMgrFactory(OATSDBType oatsdbType)
  throws InstantiationException
  {
    logger.info("Creating TxMgr implementation for {}", oatsdbType);

    TxMgr txMgr = null;

    try {
      if (oatsdbType == null) {
        final String msg = "Null 'oatsdbType' parameter";
        logger.error(msg);
        throw new ClassNotFoundException(msg);
      }
      
      Class<?> clazz = null;
      Object instance = null;

      switch(oatsdbType) {
      case V0:
        clazz = Class.forName("edu.yu.oatsdb.v1.TxMgrImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        txMgr = TxMgr.class.cast(instance);
        break;
      case V1:
        clazz = Class.forName("edu.yu.oatsdb.v1.TxMgrImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        txMgr = TxMgr.class.cast(instance);
        break;
      case V2:
        clazz = Class.forName("edu.yu.oatsdb.v2.TxMgrImpl");
        instance = Enum.valueOf((Class<Enum>)clazz, "Instance");
        txMgr = TxMgr.class.cast(instance);
        break;
      default:
        logger.error("Unknown 'oatsdbType' parameter: {}", oatsdbType);
        throw new ClassNotFoundException
          ("Unknown 'oatsdbType' parameter: "+oatsdbType);
      } // switch
    }   // try
    catch(Exception e) {
      final String msg = "Problem creating instance of "+oatsdbType+" ["+e+"]";
      logger.error(msg);
      throw new InstantiationException(msg); // no version which takes "cause"
    }

    return txMgr;
  }
  
  // =========================================================================
  // ivars
  // =========================================================================
  private final static Logger logger = LogManager.getLogger(OATSDBType.class);
}
