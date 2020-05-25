package com.zaxxer.hikari;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zaxxer.hikari.proxy.JavassistProxyFactory;
import com.zaxxer.hikari.util.PropertyBeanSetter;
public class HikariConfig implements HikariConfigMBean {
  public static long ACQUIRE_RETRY_DELAY=750L;
  public static long CONNECTION_TIMEOUT=5000L;
  public static long IDLE_TIMEOUT=TimeUnit.MINUTES.toMillis(10);
  public static long MAX_LIFETIME=TimeUnit.MINUTES.toMillis(30);
  public static int poolNumber;
  public volatile int acquireIncrement;
  public volatile int acquireRetries;
  public volatile long acquireRetryDelay;
  public volatile long connectionTimeout;
  public volatile long idleTimeout;
  public volatile long leakDetectionThreshold;
  public volatile long maxLifetime;
  public volatile int maxPoolSize;
  public volatile int minPoolSize;
  public String transactionIsolationName;
  public String connectionCustomizerClassName;
  public String connectionInitSql;
  public String connectionTestQuery;
  public String dataSourceClassName;
  public String catalog;
  public String poolName;
  public boolean isAutoCommit;
  public boolean isInitializationFailFast;
  public boolean isJdbc4connectionTest;
  public boolean isRegisterMbeans;
  public DataSource dataSource;
  public Properties dataSourceProperties;
  public IConnectionCustomizer connectionCustomizer;
  public int transactionIsolation;
static {
    JavassistProxyFactory.initialize();
  }
  /** 
 * Default constructor
 */
  public HikariConfig(){
    dataSourceProperties=new Properties();
    acquireIncrement=1;
    acquireRetries=3;
    acquireRetryDelay=ACQUIRE_RETRY_DELAY;
    connectionTimeout=CONNECTION_TIMEOUT;
    idleTimeout=IDLE_TIMEOUT;
    isAutoCommit=true;
    isJdbc4connectionTest=true;
    minPoolSize=10;
    maxPoolSize=60;
    maxLifetime=MAX_LIFETIME;
    poolName="HikariPool-" + poolNumber++;
    transactionIsolation=-1;
  }
  /** 
 * Construct a HikariConfig from the specified properties object.
 * @param properties the name of the property file
 */
  public HikariConfig(  Properties properties){
    this();
    PropertyBeanSetter.setTargetFromProperties(this,properties);
  }
  /** 
 * Construct a HikariConfig from the specified property file name.
 * @param propertyFileName the name of the property file
 */
  public HikariConfig(  String propertyFileName){
    this();
    File propFile=new File(propertyFileName);
    if (!propFile.isFile()) {
      throw new IllegalArgumentException("Property file " + propertyFileName + " was not found.");
    }
    try {
      FileInputStream fis=new FileInputStream(propFile);
      Properties props=new Properties();
      props.load(fis);
      PropertyBeanSetter.setTargetFromProperties(this,props);
      fis.close();
    }
 catch (    IOException io) {
      throw new RuntimeException("Error loading properties file",io);
    }
  }
  /** 
 * {@inheritDoc} 
 */
  public int getAcquireIncrement(){
    return acquireIncrement;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setAcquireIncrement(  int acquireIncrement){
    if (acquireIncrement < 1) {
      throw new IllegalArgumentException("acquireRetries cannot be less than 1");
    }
    this.acquireIncrement=acquireIncrement;
  }
  /** 
 * {@inheritDoc} 
 */
  public int getAcquireRetries(){
    return acquireRetries;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setAcquireRetries(  int acquireRetries){
    if (acquireRetries < 0) {
      throw new IllegalArgumentException("acquireRetries cannot be negative");
    }
    this.acquireRetries=acquireRetries;
  }
  /** 
 * {@inheritDoc} 
 */
  public long getAcquireRetryDelay(){
    return acquireRetryDelay;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setAcquireRetryDelay(  long acquireRetryDelayMs){
    if (acquireRetryDelayMs < 0) {
      throw new IllegalArgumentException("acquireRetryDelay cannot be negative");
    }
    this.acquireRetryDelay=acquireRetryDelayMs;
  }
  public String getCatalog(){
    return catalog;
  }
  public void setCatalog(  String catalog){
    this.catalog=catalog;
  }
  public String getConnectionCustomizerClassName(){
    return connectionCustomizerClassName;
  }
  public void setConnectionCustomizerClassName(  String connectionCustomizerClassName){
    this.connectionCustomizerClassName=connectionCustomizerClassName;
  }
  public String getConnectionTestQuery(){
    return connectionTestQuery;
  }
  public void setConnectionTestQuery(  String connectionTestQuery){
    this.connectionTestQuery=connectionTestQuery;
  }
  public String getConnectionInitSql(){
    return connectionInitSql;
  }
  public void setConnectionInitSql(  String connectionInitSql){
    this.connectionInitSql=connectionInitSql;
  }
  /** 
 * {@inheritDoc} 
 */
  public long getConnectionTimeout(){
    return connectionTimeout;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setConnectionTimeout(  long connectionTimeoutMs){
    if (connectionTimeoutMs == 0) {
      this.connectionTimeout=Integer.MAX_VALUE;
    }
 else     if (connectionTimeoutMs < 100) {
      throw new IllegalArgumentException("connectionTimeout cannot be less than 100ms");
    }
 else {
      this.connectionTimeout=connectionTimeoutMs;
    }
  }
  public DataSource getDataSource(){
    return dataSource;
  }
  public void setDataSource(  DataSource dataSource){
    this.dataSource=dataSource;
  }
  public String getDataSourceClassName(){
    return dataSourceClassName;
  }
  public void setDataSourceClassName(  String className){
    this.dataSourceClassName=className;
  }
  public void addDataSourceProperty(  String propertyName,  Object value){
    dataSourceProperties.put(propertyName,value);
  }
  public Properties getDataSourceProperties(){
    return dataSourceProperties;
  }
  public void setDataSourceProperties(  Properties dsProperties){
    dataSourceProperties.putAll(dsProperties);
  }
  /** 
 * {@inheritDoc} 
 */
  public long getIdleTimeout(){
    return idleTimeout;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setIdleTimeout(  long idleTimeoutMs){
    this.idleTimeout=idleTimeoutMs;
  }
  public boolean isAutoCommit(){
    return isAutoCommit;
  }
  public void setAutoCommit(  boolean isAutoCommit){
    this.isAutoCommit=isAutoCommit;
  }
  public boolean isInitializationFailFast(){
    return isInitializationFailFast;
  }
  public void setInitializationFailFast(  boolean failFast){
    isInitializationFailFast=failFast;
  }
  public boolean isJdbc4ConnectionTest(){
    return isJdbc4connectionTest;
  }
  public void setJdbc4ConnectionTest(  boolean useIsValid){
    this.isJdbc4connectionTest=useIsValid;
  }
  public boolean isRegisterMbeans(){
    return isRegisterMbeans;
  }
  public void setRegisterMbeans(  boolean register){
    this.isRegisterMbeans=register;
  }
  /** 
 * {@inheritDoc} 
 */
  public long getLeakDetectionThreshold(){
    return leakDetectionThreshold;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setLeakDetectionThreshold(  long leakDetectionThresholdMs){
    this.leakDetectionThreshold=leakDetectionThresholdMs;
  }
  public void setUseInstrumentation(  boolean useInstrumentation){
  }
  /** 
 * {@inheritDoc} 
 */
  public long getMaxLifetime(){
    return maxLifetime;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setMaxLifetime(  long maxLifetimeMs){
    this.maxLifetime=maxLifetimeMs;
  }
  /** 
 * {@inheritDoc} 
 */
  public int getMinimumPoolSize(){
    return minPoolSize;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setMinimumPoolSize(  int minPoolSize){
    if (minPoolSize < 0) {
      throw new IllegalArgumentException("minPoolSize cannot be negative");
    }
    this.minPoolSize=minPoolSize;
  }
  /** 
 * {@inheritDoc} 
 */
  public int getMaximumPoolSize(){
    return maxPoolSize;
  }
  /** 
 * {@inheritDoc} 
 */
  public void setMaximumPoolSize(  int maxPoolSize){
    if (maxPoolSize < 0) {
      throw new IllegalArgumentException("maxPoolSize cannot be negative");
    }
    this.maxPoolSize=maxPoolSize;
  }
  /** 
 * {@inheritDoc} 
 */
  public String getPoolName(){
    return poolName;
  }
  /** 
 * Set the name of the connection pool.  This is primarily used for the MBean to uniquely identify the pool configuration.
 * @param poolName the name of the connection pool to use
 */
  public void setPoolName(  String poolName){
    this.poolName=poolName;
  }
  public int getTransactionIsolation(){
    return transactionIsolation;
  }
  /** 
 * Set the default transaction isolation level.  The specified value is the constant name from the <code>Connection</code> class, eg.  <code>TRANSACTION_REPEATABLE_READ</code>.
 * @param isolationLevel the name of the isolation level
 */
  public void setTransactionIsolation(  String isolationLevel){
    this.transactionIsolationName=isolationLevel;
  }
  public void validate(){
    Logger logger=LoggerFactory.getLogger(getClass());
    if (acquireRetryDelay < 0) {
      logger.error("acquireRetryDelay cannot be negative.");
      throw new IllegalStateException("acquireRetryDelay cannot be negative.");
    }
 else     if (acquireRetryDelay < 100) {
      logger.warn("acquireRetryDelay is less than 100ms, did you specify the wrong time unit?  Using default instead.");
      acquireRetryDelay=ACQUIRE_RETRY_DELAY;
    }
    if (connectionCustomizerClassName != null && connectionCustomizer == null) {
      try {
        Class<?> customizerClass=getClass().getClassLoader().loadClass(connectionCustomizerClassName);
        connectionCustomizer=(IConnectionCustomizer)customizerClass.newInstance();
      }
 catch (      Exception e) {
        logger.warn("connectionCustomizationClass specified class '" + connectionCustomizerClassName + "' could not be loaded",e);
        connectionCustomizerClassName=null;
      }
    }
    if (connectionTimeout == Integer.MAX_VALUE) {
      logger.warn("No connection wait timeout is set, this might cause an infinite wait.");
    }
 else     if (connectionTimeout < 100) {
      logger.warn("connectionTimeout is less than 100ms, did you specify the wrong time unit?  Using default instead.");
      connectionTimeout=CONNECTION_TIMEOUT;
    }
    if (dataSource == null && dataSourceClassName == null) {
      logger.error("one of either dataSource or dataSourceClassName must be specified");
      throw new IllegalStateException("one of either dataSource or dataSourceClassName must be specified");
    }
 else     if (dataSource != null && dataSourceClassName != null) {
      logger.warn("both dataSource and dataSourceClassName are specified, ignoring dataSourceClassName");
    }
    if (idleTimeout < 0) {
      logger.error("idleTimeout cannot be negative.");
      throw new IllegalStateException("idleTimeout cannot be negative.");
    }
 else     if (idleTimeout < 30000 && idleTimeout != 0) {
      logger.warn("idleTimeout is less than 30000ms, did you specify the wrong time unit?  Using default instead.");
      idleTimeout=IDLE_TIMEOUT;
    }
    if (!isJdbc4connectionTest && connectionTestQuery == null) {
      logger.error("Either jdbc4ConnectionTest must be enabled or a connectionTestQuery must be specified.");
      throw new IllegalStateException("Either jdbc4ConnectionTest must be enabled or a connectionTestQuery must be specified.");
    }
    if (leakDetectionThreshold != 0 && leakDetectionThreshold < 10000) {
      logger.warn("leakDetectionThreshold is less than 10000ms, did you specify the wrong time unit?  Disabling leak detection.");
      leakDetectionThreshold=0;
    }
    if (maxPoolSize < minPoolSize) {
      logger.warn("maxPoolSize is less than minPoolSize, forcing them equal.");
      maxPoolSize=minPoolSize;
    }
    if (maxLifetime < 0) {
      logger.error("maxLifetime cannot be negative.");
      throw new IllegalStateException("maxLifetime cannot be negative.");
    }
 else     if (maxLifetime < 120000 && maxLifetime != 0) {
      logger.warn("maxLifetime is less than 120000ms, did you specify the wrong time unit?  Using default instead.");
      maxLifetime=MAX_LIFETIME;
    }
    if (transactionIsolationName != null) {
      try {
        Field field=Connection.class.getField(transactionIsolationName);
        int level=field.getInt(null);
        this.transactionIsolation=level;
      }
 catch (      Exception e) {
        throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
      }
    }
  }
  IConnectionCustomizer getConnectionCustomizer(){
    return connectionCustomizer;
  }
  void copyState(  HikariConfig other){
    for (    Field field : HikariConfig.class.getDeclaredFields()) {
      if (!Modifier.isFinal(field.getModifiers())) {
        field.setAccessible(true);
        try {
          field.set(other,field.get(this));
        }
 catch (        Exception e) {
          throw new RuntimeException("Exception copying HikariConfig state: " + e.getMessage(),e);
        }
      }
    }
  }
}
