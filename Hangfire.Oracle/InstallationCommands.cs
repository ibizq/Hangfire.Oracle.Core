namespace Hangfire.Oracle.Core
{
    public static class InstallationCommands
    {
        public static string Command01 => @"CREATE SEQUENCE HF_SEQUENCE START WITH 1 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER";

        public static string Command02 => @"CREATE SEQUENCE HF_JOB_ID_SEQ START WITH 1 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER";

        /// <summary>
        /// Table structure for `Job`
        /// </summary>
        public static string Command03 => @"CREATE TABLE HF_JOB(ID NUMBER (10)
                         ,STATE_ID NUMBER(10)
                         ,STATE_NAME NVARCHAR2(20)
                         ,INVOCATION_DATA NCLOB
                         , ARGUMENTS         NCLOB
                         ,CREATED_AT TIMESTAMP(4)
                         ,EXPIRE_AT TIMESTAMP(4))
LOB(INVOCATION_DATA) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOB(ARGUMENTS) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command04 => @"ALTER TABLE HF_JOB ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `Counter`
        /// </summary>
        public static string Command05 => @"CREATE TABLE HF_COUNTER(ID NUMBER (10)
                             ,KEY NVARCHAR2(255)
                             ,VALUE NUMBER(10)
                             ,EXPIRE_AT TIMESTAMP(4))
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command06 => @"ALTER TABLE HF_COUNTER ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";


        public static string Command07 => @"CREATE TABLE HF_AGGREGATED_COUNTER(ID NUMBER (10)
                                        ,KEY NVARCHAR2(255)
                                        ,VALUE NUMBER(10)
                                        ,EXPIRE_AT TIMESTAMP(4))
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command08 => @"ALTER TABLE HF_AGGREGATED_COUNTER ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE,
  UNIQUE (KEY)
  USING INDEX
  ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `DistributedLock`
        /// </summary>
        public static string Command09 => @"CREATE TABLE HF_DISTRIBUTED_LOCK(""RESOURCE"" NVARCHAR2 (100), CREATED_AT TIMESTAMP(4))
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        /// <summary>
        /// Table structure for `Hash`
        /// </summary>
        public static string Command10 => @"CREATE TABLE HF_HASH(ID NUMBER (10)
                          ,KEY NVARCHAR2(255)
                          ,VALUE NCLOB
                          , EXPIRE_AT   TIMESTAMP(4)
                          ,FIELD NVARCHAR2(40))
LOB(VALUE) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command11 => @"ALTER TABLE HF_HASH ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE,
  UNIQUE (KEY, FIELD)
  USING INDEX
  ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `JobParameter`
        /// </summary>
        public static string Command12 => @"CREATE TABLE HF_JOB_PARAMETER(ID NUMBER (10)
                                   ,NAME NVARCHAR2(40)
                                   ,VALUE NCLOB
                                   , JOB_ID   NUMBER(10))
LOB(VALUE) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command13 => @"ALTER TABLE HF_JOB_PARAMETER ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";

        public static string Command14 => @"ALTER TABLE HF_JOB_PARAMETER ADD(
          CONSTRAINT FK_JOB_PARAMETER_JOB
        
          FOREIGN KEY (JOB_ID)
          REFERENCES HF_JOB (ID)
          ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `JobQueue`
        /// </summary>
        public static string Command15 => @"CREATE TABLE HF_JOB_QUEUE(ID NUMBER (10)
                               ,JOB_ID NUMBER(10)
                               ,QUEUE NVARCHAR2(50)
                               ,FETCHED_AT TIMESTAMP(4)
                               ,FETCH_TOKEN NVARCHAR2(36))
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command16 => @"ALTER TABLE HF_JOB_QUEUE ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";

        public static string Command17 => @"ALTER TABLE HF_JOB_QUEUE ADD(
          CONSTRAINT FK_JOB_QUEUE_JOB
        
          FOREIGN KEY (JOB_ID)
          REFERENCES HF_JOB (ID)
          ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `JobState`
        /// </summary>
        public static string Command18 => @"CREATE TABLE HF_JOB_STATE(ID NUMBER (10)
                               ,JOB_ID NUMBER(10)
                               ,NAME NVARCHAR2(20)
                               ,REASON NVARCHAR2(100)
                               ,CREATED_AT TIMESTAMP(4)
                               ,DATA NCLOB)
LOB(DATA) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command19 => @"ALTER TABLE HF_JOB_STATE ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";

        public static string Command20 => @"ALTER TABLE HF_JOB_STATE ADD(
          CONSTRAINT FK_JOB_STATE_JOB
        
          FOREIGN KEY (JOB_ID)
          REFERENCES HF_JOB (ID)
          ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `Server`
        /// </summary>
        public static string Command21 => @"CREATE TABLE HF_SERVER(ID NVARCHAR2 (100), DATA NCLOB, LAST_HEART_BEAT TIMESTAMP(4))
LOB(DATA) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command22 => @"ALTER TABLE HF_SERVER ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";

        /// <summary>
        /// Table structure for `Set`
        /// </summary>
        public static string Command23 => @"CREATE TABLE HF_SET(ID NUMBER (10)
                         ,KEY NVARCHAR2(255)
                         ,VALUE NVARCHAR2(255)
                         ,SCORE FLOAT(126)
                         ,EXPIRE_AT TIMESTAMP(4))
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command24 => @"ALTER TABLE HF_SET ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE,
  UNIQUE (KEY, VALUE)
  USING INDEX
  ENABLE VALIDATE)";

        public static string Command25 => @"CREATE TABLE HF_LIST(ID NUMBER (10)
                          ,KEY NVARCHAR2(255)
                          ,VALUE NCLOB
                          , EXPIRE_AT   TIMESTAMP(4))
LOB(VALUE) STORE AS BASICFILE
  (ENABLE STORAGE IN ROW

   CHUNK 8192

   RETENTION
   NOCACHE LOGGING)
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
MONITORING";

        public static string Command26 => @"ALTER TABLE HF_LIST ADD(
  PRIMARY KEY
  (ID)
  USING INDEX
  ENABLE VALIDATE)";
    }
}
