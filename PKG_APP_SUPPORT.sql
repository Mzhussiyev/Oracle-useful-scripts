  CREATE OR REPLACE PACKAGE "DBADMIN"."PKG_APP_SUPPORT" 
  IS

  -- Author  : Mzhussiyev
  -- Created : 2016-06-20 14:04:22
  -- Purpose : Tools for kill sessions

  ------------------------------------------------------------------------------------
  --  METHOD CONTROLED BY "SETUP"
  --   Setup table contains two columns
  --   - MANAGER: it is column which is tested for current DB_USER
  --   - MANAGED: columns contains exact DB_USER names which can be killed by MANAGER
  ------------------------------------------------------------------------------------

  --  Kill one session
  PROCEDURE pKillSession( anSid NUMBER, anSerial# NUMBER, anInstID NUMBER DEFAULT 1 ) ;
  --  Kill all session for DB_USER name like
  PROCEDURE pKillDbUser( acDbUserNameLike VARCHAR2 ) ;
  --  Kill all session for OS_USER name like
  PROCEDURE pKillOsUser( acOsUserNameUpperLike VARCHAR2 ) ;
  --  Kill all jobs for JOB_OWNER like and JOB_NAME like
  PROCEDURE pKillJob( acJobOwnerLike VARCHAR2, acJobNameLike VARCHAR2 ) ;

  ----------------------------------------------
  --  METHOD WHICH ALLOWS TO KILL "MY SESSIONS"
  ----------------------------------------------

  --  Kill one session identified by current DB_USER and OS_USER
  PROCEDURE pKillMySession( anSid NUMBER, anSerial# NUMBER, anInstID NUMBER DEFAULT 1 ) ;
  --  Kill all session identified by current DB_USER and OS_USER
  PROCEDURE pKillMySessions ;

  ---------------------------
  --  VARIABLES FOR DEBUGING
  ---------------------------
  --  Enable console output
  B_DEBUG     BOOLEAN := False ;
  --  Values
  --    False:  standard ussage
  --    True :  complete run without "killing"
  B_TEST_ONLY BOOLEAN := False ;

END PKG_APP_SUPPORT;
CREATE OR REPLACE PACKAGE BODY "DBADMIN"."PKG_APP_SUPPORT" 
  IS
  --  Type of kill/method
  KILL_TYPE_SESSION CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 1 ;
  KILL_TYPE_DB_USER CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 2 ;
  KILL_TYPE_OS_USER CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 3 ;
  KILL_TYPE_JOBNAME CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 4 ;
  KILL_TYPE_MY_SESS CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 5 ;
  KILL_TYPE_MY_SESA CONSTANT APP_SUPPORT_LOG.KILL_TYPE%Type := 6 ;
  --  Information/identification current session
  gCurrentSessionID   CONSTANT GV$SESSION.SID%Type    := Sys_Context( 'USERENV', 'SID' ) ;
  gCurrentOsUserName  CONSTANT GV$SESSION.OSUSER%Type := Sys_Context( 'USERENV', 'OS_USER' ) ;
  --
  CRLF  CONSTANT VARCHAR2(2) := Chr(13)||Chr(10) ;
  ------------------------------------
  --  CURSOR WHICH GENERATES COMMANDS
  ------------------------------------
  --  Cursor for identify: session
  CURSOR  CrsKillSession( bnSid NUMBER, bnSerial# NUMBER, bnInstId NUMBER )
    IS
    SELECT  SE.USERNAME, SE.OSUSER
          , 'ALTER /* USR:'||SE.USERNAME||' OS_USR:'||SE.OSUSER||'*/'||
          ' SYSTEM KILL SESSION '''||SE.SID||','||SE.SERIAL#||',@'||SE.INST_ID||''' IMMEDIATE' CmdKill
      FROM  dbadmin.APP_SUPPORT_STP ST
      JOIN  GV$SESSION      SE ON SE.USERNAME = ST.MANAGED_USER
      WHERE ST.USER_MANAGER = User
        And SE.SID != gCurrentSessionID
        And SE.SID = bnSid And SE.SERIAL# = bnSerial# And SE.INST_ID = bnInstId ;
  --  Cursor for identify: db user name
  CURSOR  CrsDbUserName( bcDbUser VARCHAR2 )
    IS
    SELECT  SE.USERNAME, SE.OSUSER
          , 'ALTER /* USR:'||SE.USERNAME||' OS_USR:'||SE.OSUSER||'*/'||
          ' SYSTEM KILL SESSION '''||SE.SID||','||SE.SERIAL#||',@'||SE.INST_ID||''' IMMEDIATE' CmdKill
      FROM  APP_SUPPORT_STP ST
      JOIN  GV$SESSION      SE ON SE.USERNAME = ST.MANAGED_USER
      WHERE ST.USER_MANAGER = User
        And SE.SID != gCurrentSessionID
        And SE.USERNAME LIKE bcDbUser ;
  --  Cursor for identify: os user name
  CURSOR  CrsOsUserName( bcOsUser VARCHAR2 )
    IS
    SELECT  SE.USERNAME, SE.OSUSER
          , 'ALTER /* USR:'||SE.USERNAME||' OS_USR:'||SE.OSUSER||'*/'||
          ' SYSTEM KILL SESSION '''||SE.SID||','||SE.SERIAL#||',@'||SE.INST_ID||''' IMMEDIATE' CmdKill
      FROM  APP_SUPPORT_STP ST
      JOIN  GV$SESSION      SE ON SE.USERNAME = ST.MANAGED_USER
      WHERE ST.USER_MANAGER = User
        And SE.SID != gCurrentSessionID
        And Upper( SE.OSUSER ) LIKE Upper( bcOsUser ) ;
  --  Cursor for identify: job
  CURSOR  CrsJobOwnerName( bcJobOwner VARCHAR2, bcJobName VARCHAR2 )
    IS
    SELECT  SJ.OWNER, SJ.JOB_NAME
          , 'BEGIN /*STATE='||STATE||'*/DBMS_SCHEDULER.Stop_Job('||
            'Job_Name => ''"'||OWNER||'"."'||JOB_NAME||'"'', Force=>True); END;' CmdKill
      FROM  APP_SUPPORT_STP     ST
      JOIN  ALL_SCHEDULER_JOBS  SJ ON SJ.OWNER = ST.MANAGED_USER
      WHERE ST.USER_MANAGER = User
        And SJ.STATE = 'RUNNING'
        And SJ.OWNER LIKE bcJobOwner And SJ.JOB_NAME LIKE bcJobName ;
  --  Cursor for my session: SID/SER#
  CURSOR  CrsKillMySession( bnSid NUMBER, bnSerial# NUMBER, bnInstId NUMBER )
    IS
    SELECT  SE.USERNAME, SE.OSUSER
          , 'ALTER /* USR:'||SE.USERNAME||' OS_USR:'||SE.OSUSER||'*/'||
          ' SYSTEM KILL SESSION '''||SE.SID||','||SE.SERIAL#||',@'||SE.INST_ID||''' IMMEDIATE' CmdKill
      FROM  GV$SESSION  SE
      WHERE SE.USERNAME = User
        And SE.OSUSER = gCurrentOsUserName
        And SE.SID != gCurrentSessionID
        And SE.SID = bnSid And SE.SERIAL# = bnSerial# And SE.INST_ID = bnInstId ;
  --  Cursor for my sessions: all
  CURSOR  CrsKillMySessions
    IS
    SELECT  SE.USERNAME, SE.OSUSER
          , 'ALTER /* USR:'||SE.USERNAME||' OS_USR:'||SE.OSUSER||'*/'||
          ' SYSTEM KILL SESSION '''||SE.SID||','||SE.SERIAL#||',@'||SE.INST_ID||''' IMMEDIATE' CmdKill
      FROM  GV$SESSION  SE
      WHERE SE.USERNAME = User
        And SE.OSUSER = gCurrentOsUserName
        And SE.SID != gCurrentSessionID ;
  --  Definition for killing
  SUBTYPE ST_ORA_NAME IS VARCHAR2(  30 ) ;
  SUBTYPE ST_LONGNAME IS VARCHAR2( 256 ) ;
  SUBTYPE ST_KILL_CMD IS VARCHAR2( 512 ) ;
  TYPE  REC_KILL_SESSION IS RECORD
    ( DbName  ST_ORA_NAME
    , OsName  ST_LONGNAME
    , CmdKill ST_KILL_CMD
    ) ;
  TYPE TAB_KILL_SESSION IS TABLE OF REC_KILL_SESSION INDEX BY BINARY_INTEGER ;
  ------------------
  --  LOCAL METHODS
  ------------------
  --  Debugging
  PROCEDURE pDebug( ac VARCHAR2 )
    IS
  BEGIN
    IF B_DEBUG THEN
      DBMS_OUTPUT.Put_Line( ac ) ;
    END IF ;
  END ;
  --  Logging start
  PROCEDURE pLogStart( aRID       OUT ROWID
                     , anKillType APP_SUPPORT_LOG.KILL_TYPE%Type
                     , acRequest  APP_SUPPORT_LOG.REQUEST%Type )
    IS
    PRAGMA AUTONOMOUS_TRANSACTION ;
  BEGIN
    INSERT INTO APP_SUPPORT_LOG( LOG_PK, KILL_TYPE, REQUEST)
      VALUES ( APP_SUPPORT_LOG_SQ.NextVal, anKillType, acRequest )
      RETURNING ROWID INTO aRID ;
    COMMIT ;
  END ;
  --  Logging end
  PROCEDURE pLogEnd( aRID     ROWID
                   , anTotal  APP_SUPPORT_LOG.N_TOTAL%Type
                   , anErrors APP_SUPPORT_LOG.N_ERRORS%Type
                   , acResult APP_SUPPORT_LOG.RESULT%Type )
    IS
    PRAGMA AUTONOMOUS_TRANSACTION ;
  BEGIN
    IF aRID Is Not Null THEN
      UPDATE  APP_SUPPORT_LOG L
        SET   FINISHED = SysDate
            , N_TOTAL  = anTotal
            , N_ERRORS = anErrors
            , RESULT   = acResult
        WHERE L.ROWID = aRID ;
      COMMIT ;
    END IF ;
  END ;
  --  Process kill
  PROCEDURE processKill( anKillType APP_SUPPORT_LOG.KILL_TYPE%Type
                       , acRequest  APP_SUPPORT_LOG.REQUEST%Type
                       , atSessions TAB_KILL_SESSION )
    IS
    lLogRID   ROWID ;
    lbiIdx    BINARY_INTEGER ;
    liErrors  INTEGER := 0 ;
    lcResult  APP_SUPPORT_LOG.RESULT%Type ;
  BEGIN
    pLogStart( lLogRID, anKillType, acRequest ) ;
    lbiIdx    := atSessions.First ;
    lcResult  :=  'REQUEST '||CASE WHEN B_TEST_ONLY THEN '(Test mode)' END ||
                  ':'||CRLF||acRequest||CRLF||
                  CASE WHEN lbiIdx Is Null THEN '- No data for given conditions.' ELSE 'RESULT:' END ;
    WHILE lbiIdx Is Not Null LOOP
      BEGIN
        lcResult  := lcResult||CRLF||atSessions( lbiIdx ).CmdKill ;
        pDebug( atSessions( lbiIdx ).CmdKill ) ;
        IF Not B_TEST_ONLY THEN
          EXECUTE IMMEDIATE atSessions( lbiIdx ).CmdKill ;
        END IF ;
      EXCEPTION WHEN Others THEN
        liErrors := liErrors + 1 ;
        lcResult  := lcResult||CRLF||SqlErrM ;
      END ;
      lbiIdx  := atSessions.Next( lbiIdx ) ;
    END LOOP ;
    pLogEnd( lLogRID, atSessions.Count, liErrors, lcResult ) ;
  END ;
  --------------------
  --  PUBLIC METHODS
  --------------------
  --  KILL: SID/SERIAL#
  PROCEDURE pKillSession( anSid NUMBER, anSerial# NUMBER, anInstID NUMBER DEFAULT 1 )
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsKillSession( anSid, anSerial#, anInstID ) ;
    FETCH CrsKillSession BULK COLLECT INTO ltCurs ;
    CLOSE CrsKillSession ;
    processKill
      ( KILL_TYPE_SESSION
      , 'SID:'||anSid||' SER#:'|| anSerial#||' INST_ID:'|| anInstID
      , ltCurs ) ;
  END ;
  --  KILL: DB_USER_NAME
  PROCEDURE pKillDbUser( acDbUserNameLike VARCHAR2 )
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsDbUserName( acDbUserNameLike ) ;
    FETCH CrsDbUserName BULK COLLECT INTO ltCurs ;
    CLOSE CrsDbUserName ;
    processKill
      ( KILL_TYPE_DB_USER
      , 'DB_USER_NAME:'||acDbUserNameLike
      , ltCurs ) ;
  END ;
  --  KILL: OS_USER_NAME
  PROCEDURE pKillOsUser( acOsUserNameUpperLike VARCHAR2 )
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsOsUserName( acOsUserNameUpperLike ) ;
    FETCH CrsOsUserName BULK COLLECT INTO ltCurs ;
    CLOSE CrsOsUserName ;
    processKill
      ( KILL_TYPE_OS_USER
      , 'OS_USER_NAME:'||acOsUserNameUpperLike
      , ltCurs ) ;
  END ;
  --  KILL: JOB_OWNER + JOB_NAME
  PROCEDURE pKillJob( acJobOwnerLike VARCHAR2, acJobNameLike VARCHAR2 )
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsJobOwnerName( acJobOwnerLike, acJobNameLike ) ;
    FETCH CrsJobOwnerName BULK COLLECT INTO ltCurs ;
    CLOSE CrsJobOwnerName ;
    processKill
      ( KILL_TYPE_JOBNAME
      , 'JOB_OWNER:'||acJobOwnerLike||' JOB_NAME:'|| acJobNameLike
      , ltCurs ) ;
  END ;
  --  KILL MY SESSION: SID/SERIAL#
  PROCEDURE pKillMySession( anSid NUMBER, anSerial# NUMBER, anInstID NUMBER DEFAULT 1 )
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsKillMySession( anSid, anSerial#, anInstID ) ;
    FETCH CrsKillMySession BULK COLLECT INTO ltCurs ;
    CLOSE CrsKillMySession ;
    processKill
      ( KILL_TYPE_MY_SESS
      , 'MY SESSION:'||'SID:'||anSid||' SER#:'|| anSerial#||' INST_ID:'|| anInstID
      , ltCurs ) ;
  END ;
  --  KILL MY SESSIONS: all
  PROCEDURE pKillMySessions
    IS
    ltCurs  TAB_KILL_SESSION ;
  BEGIN
    OPEN  CrsKillMySessions ;
    FETCH CrsKillMySessions BULK COLLECT INTO ltCurs ;
    CLOSE CrsKillMySessions ;
    processKill
      ( KILL_TYPE_MY_SESA
      , 'ALL MY SESSIONS'
      , ltCurs ) ;
  END ;
END PKG_APP_SUPPORT;
