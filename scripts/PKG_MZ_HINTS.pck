CREATE OR REPLACE PACKAGE AP_CRM.PKG_MZ_HINTS IS

  ---------------------------------------------------
  -- variables
  SUBTYPE GST_ORA_NAME     IS VARCHAR2(30);
  SUBTYPE GST_MAX_SQL_STR  IS VARCHAR2(4000);
  SUBTYPE GST_MAX_PLS_STR  IS VARCHAR2(32000);
  TYPE    GT_MVIEW_NAME    IS TABLE OF VARCHAR2(64);
  TYPE    GT_MVIEW_NAME2   IS TABLE OF VARCHAR2(64) INDEX BY BINARY_INTEGER;
  
  GCV_FMT_STEP     CONSTANT VARCHAR2(10) := '0000';
  GCV_FMT_ROWS     CONSTANT VARCHAR2(15) := '9999,999,990';
  GCV_FMT_DATE     CONSTANT VARCHAR2(25) := 'HH24:Mi:SS';
  GCI_LEN_ROWS     CONSTANT INTEGER      := 20;
  GCI_LEN_ACTION   CONSTANT INTEGER      := 30;
  GCV_ENDLINE      CONSTANT VARCHAR2(4)  := CHR(13) || CHR(10);
  GCV_ENDFORM      CONSTANT VARCHAR2(4)  := '<BR>';
  GI_STEP_No                INTEGER      := 0;
  GD_ACTION_START           DATE;
  GD_STEP_START             DATE;

  AC_MODULE         GST_ORA_NAME := 'ZHUSSIYEV';
  AC_ACTION         GST_ORA_NAME := 'INIT';
  EMAIL_MZHUSSIYEV  GST_MAX_SQL_STR := 'ZHUSSIYEV MEDEU <MZhussiyev@Homecredit.kz>;';
  EMAIL_ADDRESS_OUT GST_MAX_SQL_STR := 'REPORT RESULTS <PKG_MZ_HINTS_pMail@Homecredit.kz>;';

  ---------------------------------------------------
  -- procedures
  PROCEDURE pAppInfo   (acAction      VARCHAR2,
                        acModule      VARCHAR2 DEFAULT AC_MODULE);
  --                      
  PROCEDURE pAppAction (acAction      VARCHAR2);
  --
  PROCEDURE pAppclient (acClientInfo  VARCHAR2);
  --
  PROCEDURE pStats     (acTable       GST_ORA_NAME,
                        acOwner       GST_ORA_NAME DEFAULT User,
                        anPercents    NUMBER DEFAULT 0.000001,
                        anDegree      NUMBER DEFAULT 4);
  --                      
  PROCEDURE pExec       (acExecute    VARCHAR2);
  --
  PROCEDURE pConsole    (acConsole    VARCHAR2);
  --
  PROCEDURE pTruncate   (acTable      GST_ORA_NAME,
                         acOwner      GST_ORA_NAME DEFAULT User);
  --
  PROCEDURE pAlterSession(anDegree    INTEGER DEFAULT 1);
  --
  FUNCTION fnGetSeconds  (adStartTime DATE) RETURN NUMBER;
  --
  FUNCTION fcGetSeconds  (adStartTime DATE) RETURN VARCHAR2;
  --
  PROCEDURE pStepStart   (anStepNo    INTEGER  DEFAULT NULL,
                          acAction    VARCHAR2 DEFAULT Null,
                          acModule    VARCHAR2 DEFAULT NULL);
  --
  PROCEDURE pStepEnd      (anRowsResult INTEGER DEFAULT Null,
                           acTable      VARCHAR2 DEFAULT Null,
                           adStart      DATE DEFAULT Null,
                           isFinish     NUMBER DEFAULT 0);
  --
  PROCEDURE pStepErr       (fnEmailSend number   default 0,
                            AcModule    varchar2 default null);
  --
  PROCEDURE pGttCreate     (p_tab_name  VARCHAR2, p_sql IN VARCHAR2);
  --
  PROCEDURE pMviewRefresh  (acMview     GST_ORA_NAME);
  --
  PROCEDURE pLog           (fTruncate   number default 0);
  PROCEDURE pMail( pSubjects    GT_MVIEW_NAME2, pProcess varchar2 default 'REPORT RESULT');

END;
/
CREATE OR REPLACE PACKAGE BODY AP_CRM.PKG_MZ_HINTS IS

  -------------------------------------

  --  Set application info
  PROCEDURE pAppInfo(acAction VARCHAR2,
                     acModule VARCHAR2 DEFAULT AC_MODULE) IS
  BEGIN
    DBMS_APPLICATION_INFO.Set_Module(module_name => acModule,
                                     action_name => acAction);
  END;
  PROCEDURE pAppAction(acAction VARCHAR2) IS
  BEGIN
    DBMS_APPLICATION_INFO.Set_Action(Action_Name => acAction);
  END;
  --
  PROCEDURE pAppClient(acClientInfo VARCHAR2) IS
  BEGIN
    DBMS_APPLICATION_INFO.Set_Client_Info(client_info => acClientInfo);
  END;
  ------------------------
  PROCEDURE pStats(acTable    GST_ORA_NAME,
                   acOwner    GST_ORA_NAME DEFAULT User,
                   anPercents NUMBER DEFAULT 0.000001,
                   anDegree   NUMBER DEFAULT 4) IS
  BEGIN
    DBMS_STATS.Gather_Table_Stats(OwnName          => acOwner,
                                  TabName          => acTable,
                                  Estimate_Percent => anPercents,
                                  degree           => anDegree);
    pAppInfo('pStats',
             acTable || ' - ' || To_Char(SysDate, GCV_FMT_DATE));
  END;

  --  Dynamic execution
  PROCEDURE pExec(acExecute VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE acExecute;
  END;
  --  Console/Log output
  PROCEDURE pConsole(acConsole VARCHAR2) IS
  BEGIN
    DBMS_OUTPUT.Put_Line(To_Char(SysDate, GCV_FMT_DATE) || ' ' || CASE WHEN
                         AC_ACTION Is Not Null THEN
                         rPad(AC_ACTION, GCI_LEN_ACTION)
                         END || ' ' || acConsole);
  END;
  ----------------------

  PROCEDURE pTruncate(acTable GST_ORA_NAME,
                      acOwner GST_ORA_NAME DEFAULT User) IS
    vTSQL varchar2(4000);
  BEGIN
    vTSQL := 'truncate table ' || acOwner || '.' || acTable; -- || ' drop storage';
    pAppInfo('pTruncate',
             acTable || ' - ' || To_Char(SysDate, GCV_FMT_DATE));
    pExec(acExecute => vTSQL);  
  END;
  --  Modify session
  PROCEDURE pAlterSession(anDegree INTEGER DEFAULT 1) IS
    TYPE TAB_COMMANDS IS TABLE OF GST_MAX_SQL_STR;
    lcExecute  GST_MAX_PLS_STR;
    ltCommands TAB_COMMANDS;
    lbiIndex   BINARY_INTEGER;
  BEGIN
    AC_ACTION       := '--------Parallel---------';
    pConsole('Paralallel option ' || anDegree || ' Start at: ' ||
             To_Char(SysDate, 'YYYY-MM-DD HH24:Mi'));
    ltCommands := TAB_COMMANDS('ALTER SESSION ENABLE PARALLEL DML',
                               'ALTER SESSION ENABLE PARALLEL QUERY',
                               'ALTER SESSION FORCE PARALLEL DML PARALLEL ' ||
                               anDegree,
                               'ALTER SESSION FORCE PARALLEL QUERY PARALLEL ' ||
                               anDegree,
                               'ALTER SESSION SET WORKAREA_SIZE_POLICY = MANUAL',
                               'ALTER SESSION SET SORT_AREA_SIZE = 1073741824',
                               'ALTER SESSION SET HASH_AREA_SIZE = 1073741824');
    lbiIndex   := ltCommands.First;
    WHILE lbiIndex Is Not Null LOOP
      lcExecute := ltCommands(lbiIndex);
      pExec(lcExecute);
      lbiIndex := ltCommands.Next(lbiIndex);
    END LOOP;
  END pAlterSession;

  --  Calculate/formate elapsed time
  FUNCTION fnGetSeconds(adStartTime DATE) RETURN NUMBER IS
  BEGIN
    RETURN 86400 *(SysDate - adStartTime);
  END;
  --
  FUNCTION fcGetSeconds(adStartTime DATE) RETURN VARCHAR2 IS
  BEGIN
    RETURN To_Char(fnGetSeconds(adStartTime), '999,990') || '[secs]';
  END;
  --  Action
  --
  --  Step Start
  PROCEDURE pStepStart(anStepNo  INTEGER DEFAULT NULL,
                       acAction  VARCHAR2 DEFAULT NULL,
                       acModule  VARCHAR2 DEFAULT Null) IS
  BEGIN
    
    IF acModule Is Not Null THEN
      GI_STEP_No      := 0;
      AC_MODULE       := acModule;
      AC_ACTION       := 'INIT';
      GD_ACTION_START := SysDate;
      
      ap_public.core_log_pkg.pInit(acLogModule  => USER,
                                   acLogProcess => AC_MODULE);
      pAppInfo( acAction || ' ' || 'Start:' || To_Char(GD_ACTION_START, GCV_FMT_DATE), AC_MODULE);      
      pConsole('*** START');
    END IF;
    
    IF anStepNo Is Not Null THEN
      GI_STEP_No    := anStepNo;
      AC_ACTION     := acAction;
      GD_STEP_START := SysDate;
      
      ap_public.core_log_pkg.pStart(acLogInfo => acAction);   
      pAppInfo( acAction || ' ' || To_Char(GD_ACTION_START, GCV_FMT_DATE), AC_MODULE); 
      /*pAppClient(acClientInfo => 'STEP ' || To_Char(anStepNo, FMT_STEP) || ' ' ||
                                 'Start:' ||
                                 To_Char(GD_STEP_START, GCV_FMT_DATE));*/
    END IF;
  END;
  --  Step End
  PROCEDURE pStepEnd(anRowsResult INTEGER DEFAULT Null,
                     acTable      VARCHAR2 DEFAULT Null,
                     adStart      DATE DEFAULT Null,
                     isFinish     NUMBER DEFAULT 0) IS
  BEGIN
  
  IF acTable is not null then
  pConsole(rPad(CASE WHEN adStart Is Null THEN
                    'Step ' || To_Char(GI_STEP_No, GCV_FMT_STEP) ELSE '*** END' END,
                    10) ||
               rPad(Nvl(CASE WHEN anRowsResult Is Not Null THEN
                        ' ' || To_Char(anRowsResult, GCV_FMT_ROWS) || ' rows' END,
                        ' '),
                    GCI_LEN_ROWS) || CASE WHEN
               GD_STEP_START Is Not Null Or adStart Is Not Null THEN
               ' ' || fcGetSeconds(Nvl(adStart, GD_STEP_START)) END);
    
        IF anRowsResult Is Not Null THEN
          COMMIT;
          IF acTable IS Not Null THEN
            pStats(acTable);
          END IF;
        END IF;
    END IF;
    
    IF isFinish = 1 THEN
    AC_ACTION       := '-------------------------';
      ap_public.core_log_pkg.pFinish;
      pConsole(rPad('*** END', 10) || rPad(' ', GCI_LEN_ROWS) 
                 || CASE WHEN GD_ACTION_START Is Not Null Or adStart Is Not Null THEN
             ' ' || fcGetSeconds(Nvl(adStart, GD_ACTION_START)) END);
    else    
      ap_public.core_log_pkg.pEnd(acLogResult => 'End ' || acTable);   
    END IF;
  END;
  --  Step Error
  PROCEDURE pStepErr( fnEmailSend number   default 0,
                      AcModule   varchar2 default null) is
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN    
    AC_MODULE       := nvl(AcModule, AC_MODULE);
    
    ap_public.core_log_pkg.pError;
    pConsole('Err: ' || dbms_utility.format_error_stack || '~' ||
             dbms_utility.format_error_backtrace);
    
    IF fnEmailSend = 1 THEN         
    ap_it.mail_pkg.send(P_FROM    => AC_MODULE/*EMAIL_ADDRESS_FROM*/,
                        P_TO      => EMAIL_MZHUSSIYEV,
                        P_SUBJECT => 'ALERT! ORA-' ||
                                     to_char(sqlcode()) || GCV_ENDFORM ||
                                     GI_STEP_No || '. ' || AC_action,
                        P_BODY    => sqlerrm() || GCV_ENDFORM || GCV_ENDFORM ||
                                    'Err: ' || dbms_utility.format_error_stack || '~' ||
                                    dbms_utility.format_error_backtrace);
    END IF;
    
  END;
  ----------------------
  -- create GTT
  PROCEDURE pGttCreate(p_tab_name VARCHAR2, p_sql IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    e_tab_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_tab_does_not_exist, -00942);
    v_tmp_prefix VARCHAR2(8) := 'tbl_tmp_';
  BEGIN
    BEGIN
      pexec('TRUNCATE TABLE ' || v_tmp_prefix || p_tab_name);
      pexec('INSERT /*+APPEND*/ INTO ' || v_tmp_prefix || p_tab_name || ' ' ||
            p_sql);
      COMMIT;
    EXCEPTION
      WHEN e_tab_does_not_exist THEN
        --dbms_output.put_line('CREATE GLOBAL TEMPORARY TABLE tbl_tmp_' || p_tab_name ||
        --         ' ON COMMIT PRESERVE ROWS AS ' || p_sql);
        pexec('CREATE GLOBAL TEMPORARY TABLE tbl_tmp_' || p_tab_name ||
              ' ON COMMIT PRESERVE ROWS AS ' || p_sql);
    END;
    pstats(v_tmp_prefix || p_tab_name);
  END;
  -- Mview refresh
  PROCEDURE pMviewRefresh(acMview GST_ORA_NAME) IS
  BEGIN
  
    pTruncate(acMview);
    pExec('ALTER MATERIALIZED VIEW ' || acMview || ' compile');
  
    pAppInfo('pMviewRef', acMview || ' - ' || To_Char(SysDate, GCV_FMT_DATE));
    dbms_mview.refresh(acMview, 'c');
    --pStats(acMview);
  
  END;
  -------------------------------------------
  -- Log script
  PROCEDURE pLog( fTruncate    number default 0
                  ) IS  
                  
  TAB GT_MVIEW_NAME := GT_MVIEW_NAME(AC_MODULE);
  
  begin    
  
  if fTruncate != 0 then
    pTruncate('T_MZ_SAS_PACKAGE_LOG');
  end if;
  
    FOR I IN 1 .. TAB.COUNT LOOP
      ------- logging ----------------------
      INSERT /*APPEND*/
      INTO T_MZ_SAS_PACKAGE_LOG
        SELECT *
          FROM (select rownum as i,
                       t.log_pk,
                       T.LOG_PROCESS,
                       t.log_info,
                       /*T.LOG_RESULT,
                       T.DML_ROWS,
                       T.ERR_MSG,
                       T.ERR_STACK,*/
                       t.started,
                       t.finished,
                       (t.finished - t.started) * 24 * 60 * 60 time_
                  from AP_PUBLIC.CORE_LOG_DETAIL t
                  LEFT JOIN T_MZ_SAS_PACKAGE_LOG R
                    ON T.LOG_PK = R.LOG_PK
                 where t.aud_db_user = user
                   and t.log_process = TAB(I)
                   and t.work_day = trunc(sysdate)
                      --and t.log_info not in ('Started', 'End')
                   AND R.LOG_PK IS NULL
                   and t.run_id = (select max(d.run_id)
                                     from AP_PUBLIC.CORE_LOG_DETAIL d
                                    where d.aud_db_user = t.aud_db_user
                                      and d.log_process = t.log_process
                                      and d.work_day = t.work_day --trunc(sysdate))
                                   ))
         WHERE TIME_ IS NOT NULL
         ORDER BY LOG_PK;
      COMMIT;
    END LOOP;
  
  end;
  
  -------------------------------------------
  -- Mail script
  PROCEDURE pMail( pSubjects    GT_MVIEW_NAME2, pProcess varchar2 default 'REPORT RESULT') IS                    
  
  STEP_NO         INTEGER := 0;
  TOTAL_SEC       NUMBER  := 0;
  v_table_Start   VARCHAR2(20) := '<table border="2">';
  v_table_End     VARCHAR2(10) := '</table>';
  V_ROW_Start     VARCHAR2(10) := '<TR>';
  V_ROW_End       VARCHAR2(10) := '</TR>';
  V_Column_Start  VARCHAR2(10) := '<Th>';
  V_Column_End    VARCHAR2(10) := '</Th>';
  
  P_MESSAGE       GST_MAX_SQL_STR;                                            
  type row_type   is table of AP_PUBLIC.CORE_LOG_DETAIL%rowtype;
  row_tt          row_type;
  
  begin
    
  P_MESSAGE := v_table_Start || GCV_ENDFORM || GCV_ENDLINE;
      
  for i in 1 .. pSubjects.count loop
  
    select * bulk collect
      into row_tt    
      from AP_PUBLIC.CORE_LOG_DETAIL t
     where t.aud_db_user = user
       and t.log_process = pSubjects(i)
       and t.work_day = trunc(sysdate)
       and t.log_info not in ('Started', 'End')
       and t.run_id = (select max(d.run_id)
                         from AP_PUBLIC.CORE_LOG_DETAIL d
                        where d.aud_db_user = t.aud_db_user
                          and d.log_process = t.log_process
                          and d.work_day = t.work_day)
     order by 1;
    
    for e in row_tt.first .. row_tt.last loop
      step_no   := step_no + 1;
      TOTAL_SEC := TOTAL_SEC + (row_tt(e).finished - row_tt(e).started) * 24 * 60;
      
      P_MESSAGE := P_MESSAGE   	  || V_ROW_Start                                 ||
                   V_Column_Start || step_no                                     || V_Column_End ||
                   V_Column_Start || row_tt(e).work_day                          || V_Column_End ||
                   V_Column_Start || row_tt(e).log_process                       || V_Column_End ||
                   V_Column_Start || row_tt(e).log_info                          || V_Column_End ||
                   V_Column_Start || to_char(row_tt(e).started, 'hh24:mi:ss')    || V_Column_End ||
                   V_Column_Start || nvl(to_char(row_tt(e).err_msg),
                            to_char(row_tt(e).finished, 'hh24:mi:ss'))           || V_Column_End ||
                   V_Column_Start || trunc(((row_tt(e).finished - 
                            row_tt(e).started) * 24 * 60 * 60))                  || V_Column_End ||
                   V_ROW_End || PKG_MZ_HINTS.GCV_ENDFORM || pkg_mz_hints.GCV_ENDLINE;
    end loop;
  
  end loop;
      P_MESSAGE := P_MESSAGE || v_table_End || GCV_ENDFORM || GCV_ENDLINE;
      P_MESSAGE := P_MESSAGE || 'Total time (min) = ' || round(TOTAL_SEC, 2);
      
    dbms_output.put_line(P_MESSAGE);
    ap_it.mail_pkg.send(P_FROM    => EMAIL_ADDRESS_OUT,
                        P_TO      => EMAIL_MZHUSSIYEV,
                        P_SUBJECT => PPROCESS,
                        P_BODY    => P_MESSAGE);
  END;

END;
/
