Declare

  Cursor CUR Is
    Select 
       JOB_NAME
      ,ELAPSED_TIME
      ,V.SQL_EXEC_START
      ,V.LOGON_TIME
      , ROUND((SYSDATE - LOGON_TIME)*24*60,1) AS LOGON_DUR_MIN
      , ROUND((SYSDATE - SQL_EXEC_START)*24*60,1) AS SQL_DUR_MIN
  From USER_scheduler_running_jobs r
  LEFT JOIN V$SESSION V
    ON R.session_id = V.SID
 Where R.ELAPSED_TIME > INTERVAL '170' minute
   AND ROUND((SYSDATE - SQL_EXEC_START)*24*60,1) > 170
 ;

  RW CUR%Rowtype;
  EMAIL_ADDRESS_OUT varchar2(100) := 'AP_CRM_DWH@Homecredit.kz;';
  EMAIL_ADDRESS_IN VARCHAR2(4000) := 'MZhussiyev@Homecredit.kz;';
  v_table_Start  VARCHAR2(20) := '<table border="1">';
  v_table_End    VARCHAR2(10) := '</table>';
  V_ROW_Start    VARCHAR2(10) := '<TR>';
  V_ROW_End      VARCHAR2(10) := '</TR>';
  V_Column_Start VARCHAR2(50) := '<Th width="200" align="center">';
  V_Column_End   VARCHAR2(10) := '</Th>';
  GCV_ENDLINE    CONSTANT VARCHAR2(4) := CHR(13) || CHR(10);
  GCV_ENDFORM    CONSTANT VARCHAR2(4) := '<BR>';
  P_MESSAGE      PKG_MZ_HINTS.GST_MAX_PLS_STR;
  HOURS          NUMBER;
  MINUTES        NUMBER;
  V_NAME         VARCHAR2(100);

Begin
  P_MESSAGE := v_table_Start || GCV_ENDFORM || GCV_ENDLINE;
  P_MESSAGE := P_MESSAGE || 
               V_ROW_Start || 
               V_Column_Start || 'JOB_NAME     ' || V_Column_End || 
               V_Column_Start || 'RUNNING TIME ' || V_Column_End || 
               V_Column_Start || 'OWNER        ' || V_Column_End || 
               V_ROW_End || 
               GCV_ENDFORM ||
               GCV_ENDLINE;
                       
  Open CUR;
  Loop
    Fetch CUR
      Into RW;
    Exit When CUR%Notfound;
    
  begin
    DBMS_SCHEDULER.stop_job(job_name => RW.JOB_NAME);
    exception when others then
    DBMS_OUTPUT.put_line('SOME ERROR' || Sqlcode || Sqlerrm);
  end;
  
    HOURS   := EXTRACT(HOUR FROM RW.ELAPSED_TIME);
    MINUTES := EXTRACT(MINUTE FROM RW.ELAPSED_TIME);
    
    IF    RW.JOB_NAME LIKE '%\_MZ\_%' ESCAPE '\'  THEN V_NAME := 'MZhussiyev@Homecredit.kz'; 
    ELSIF RW.JOB_NAME LIKE '%\_NA\_%' ESCAPE '\'  THEN V_NAME := 'ANurpeisov@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_ATA\_%' ESCAPE '\' THEN V_NAME := 'ATobataev@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_ZB\_%'  ESCAPE '\' THEN V_NAME := 'ZBAZARBAYEVA@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_AA\_%'  ESCAPE '\' THEN V_NAME := 'AAptiyaliyev@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_BA\_%'  ESCAPE '\' THEN V_NAME := 'BAbdumalikov@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_ASH\_%' ESCAPE '\' THEN V_NAME := 'AShaibekova@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_MO\_%' ESCAPE '\'  THEN V_NAME := 'MOSSER@Homecredit.kz;';
    ELSIF RW.JOB_NAME LIKE '%\_DDM\_%' ESCAPE '\' THEN V_NAME := 'DDAUIT@Homecredit.kz;';
    END IF;
    
    IF EMAIL_ADDRESS_IN LIKE '%' || V_NAME || '%' THEN NULL; 
    ELSE 
       EMAIL_ADDRESS_IN := CONCAT(EMAIL_ADDRESS_IN, V_NAME);
    END IF;
    
    P_MESSAGE := P_MESSAGE || 
                 V_ROW_Start || 
                 V_Column_Start || RW.JOB_NAME     || V_Column_End || 
                 V_Column_Start || HOURS || 'h ' || MINUTES || 'min' || V_Column_End || 
                 V_Column_Start || V_NAME || V_Column_End || 
                 V_ROW_End || 
                 GCV_ENDFORM ||
                 GCV_ENDLINE;  
    
  End Loop;

  Close CUR;
    DBMS_OUTPUT.put_line(EMAIL_ADDRESS_OUT || GCV_ENDLINE || EMAIL_ADDRESS_IN || GCV_ENDLINE || P_MESSAGE);
    
    ap_it.mail_pkg.send(P_FROM    => EMAIL_ADDRESS_OUT,
                        P_TO      => EMAIL_ADDRESS_IN,
                        P_SUBJECT => 'JOB KILL STATISTICS',
                        P_BODY    => 'Dear friends, your jobs were killed due to long running time (more than 3h). 
                                      Please consider it in your work!' || 
                                     GCV_ENDFORM || GCV_ENDFORM || P_MESSAGE);
End;
