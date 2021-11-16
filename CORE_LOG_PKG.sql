
  CREATE OR REPLACE PACKAGE "AP_PUBLIC"."CORE_LOG_PKG" 
  IS

  -- Author  : MILOS.BENO
  -- Created : 2016-04-04 09:26:37
  -- Purpose : Logging
  B_MODIFY_SESS_INFO  BOOLEAN := True ;
  C_FMT_LOG_ID        VARCHAR2(10) := 'FM000' ;

  PROCEDURE pInit ( acLogModule   CORE_LOG_DETAIL.LOG_MODULE%Type  DEFAULT 'DEFAULT'
                  , acLogProcess  CORE_LOG_DETAIL.LOG_PROCESS%Type DEFAULT 'DEFAULT'
                  , adWorkDate    CORE_LOG_DETAIL.WORK_DAY%Type    DEFAULT Trunc( SysDate )
                  ) ;
  PROCEDURE pNote ( acLogInfo   CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  , acLogId     CORE_LOG_DETAIL.LOG_ID%Type     DEFAULT Null
                  ) ;
  PROCEDURE pStart( acLogInfo   CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null
                  , acLogId     CORE_LOG_DETAIL.LOG_ID%Type     DEFAULT Null
                  ) ;
  PROCEDURE pEnd  ( anDmlRows   CORE_LOG_DETAIL.DML_ROWS%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  ) ;
  PROCEDURE pError( anDmlRows   CORE_LOG_DETAIL.DML_ROWS%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  ) ;
  PROCEDURE pFinish( acLogInfo  CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null) ;
END CORE_LOG_PKG;


CREATE OR REPLACE PACKAGE BODY "AP_PUBLIC"."CORE_LOG_PKG" 
  IS

  C_LOG_MODULE  CORE_LOG_DETAIL.LOG_MODULE%Type  := User ;
  C_LOG_PROCESS CORE_LOG_DETAIL.LOG_PROCESS%Type := 'DEFAULT' ;
  D_WORK_DAY    CORE_LOG_DETAIL.WORK_DAY%Type    := Trunc( SysDate ) ;
  N_RUN_ID      CORE_LOG_DETAIL.RUN_ID%Type ;
  --
  N_LOG_ID      INTEGER := 0 ;
  gLogRowId     ROWID   ;

  PROCEDURE pInsert( aRec CORE_LOG_DETAIL%RowType )
    IS
    PRAGMA AUTONOMOUS_TRANSACTION ;
  BEGIN
    IF N_RUN_ID Is Null THEN
      N_RUN_ID := CORE_LOG_DETAIL_RUN_SQ.NextVal ;
    END IF ;
    INSERT INTO AP_PUBLIC.CORE_LOG_DETAIL
        ( LOG_PK      
        , RUN_ID
        , WORK_DAY
        , LOG_MODULE  
        , LOG_PROCESS
        , LOG_ID      
        , LOG_INFO
        , FINISHED    
        , LOG_RESULT
        )
      VALUES
        ( CORE_LOG_DETAIL_SQ.NextVal      
        , N_RUN_ID
        , Nvl( aRec.Work_Day    , D_WORK_DAY) 
        , Nvl( aRec.Log_Module  , C_LOG_MODULE )  
        , Nvl( aRec.Log_Process , C_LOG_PROCESS )
        , aRec.Log_Id                     
        , aRec.Log_Info
        , aRec.Finished                   
        , aRec.Log_Result
        )
      RETURNING ROWID INTO gLogRowId ;
    COMMIT ;
  END pInsert ;

  PROCEDURE pUpdate( aRec CORE_LOG_DETAIL%RowType )
    IS
    PRAGMA AUTONOMOUS_TRANSACTION ;
    lnDmlRow INTEGER := SQL%RowCount ;
  BEGIN
    IF  gLogRowId Is Null THEN
      Raise_Application_Error(-20000
          , 'Use method for start before for end [RowId is Null].') ;
    END IF ;
    UPDATE CORE_LOG_DETAIL L
      SET
          L.FINISHED    = aRec.Finished
        , L.DML_ROWS    = Nvl( aRec.Dml_Rows, lnDmlRow )
        , L.LOG_RESULT  = aRec.Log_Result
        , L.COMMAND     = aRec.Command
        , L.ERR_CODE    = aRec.Err_Code
        , L.ERR_MSG     = aRec.Err_Msg
        , L.ERR_STACK   = aRec.Err_Stack
      WHERE L.ROWID = gLogRowId ;
    COMMIT ;
    gLogRowId := Null ;
  END pUpdate ;

  PROCEDURE pAutoEnd
    IS
    lRec CORE_LOG_DETAIL%RowType ;
  BEGIN
    IF gLogRowId Is Not Null THEN
      lRec.Finished   := SysDate ;
      lRec.Log_Result := 'Auto ended' ;
      pUpdate( lRec ) ;
    END IF ;
  END pAutoEnd ;

  FUNCTION fCheckLogIdArgument( acLogId CORE_LOG_DETAIL.LOG_ID%Type )
    RETURN CORE_LOG_DETAIL.LOG_ID%Type
    IS
  BEGIN
    IF acLogId Is Not Null THEN
      RETURN acLogId ;
    END IF ;
    N_LOG_ID  := N_LOG_ID + 1 ;
    RETURN '#'||To_Char( N_LOG_ID, Nvl( C_FMT_LOG_ID, 'FM000') );
  END fCheckLogIdArgument ;

  PROCEDURE pInit ( acLogModule   CORE_LOG_DETAIL.LOG_MODULE%Type  DEFAULT 'DEFAULT'
                  , acLogProcess  CORE_LOG_DETAIL.LOG_PROCESS%Type DEFAULT 'DEFAULT'
                  , adWorkDate    CORE_LOG_DETAIL.WORK_DAY%Type    DEFAULT Trunc( SysDate )
                  )
    IS
  BEGIN
    C_LOG_MODULE  := acLogModule ;
    C_LOG_PROCESS := acLogProcess ;
    D_WORK_DAY    := adWorkDate ;
    N_RUN_ID      := CORE_LOG_DETAIL_RUN_SQ.NextVal ;
    N_LOG_ID      := 0 ;
    pNote( 'Started' ) ;
    gLogRowId := Null ;
    IF B_MODIFY_SESS_INFO THEN
      DBMS_APPLICATION_INFO.Set_Action
        ( To_Char( SysDate,'YYYY-MM-DD HH24:Mi:SS' )||' '||
          C_LOG_MODULE||'/'||C_LOG_PROCESS ) ;
    END IF ;
  END pInit ;

  PROCEDURE pNote ( acLogInfo   CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  , acLogId     CORE_LOG_DETAIL.LOG_ID%Type     DEFAULT Null
                  )
    IS
    lRec CORE_LOG_DETAIL%RowType ;
  BEGIN
    pAutoEnd ;
    lRec.Log_Info   := acLogInfo ;
    lRec.Log_Result := acLogResult ;
    lRec.Log_Id     := fCheckLogIdArgument( acLogId ) ;
    lRec.Finished   := SysDate  ;
    pInsert( lRec ) ;
  END pNote;

  PROCEDURE pStart( acLogInfo   CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null
                  , acLogId     CORE_LOG_DETAIL.LOG_ID%Type     DEFAULT Null
                  )
    IS
    lRec CORE_LOG_DETAIL%RowType ;
  BEGIN
    pAutoEnd ;
    lRec.Log_Info   := acLogInfo ;
    lRec.Log_Id     := fCheckLogIdArgument( acLogId ) ;
    pInsert( lRec ) ;
    IF B_MODIFY_SESS_INFO THEN
      DBMS_APPLICATION_INFO.Set_Client_Info
        ( To_Char( SysDate,'YYYY-MM-DD HH24:Mi:SS' )||' '||
          lRec.Log_Id||'/'||lRec.Log_Info ) ;
    END IF ;
  END pStart ;

  PROCEDURE pEnd  ( anDmlRows   CORE_LOG_DETAIL.DML_ROWS%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  )
    IS
    lRec CORE_LOG_DETAIL%RowType ;
  BEGIN
    lRec.Finished   := SysDate  ;
    lRec.Dml_Rows   := anDmlRows ;
    lRec.Log_Result := acLogResult ;
    lRec.Command    := Null ;
    lRec.Err_Code   := SqlCode ;
    lRec.Err_Msg    := CASE WHEN SqlCode != 0 THEN SqlErrM END ;
    lRec.Err_Stack  := Null ;
    pUpdate( lRec ) ;
  END pEnd ;

  PROCEDURE pError( anDmlRows   CORE_LOG_DETAIL.DML_ROWS%Type   DEFAULT Null
                  , acLogResult CORE_LOG_DETAIL.LOG_RESULT%Type DEFAULT Null
                  )
    IS
    lRec CORE_LOG_DETAIL%RowType ;
  BEGIN
    lRec.Finished   := SysDate  ;
    lRec.Dml_Rows   := anDmlRows ;
    lRec.Log_Result := acLogResult ;
    lRec.Command    := Null ;
    lRec.Err_Code   := SqlCode ;
    lRec.Err_Msg    := SqlErrM ;
    lRec.Err_Stack  := DBMS_UTILITY.Format_Error_BackTrace() ;
    pUpdate( lRec ) ;
  END pError ;

  PROCEDURE pFinish( acLogInfo CORE_LOG_DETAIL.LOG_INFO%Type   DEFAULT Null )
    IS
  BEGIN
    pAutoEnd ;
    pNote( acLogInfo   => Nvl( acLogInfo, 'End' )
         , acLogResult => Null
         , acLogId     => Null
         ) ;
  gLogRowId := Null ;
  END pFinish ;
BEGIN
  Null ;
END CORE_LOG_PKG;

