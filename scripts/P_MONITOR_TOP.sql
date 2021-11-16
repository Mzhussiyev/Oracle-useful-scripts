PROCEDURE P_MONITOR_TOP( anTopN INTEGER DEFAULT 50, anDeleteOlderThan INTEGER DEFAULT 2 )
  --AUTHID DEFINER
  AUTHID CURRENT_USER
  IS
  --
  --  Store information from SQL area to table for future check)
  --
  OUTPUT_T_NAME CONSTANT VARCHAR2(30) := 'T_MONITOR_TOP' ;
  --
  lcQuery VARCHAR2(32000) ;
  EX_INVALID_IDENTIFIER EXCEPTION;
    PRAGMA Exception_Init( EX_INVALID_IDENTIFIER, -904 );
  EX_TOO_MANY_VALUES EXCEPTION;
    PRAGMA Exception_Init( EX_TOO_MANY_VALUES, -913 );
  EX_TABLE_DOESNT_EXIST EXCEPTION;
    PRAGMA Exception_Init( EX_TABLE_DOESNT_EXIST, -942 );
  --
  PROCEDURE pCreateTable
    IS
    lcCmd VARCHAR2(32000) ;
  BEGIN
    DBMS_OUTPUT.Put_Line( '**Error on insert:') ;
    DBMS_OUTPUT.Put_Line( SqlErrM ) ;
    DBMS_OUTPUT.Put_Line( lcCmd ) ;
    BEGIN
      BEGIN
        lcCmd :=  'DROP TABLE '||OUTPUT_T_NAME ;
        EXECUTE IMMEDIATE lcCmd ;
        DBMS_OUTPUT.Put_Line( 'Dropped' ) ;
      EXCEPTION WHEN Others THEN
        DBMS_OUTPUT.Put_Line( 'Drop '||SqlErrM ) ;
      END ;
      lcCmd :=  'CREATE TABLE '||OUTPUT_T_NAME||' PCTFREE 0 COMPRESS FOR ALL OPERATIONS '||
        /*'PARTITION BY RANGE (D_SNAPSHOTED) INTERVAL ( NumToDsInterval( 1, ''DAY'' ) )'||
        ' ( PARTITION P_INIT VALUES LESS THAN (DATE ''2010-01-01'') ) '||*/' AS '||lcQuery ;
      EXECUTE IMMEDIATE lcCmd ;
      DBMS_OUTPUT.Put_Line( 'Created '||SQL%RowCount ) ;
    EXCEPTION WHEN Others THEN
      DBMS_OUTPUT.Put_Line( '**Error on create:') ;
      DBMS_OUTPUT.Put_Line( SqlErrM ) ;
      DBMS_OUTPUT.Put_Line( lcCmd ) ;
      RAISE ;
    END ;
  END ;
  --
  PROCEDURE pInsertData
    IS
    lcCmd VARCHAR2(32000) ;
  BEGIN
    lcCmd :=  'DELETE '||OUTPUT_T_NAME||' WHERE D_SNAPSHOTED < Trunc( SysDate -:anDeleteOlderThan)' ;
    EXECUTE IMMEDIATE lcCmd USING anDeleteOlderThan;
    --lcCmd :=  'INSERT INTO '||OUTPUT_T_NAME||' '||lcQuery ;
    lcCmd :=  'MERGE INTO '||OUTPUT_T_NAME||' T USING ( '||lcQuery||' ) S '||q'[
    ON ( T.II = S.II And T.SQL_ID = S.SQL_ID )
    WHEN MATCHED THEN
      UPDATE SET
          T.CMDT              = S.CMDT
        , T.EXECS             = S.EXECS
        , T.RWS               = S.RWS
        , T.BUFG_T            = S.BUFG_T
        , T.BUFG#T            = S.BUFG#T
        , T.BUFG_1            = S.BUFG_1
        , T.BUFG#1            = S.BUFG#1
        , T.BUFG_D            = S.BUFG_D
        , T.BUFG#D            = S.BUFG#D
        , T.DSKR_T            = S.DSKR_T
        , T.DSKR#T            = S.DSKR#T
        , T.DSKR_1            = S.DSKR_1
        , T.DSKR#1            = S.DSKR#1
        , T.DSKR_D            = S.DSKR_D
        , T.DSKR#D            = S.DSKR#D
        , T.ELAT_T            = S.ELAT_T
        , T.ELAT#T            = S.ELAT#T
        , T.ELAT_1            = S.ELAT_1
        , T.ELAT#1            = S.ELAT#1
        , T.ELAT_D            = S.ELAT_D
        , T.ELAT#D            = S.ELAT#D
        , T.CPUT_T            = S.CPUT_T
        , T.CPUT#T            = S.CPUT#T
        , T.CPUT_1            = S.CPUT_1
        , T.CPUT#1            = S.CPUT#1
        , T.CPUT_D            = S.CPUT_D
        , T.CPUT#D            = S.CPUT#D
        , T.FIRST_LOAD_TIME   = S.FIRST_LOAD_TIME
        , T.LAST_LOAD_TIME    = S.LAST_LOAD_TIME
        , T.DAYS              = S.DAYS
        , T.PARS_SCHEMA       = S.PARS_SCHEMA
        , T.MODULE            = S.MODULE
        , T.ACTION            = S.ACTION
        , T.PROGRAM_ID        = S.PROGRAM_ID
        , T.PROGRAM_LINE#     = S.PROGRAM_LINE#
        , T.TOPN$             = S.TOPN$
        , T.D_SNAPSHOTED      = S.D_SNAPSHOTED
        --  ON columns
        , T.LAST_ACTIVE_TIME  = S.LAST_ACTIVE_TIME

        --, T.II                = S.II
        --, T.SQL_ID            = S.SQL_ID
    WHEN NOT MATCHED THEN
      INSERT
      ( T.II, T.SQL_ID, T.PLAN_HASH_VALUE, T.CMDT, T.EXECS, T.RWS
      , T.BUFG_T, T.BUFG#T, T.BUFG_1, T.BUFG#1, T.BUFG_D, T.BUFG#D
      , T.DSKR_T, T.DSKR#T, T.DSKR_1, T.DSKR#1, T.DSKR_D, T.DSKR#D
      , T.ELAT_T, T.ELAT#T, T.ELAT_1, T.ELAT#1, T.ELAT_D, T.ELAT#D
      , T.CPUT_T, T.CPUT#T, T.CPUT_1, T.CPUT#1, T.CPUT_D, T.CPUT#D
      , T.FIRST_LOAD_TIME, T.LAST_LOAD_TIME, T.LAST_ACTIVE_TIME
      , T.DAYS, T.PARS_SCHEMA, T.MODULE, T.ACTION
      , T.PROGRAM_ID, T.PROGRAM_LINE#, T.TOPN$, T.D_SNAPSHOTED
      )
      VALUES
      ( S.II, S.SQL_ID, S.PLAN_HASH_VALUE, S.CMDT, S.EXECS, S.RWS
      , S.BUFG_T, S.BUFG#T, S.BUFG_1, S.BUFG#1, S.BUFG_D, S.BUFG#D
      , S.DSKR_T, S.DSKR#T, S.DSKR_1, S.DSKR#1, S.DSKR_D, S.DSKR#D
      , S.ELAT_T, S.ELAT#T, S.ELAT_1, S.ELAT#1, S.ELAT_D, S.ELAT#D
      , S.CPUT_T, S.CPUT#T, S.CPUT_1, S.CPUT#1, S.CPUT_D, S.CPUT#D
      , S.FIRST_LOAD_TIME, S.LAST_LOAD_TIME, S.LAST_ACTIVE_TIME
      , S.DAYS, S.PARS_SCHEMA, S.MODULE, S.ACTION
      , S.PROGRAM_ID, S.PROGRAM_LINE#, S.TOPN$, S.D_SNAPSHOTED
      )]' ;
    EXECUTE IMMEDIATE lcCmd ;
    DBMS_OUTPUT.Put_Line( 'Inserted '||SQL%RowCount ) ;
    COMMIT ;
  EXCEPTION
    WHEN EX_INVALID_IDENTIFIER  THEN
      pCreateTable ;
    WHEN EX_TOO_MANY_VALUES     THEN
      pCreateTable ;
    WHEN EX_TABLE_DOESNT_EXIST  THEN
      pCreateTable ;
  END ;
  --
BEGIN
  lcQuery :=  q'[SELECT  INST_ID II, SQL_ID, PLAN_HASH_VALUE, CMDT, EXECS, RWS
      , BUFG_T, BUFG#T, BUFG_1, BUFG#1, Round( BUFG_T/DAYS ) BUFG_D, BUFG#D
      , DSKR_T, DSKR#T, DSKR_1, DSKR#1, Round( DSKR_T/DAYS ) DSKR_D, DSKR#D
      , ELAT_T, ELAT#T, ELAT_1, ELAT#1, Round( ELAT_T/DAYS ) ELAT_D, ELAT#D
      , CPUT_T, CPUT#T, CPUT_1, CPUT#1, Round( CPUT_T/DAYS ) CPUT_D, CPUT#D
      --, UIOT_T, UIOT#T, UIOT_1, UIOT#1, Round( UIOT_T/DAYS ) UIOT_D, UIOT#D
      --, SRTS_T, SRTS#T, SRTS_1, SRTS#1, Round( SRTS_T/DAYS ) SRTS_D, SRTS#D
      , FIRST_LOAD_TIME, LAST_LOAD_TIME, LAST_ACTIVE_TIME, DAYS
      , PARS_SCHEMA, MODULE, ACTION, PROGRAM_ID, PROGRAM_LINE#
      , TOPN$, SysDate D_SNAPSHOTED
  FROM
  ( SELECT  S.*
          --  Buffer gets
          , Row_Number() OVER ( ORDER BY BUFG_T       DESC Nulls Last ) BUFG#T
          , Row_Number() OVER ( ORDER BY BUFG_1       DESC Nulls Last ) BUFG#1
          , Row_Number() OVER ( ORDER BY BUFG_T/DAYS  DESC Nulls Last ) BUFG#D
          --  Disk reads
          , Row_Number() OVER ( ORDER BY DSKR_T       DESC Nulls Last ) DSKR#T
          , Row_Number() OVER ( ORDER BY DSKR_1       DESC Nulls Last ) DSKR#1
          , Row_Number() OVER ( ORDER BY DSKR_T/DAYS  DESC Nulls Last ) DSKR#D
          --  Elapsed time
          , Row_Number() OVER ( ORDER BY ELAT_T       DESC Nulls Last ) ELAT#T
          , Row_Number() OVER ( ORDER BY ELAT_1       DESC Nulls Last ) ELAT#1
          , Row_Number() OVER ( ORDER BY ELAT_T/DAYS  DESC Nulls Last ) ELAT#D
          --  CPU time
          , Row_Number() OVER ( ORDER BY CPUT_T       DESC Nulls Last ) CPUT#T
          , Row_Number() OVER ( ORDER BY CPUT_1       DESC Nulls Last ) CPUT#1
          , Row_Number() OVER ( ORDER BY CPUT_T/DAYS  DESC Nulls Last ) CPUT#D
          --  User I/O wait time
          --, Row_Number() OVER ( ORDER BY UIOT_T       DESC Nulls Last ) UIOT#T
          --, Row_Number() OVER ( ORDER BY UIOT_1       DESC Nulls Last ) UIOT#1
          --, Row_Number() OVER ( ORDER BY UIOT_T/DAYS  DESC Nulls Last ) UIOT#D
          --  Sorts
          --, Row_Number() OVER ( ORDER BY SRTS_T       DESC Nulls Last ) SRTS#T
          --, Row_Number() OVER ( ORDER BY SRTS_1       DESC Nulls Last ) SRTS#1
          --, Row_Number() OVER ( ORDER BY SRTS_T/DAYS  DESC Nulls Last ) SRTS#D
      FROM
      ( SELECT  1 INST_ID, SQA.SQL_ID, SQA.PLAN_HASH_VALUE, SQA.COMMAND_TYPE CMDT, SQA.EXECUTIONS EXECS, SQA.ROWS_PROCESSED RWS
              , SQA.MODULE, SQA.ACTION, SQA.PARSING_SCHEMA_NAME PARS_SCHEMA, SQA.PROGRAM_ID, SQA.PROGRAM_LINE#
              , SQA.BUFFER_GETS       BUFG_T, Round( SQA.BUFFER_GETS        / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) BUFG_1
              , SQA.DISK_READS        DSKR_T, Round( SQA.DISK_READS         / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) DSKR_1
              , SQA.ELAPSED_TIME      ELAT_T, Round( SQA.ELAPSED_TIME       / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) ELAT_1
              , SQA.CPU_TIME          CPUT_T, Round( SQA.CPU_TIME           / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) CPUT_1
              --, SQA.USER_IO_WAIT_TIME UIOT_T, Round( SQA.USER_IO_WAIT_TIME  / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) UIOT_1
              --, SQA.SORTS             SRTS_T, Round( SQA.SORTS              / Decode( SQA.EXECUTIONS,0,1, SQA.EXECUTIONS)  ) SRTS_1
              , To_Date( FIRST_LOAD_TIME, 'YYYY-MM-DD/HH24:Mi:SS') FIRST_LOAD_TIME
              --, To_Date( LAST_LOAD_TIME , 'YYYY-MM-DD/HH24:Mi:SS') LAST_LOAD_TIME
              , LAST_LOAD_TIME
              , LAST_ACTIVE_TIME
              , Decode( SQA.EXECUTIONS,0,1,Greatest(1,Ceil(SQA.LAST_ACTIVE_TIME - To_Date( SQA.FIRST_LOAD_TIME,'YYYY-MM-DD HH24:Mi:SS')))) DAYS
              , STP.TOPN$

          FROM V$SQLAREA SQA CROSS JOIN ( SELECT ]'||anTopN||q'[ TOPN$ FROM DUAL ) STP

          WHERE SQA.COMMAND_TYPE Not In ( 47, 170 )
            And SQA.PARSING_SCHEMA_NAME NOT IN ('SYS')
      ) S
  ) S
  WHERE BUFG#T <= TOPN$ Or BUFG#1 <= TOPN$ Or BUFG#D <= TOPN$
    Or  DSKR#T <= TOPN$ Or DSKR#1 <= TOPN$ Or DSKR#D <= TOPN$
    Or  ELAT#T <= TOPN$ Or ELAT#1 <= TOPN$ Or ELAT#D <= TOPN$
    Or  CPUT#T <= TOPN$ Or CPUT#1 <= TOPN$ Or CPUT#D <= TOPN$]' ;

  pInsertData ;
END ;
