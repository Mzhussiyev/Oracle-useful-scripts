CREATE OR REPLACE PACKAGE AP_CRM_ABT.PKG_MZ_ABT_DATAMART_SALE authid current_user is


PROCEDURE P_RESPONSE_CASH_CLIENT_MERGE( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'));
PROCEDURE P_RESPONSE_ELIGIBILITY_MERGE( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 );
PROCEDURE P_RESPONSE_CASH_APPL_MERGE  ( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 );
PROCEDURE P_RESPONSE_CASH_OFFER_MERGE ( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 );



PROCEDURE P_RESPONSE_CARD_CLIENT_MERGE;
PROCEDURE P_RESPONSE_CARD_APPL_MERGE  ( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 );
PROCEDURE P_RESPONSE_CARD_OFFER_MERGE ( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 );
                                        
                                        
PROCEDURE P_MAIN(MONTH_ DATE, cnt_month_ number);                                        


END;
/
CREATE OR REPLACE PACKAGE BODY AP_CRM_ABT.PKG_MZ_ABT_DATAMART_SALE IS


  PROCEDURE P_RESPONSE_CASH_CLIENT_MERGE(MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM')) IS
  
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_CLIENT_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(4);
    
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
    
    -- Step 1 Start -------------------------------
    i_step := i_step + 1;
     
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'T_MZ_CLIENT_ABT_ATTR_CLIENT_MERGE');
      insert 
        into T_MZ_CLIENT_ABT_ATTRIBUTES t
             (skp_client, month_)
             
      SELECT a.skp_client, a.month_  AS month_
        FROM T_ABT_CASH_DATAMART A
        left join T_MZ_CLIENT_ABT_ATTRIBUTES t
          on t.skp_client = a.skp_client
         and t.month_ = a.month_
       where a.month_ > add_months(MONTH_START, -1)
         and a.skp_client > 0
         and t.skp_client is null;
    --                   
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                          calcStats    => 1);
    --PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES');  
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      --PKG_MZ_HINTS.pStepErr(fnEmailSend => 1);
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  END;
  
  
  
  
  
  
  
  PROCEDURE P_RESPONSE_ELIGIBILITY_MERGE( MONTH_START    DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                          CNT_MONTH_CALC NUMBER DEFAULT 1 ) IS
    
  CUR_MONTH DATE	         := MONTH_START;
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_ELIGIBILITY_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(8);    
    -- Start Init Log ---------------------------
  PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
  
  
  FOR I IN 1..CNT_MONTH_CALC LOOP
        
  I_STEP := I_STEP + 1;
  PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                          acAction  => 'ABT_ATTR_ELIG_MERGE ' || CUR_MONTH);
  
    
   For i In (Select p.high_value
                  , lead(o.DATA_OBJECT_ID) over(Order By p.partition_position) As DATA_OBJECT_ID
                  From user_tab_partitions p
                  join user_objects o
                    on o.SUBOBJECT_NAME = p.partition_name
                   and o.OBJECT_NAME    = p.table_name
                 Where p.table_name = 'T_MZ_CLIENT_ABT_ATTRIBUTES') Loop
      
    If i.high_value Like '%'||to_char(CUR_MONTH, 'mm-dd')||'%' Then
      
    
    
  ---- Months after DATE'2018-12-01' we take from  AP_RISK.OB_ELIGIBILITY_HIST
  IF CUR_MONTH >= DATE'2018-12-01' THEN                            
    
    merge into T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) t
    using (
      with ELIG AS
       (SELECT /*+ MATERIALIZE FULL(H)*/
         H.SKP_CLIENT,
         H.NUM_CONTRACT_ACTIVE,
         H.RISK_GRADE,
         H.FLAG_ELIGIBILITY,
         H.FLAG_ELIGIBILITY_XSELL,
         H.FLAG_ELIGIBILITY_CE,
         TRUNC(H.DATE_EFFECTIVE, 'MM') AS MONTH_EFFECTIVE,
         LEAD(H.DATE_EFFECTIVE,1,SYSDATE) OVER(PARTITION BY SKP_CLIENT ORDER BY H.DTIME_INSERTED) AS DATE_EFFECTIVE_NEXT
        
          FROM AP_RISK.OB_ELIGIBILITY_HIST H
         WHERE DATE_EFFECTIVE BETWEEN ADD_MONTHS(CUR_MONTH, -2) AND ADD_MONTHS(CUR_MONTH, 1)
        )
         
      SELECT /*+ USE_HASH(ABT ELIG)*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,
       MAX(nvl(ELIG.FLAG_ELIGIBILITY, 0))      AS FLAG_ELIGIBILITY,
       MAX(nvl(ELIG.FLAG_ELIGIBILITY_XSELL, 0))AS FLAG_ELIGIBILITY_XSELL,
       MAX(NVL(ELIG.FLAG_ELIGIBILITY_CE, 0))   AS FLAG_ELIGIBILITY_CE,
       MAX(NVL(ELIG.NUM_CONTRACT_ACTIVE, 0))   AS NUM_CONTRACT_ACTIVE,
       MIN(ELIG.RISK_GRADE)                    AS RISK_GRADE
      
        FROM T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) ABT
        LEFT JOIN ELIG
          ON ABT.SKP_CLIENT = ELIG.SKP_CLIENT
         AND ABT.MONTH_ BETWEEN ELIG.MONTH_EFFECTIVE AND
             NVL(ELIG.DATE_EFFECTIVE_NEXT, SYSDATE)
         
       WHERE ABT.MONTH_ = CUR_MONTH
       group by ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.MONTH_ = S.MONTH_  AND T.SKP_CLIENT = S.SKP_CLIENT) WHEN MATCHED THEN
        update
           set t.FLAG_ELIGIBILITY       = s.FLAG_ELIGIBILITY,
               t.FLAG_ELIGIBILITY_XSELL = s.FLAG_ELIGIBILITY_XSELL,
               t.FLAG_ELIGIBILITY_CE    = s.FLAG_ELIGIBILITY_CE,
               t.NUM_CONTRACT_ACTIVE    = s.NUM_CONTRACT_ACTIVE,
               t.RISK_GRADE             = s.RISK_GRADE
               
         WHERE T.MONTH_ = CUR_MONTH
           AND (nvl(T.FLAG_ELIGIBILITY, '1') != nvl(S.FLAG_ELIGIBILITY, '1')
            OR nvl(T.FLAG_ELIGIBILITY_XSELL, '1') != nvl(S.FLAG_ELIGIBILITY_XSELL, '1')
            OR nvl(T.FLAG_ELIGIBILITY_CE, '1') != nvl(S.FLAG_ELIGIBILITY_CE, '1')
            OR nvl(T.NUM_CONTRACT_ACTIVE, 0) != nvl(S.NUM_CONTRACT_ACTIVE, 0)
            OR nvl(T.RISK_GRADE, '1') != nvl(S.RISK_GRADE, '1')
             );               
    
    ---- Months Before DATE'2018-12-01' we take from ap_crm.T_SELECTION_ELIGIBILITY
    /*IF CUR_MONTH < DATE'2018-12-01' THEN*/
    ELSE
    
    merge into T_MZ_CLIENT_ABT_ATTRIBUTES t
    using (
      with XSELL AS
       (SELECT /*+ MATERIALIZE */       
         EL.SKP_CLIENT,
         MAX(EL.XSELL_ELIG) AS XSELL_ELIG
         
          FROM ap_crm.T_SELECTION_ELIGIBILITY EL          
         WHERE EL.INS_DATE BETWEEN ADD_MONTHS(CUR_MONTH, 0) AND ADD_MONTHS(CUR_MONTH, 1)
         GROUP BY EL.SKP_CLIENT
         ),         
       CE AS
       (SELECT /*+ MATERIALIZE*/         
         CE.SKP_CLIENT,
         MAX(CE.CE_ELIG)    AS CE_ELIG
         
          FROM ap_crm.T_SELECTION_ELIGIBILITY_CE CE

         WHERE CE.DTIME_INSERTED BETWEEN ADD_MONTHS(CUR_MONTH, 0) AND ADD_MONTHS(CUR_MONTH, 1)
         GROUP BY CE.SKP_CLIENT
         )
         
      SELECT /*+ USE_HASH(ABT ELIG2)*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,
       MAX(GREATEST(nvl(XSELL.XSELL_ELIG, 0), nvl(CE.CE_ELIG, 0))) AS FLAG_ELIGIBILITY,
       MAX(nvl(XSELL.XSELL_ELIG, 0))                               AS FLAG_ELIGIBILITY_XSELL,
       MAX(nvl(DECODE(XSELL.XSELL_ELIG, 1, NULL, CE.CE_ELIG), 0))  AS FLAG_ELIGIBILITY_CE
      
        FROM T_MZ_CLIENT_ABT_ATTRIBUTES ABT
        LEFT JOIN XSELL
          ON ABT.SKP_CLIENT = XSELL.SKP_CLIENT
        LEFT JOIN CE
          ON ABT.SKP_CLIENT = CE.SKP_CLIENT
         
       WHERE ABT.MONTH_ = CUR_MONTH
       group by ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.SKP_CLIENT = S.SKP_CLIENT AND T.MONTH_ = S.MONTH_) 
        WHEN MATCHED THEN
      update
         set t.FLAG_ELIGIBILITY       = s.FLAG_ELIGIBILITY,
             t.FLAG_ELIGIBILITY_XSELL = s.FLAG_ELIGIBILITY_XSELL,
             t.FLAG_ELIGIBILITY_CE    = S.FLAG_ELIGIBILITY_CE
               
       WHERE T.MONTH_ = CUR_MONTH
         AND (nvl(T.FLAG_ELIGIBILITY, '1')      != nvl(S.FLAG_ELIGIBILITY, '1')
          OR nvl(T.FLAG_ELIGIBILITY_XSELL, '1') != nvl(S.FLAG_ELIGIBILITY_XSELL, '1')
          OR NVL(T.FLAG_ELIGIBILITY_CE, '1')    != NVL(S.FLAG_ELIGIBILITY_CE, '1')
             );
           
    END IF;
    
    END IF;
    END LOOP;
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                          calcStats    => 0);
    COMMIT;
    
    
    
    
    CUR_MONTH := ADD_MONTHS(CUR_MONTH, 1);
    EXIT WHEN CUR_MONTH > TRUNC(SYSDATE, 'MM');
    END LOOP;
    
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES');    
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      --PKG_MZ_HINTS.pStepErr(fnEmailSend => 1);
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  end;
  
  
  
  PROCEDURE P_RESPONSE_CASH_APPL_MERGE( MONTH_START    DATE   DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 ) IS
    
  CUR_MONTH DATE	         := MONTH_START;
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_APPLICATION_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(8);  

    -- Start Init Log ---------------------------
  PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
  
  
  FOR I IN 1..CNT_MONTH_CALC LOOP
    
  
  I_STEP := I_STEP + 1;     
  PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                          acAction  => 'ABT_ATTR_APPL_MERGE ' || CUR_MONTH);
  
   For i In (Select p.high_value
                  , lead(o.DATA_OBJECT_ID) over(Order By p.partition_position) As DATA_OBJECT_ID
                  From user_tab_partitions p
                  join user_objects o
                    on o.SUBOBJECT_NAME = p.partition_name
                   and o.OBJECT_NAME    = p.table_name
                 Where p.table_name = 'T_MZ_CLIENT_ABT_ATTRIBUTES') Loop
      
    If i.high_value Like '%'||to_char(CUR_MONTH, 'mm-dd')||'%' Then
      
     
    merge into T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) t
    using (
      with appl as
       (SELECT /*+ MATERIALIZE FULL(S)*/
         s.skp_client,
         s.dtime_proposal,
         TRUNC(S.dtime_proposal, 'MM') AS MONTH_DECISION,
         s.code_product_type,
         s.code_product,
         1 AS FLAG_APPLICATION,
         s.flag_approved,
         s.flag_booked,
         s.flag_box_insurance,
         s.flag_life_insurance,
         s.is_refinance,
         s.is_remote,
         s.amt_credit_total,
         s.amt_credit_request,
         s.amt_box_ins,
         s.amt_life_ins,
         s.cnt_instalment,
         s.rate_interest
        
          FROM AP_CRM.T_AA_NEW_ALL_APPLICATIONS S
         where s.date_decision between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1) + 15
           AND s.dtime_proposal between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1)
           and s.product_channel_general in ('CASH X-SELL'/*, 'REFINANCE'*/))
      
      SELECT /*+ USE_HASH(ABT APPL) NO_INDEX(ABT UDX_MZ_ABT_CL_SKPCL_MONTH )*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,
       
       MAX(NVL(APPL.FLAG_APPLICATION, 0)) AS FLAG_APPLICATION,
       MAX(NVL(APPL.FLAG_APPROVED, 0))    AS FLAG_APPROVED,
       MAX(NVL(APPL.FLAG_BOOKED, 0))      AS FLAG_BOOKED,
       --
       MAX(APPL.DTIME_PROPOSAL)      KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED) AS DATE_DECISION,
       MAX(APPL.FLAG_BOX_INSURANCE)  KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS FLAG_BOX_INSURANCE,
       MAX(APPL.FLAG_LIFE_INSURANCE) KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS FLAG_LIFE_INSURANCE,
       MAX(APPL.IS_REFINANCE)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS IS_REFINANCE,
       MAX(APPL.IS_REMOTE)           KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS IS_REMOTE,
       MAX(APPL.CODE_PRODUCT)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS CODE_PRODUCT,
       
       MAX(APPL.AMT_CREDIT_TOTAL)    KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_CREDIT_TOTAL,
       MAX(APPL.AMT_CREDIT_REQUEST)  KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_CREDIT_REQUEST,
       MAX(APPL.AMT_BOX_INS)         KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_BOX_INS,
       MAX(APPL.AMT_LIFE_INS)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_LIFE_INS,
       MAX(APPL.CNT_INSTALMENT)      KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS CNT_INSTALMENT,
       MAX(APPL.RATE_INTEREST)       KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS RATE_INTEREST
      
        FROM T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) ABT
        LEFT JOIN APPL
          ON ABT.SKP_CLIENT = appl.SKP_CLIENT
         AND ABT.MONTH_ = appl.MONTH_DECISION
         
       WHERE ABT.MONTH_ = CUR_MONTH
      
       GROUP BY ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.MONTH_ = S.MONTH_ and T.SKP_CLIENT = S.SKP_CLIENT ) WHEN
       MATCHED THEN
        UPDATE
           SET T.FLAG_APPLICATION    = S.FLAG_APPLICATION,
               T.FLAG_APPROVED       = S.FLAG_APPROVED,
               T.FLAG_BOOKED         = S.FLAG_BOOKED,
               T.DATE_DECISION       = S.DATE_DECISION,
               T.FLAG_BOX_INSURANCE  = S.FLAG_BOX_INSURANCE,
               T.FLAG_LIFE_INSURANCE = S.FLAG_LIFE_INSURANCE,
               T.IS_REFINANCE        = S.IS_REFINANCE,
               T.IS_REMOTE           = S.IS_REMOTE,
               T.CODE_PRODUCT        = S.CODE_PRODUCT,
               T.AMT_CREDIT_TOTAL    = S.AMT_CREDIT_TOTAL,
               T.AMT_CREDIT_REQUEST  = S.AMT_CREDIT_REQUEST,
               T.AMT_BOX_INS         = S.AMT_BOX_INS,
               T.AMT_LIFE_INS        = S.AMT_LIFE_INS,
               T.CNT_INSTALMENT      = S.CNT_INSTALMENT,
               T.RATE_INTEREST       = S.RATE_INTEREST
               
         WHERE T.MONTH_ = CUR_MONTH
           AND (nvl(T.FLAG_APPLICATION, 0) != nvl(S.FLAG_APPLICATION, 0)
            OR nvl(T.FLAG_APPROVED, 0) != nvl(S.FLAG_APPROVED, 0)
            OR nvl(T.FLAG_BOOKED, 0) != nvl(S.FLAG_BOOKED, 0)
            OR nvl(T.DATE_DECISION, DATE '1900-01-01') !=
               nvl(S.DATE_DECISION, DATE '1900-01-01')
            OR nvl(T.FLAG_BOX_INSURANCE, 0) != nvl(S.FLAG_BOX_INSURANCE, 0)
            OR nvl(T.FLAG_LIFE_INSURANCE, 0) != nvl(S.FLAG_LIFE_INSURANCE, 0)
            OR nvl(T.IS_REFINANCE, 0) != nvl(S.IS_REFINANCE, 0)
            OR nvl(T.IS_REMOTE, 0) != nvl(S.IS_REMOTE, 0)
            OR nvl(T.CODE_PRODUCT, '1') != nvl(S.CODE_PRODUCT, '1')
            OR nvl(T.AMT_CREDIT_TOTAL, 0) != nvl(S.AMT_CREDIT_TOTAL, 0)
            OR nvl(T.AMT_CREDIT_REQUEST, 0) != nvl(S.AMT_CREDIT_REQUEST, 0)
            OR nvl(T.AMT_BOX_INS, 0) != nvl(S.AMT_BOX_INS, 0)
            OR nvl(T.AMT_LIFE_INS, 0) != nvl(S.AMT_LIFE_INS, 0)
            OR nvl(T.CNT_INSTALMENT, 0) != nvl(S.CNT_INSTALMENT, 0)
            OR nvl(T.RATE_INTEREST, 0) != nvl(S.RATE_INTEREST, 0)
           );    
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                          calcStats    => 0);  
    COMMIT;
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES',anCntPartLast => 1); 
    
    
    
     
    I_STEP := I_STEP + 1;     
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'ABT_ATTR_PRIO_MERGE ' || CUR_MONTH);
 
    
    merge /*+ USE_HASH(T S)*/ into T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) t
    using (
      with PRIO as
       (SELECT /*+ MATERIALIZE FULL(S)*/
         S.SKP_CLIENT,
         S.DTIME_PROPOSAL,
         S.DTIME_COMM_FINAL,
         S.LAST_COMM_FINAL,
         S.LAST_COMM_CHANNEL_FINAL
        
          FROM AP_CRM.T_MZ_CRM_PRIORITIZED_SALES_V3 S
         where s.DTIME_PROPOSAL between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1)
           and s.product_channel_general in ('CASH X-SELL', 'REFINANCE'))
      
      SELECT /*+ USE_HASH(ABT PRIO)*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,       
       --
       MAX(PRIO.LAST_COMM_FINAL)         KEEP(DENSE_RANK LAST ORDER BY PRIO.DTIME_COMM_FINAL) AS LAST_COMM_FINAL,
       MAX(PRIO.LAST_COMM_CHANNEL_FINAL) KEEP(DENSE_RANK LAST ORDER BY PRIO.DTIME_COMM_FINAL) AS LAST_COMM_CHANNEL_FINAL
      
        FROM T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) ABT
        JOIN PRIO
          ON ABT.SKP_CLIENT = PRIO.SKP_CLIENT
         AND ABT.DATE_DECISION = PRIO.DTIME_PROPOSAL
         
       WHERE ABT.MONTH_ = CUR_MONTH      
       GROUP BY ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.MONTH_ = S.MONTH_ AND T.SKP_CLIENT = S.SKP_CLIENT) WHEN
       MATCHED THEN
        UPDATE
           SET T.LAST_COMM_FINAL               = S.LAST_COMM_FINAL,
               T.LAST_COMM_CHANNEL_FINAL       = S.LAST_COMM_CHANNEL_FINAL
               
         WHERE T.MONTH_ = CUR_MONTH
           AND (nvl(T.LAST_COMM_FINAL, '1')        != nvl(S.LAST_COMM_FINAL, '1')
            OR nvl(T.LAST_COMM_CHANNEL_FINAL, '0') != nvl(S.LAST_COMM_CHANNEL_FINAL, '0')
           );    
    
    PKG_MZ_HINTS.pStepEnd( anRowsResult => SQL%ROWCOUNT,
                           acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                           calcStats    => 0);
    commit;
    
    END IF;
    END LOOP;
                                 
    CUR_MONTH := ADD_MONTHS(CUR_MONTH, 1);    
    EXIT WHEN CUR_MONTH > TRUNC(SYSDATE, 'MM');
    END LOOP;
    
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES');    
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  end;
  
  
  
  


  PROCEDURE P_RESPONSE_CASH_OFFER_MERGE( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                         CNT_MONTH_CALC NUMBER DEFAULT 1 ) IS
    
  CUR_MONTH DATE	         := MONTH_START;
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_OFFER_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(8);    
    -- Start Init Log ---------------------------
  PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
  
  
  --PKG_MZ_HINTS.pExec('ALTER INDEX UDX_MZ_ABT_CL_SKPCL_MONTH UNUSABLE');
  
  FOR I IN 1..CNT_MONTH_CALC LOOP
  
  PKG_MZ_HINTS.pTruncate('T_ABT_ATTR_OFFER_MERGE_PREP');
  I_STEP := I_STEP + 1;
  PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                          acAction  => 'T_ABT_ATTR_OFFER_MERGE_PREP ' || CUR_MONTH);
  
  INSERT /*+ APPEND*/ INTO T_ABT_ATTR_OFFER_MERGE_PREP
        with offer as
         (SELECT /*+ MATERIALIZE USE_HASH(PR O) FULL(PR) FULL(O)*/
           distinct 
           pr.SKP_CLIENT,
           pr.CODE_PRODUCT,
           pr.CODE_PRODUCT_GROUP,
           pr.DTIME_CREATION,
           trunc(pr.DTIME_CREATION, 'mm') as DTIME_CREATION_MONTH,
           pr.DTIME_DEACTIVATION
          
            FROM OWNER_DWH.F_SAS_PARTICIPANT_AT PR
            JOIN OWNER_DWH.DC_SAS_OFFER O
              ON PR.SKF_SAS_PARTICIPANT = O.SKF_SAS_PARTICIPANT
             AND O.CODE_COMM_CHANNEL = 'BSL'
             AND O.DTIME_CREATION BETWEEN ADD_MONTHS(CUR_MONTH, -4) AND ADD_MONTHS(CUR_MONTH, 1)
             
           WHERE PR.DTIME_CREATION BETWEEN ADD_MONTHS(CUR_MONTH, -4) AND ADD_MONTHS(CUR_MONTH, 1)
             and pr.CODE_PRODUCT_GROUP NOT IN ('Виртуальный XP', 'XNA')),
        abt as
         (SELECT /*+ MATERIALIZE FULL(AT)*/
           SKP_CLIENT, MONTH_, DATE_DECISION, CODE_PRODUCT
            FROM T_MZ_CLIENT_ABT_ATTRIBUTES AT
           WHERE MONTH_ = CUR_MONTH
             /*and code_product is not null*/),
        un as
         (SELECT /*+ USE_HASH(ABT OFFER) MATERIALIZE*/
           DISTINCT
           ABT.SKP_CLIENT,
           ABT.MONTH_,
           OFFER.DTIME_CREATION,
           OFFER.CODE_PRODUCT,
           OFFER.CODE_PRODUCT_GROUP,
           case
             WHEN OFFER.CODE_PRODUCT LIKE '%'||ABT.CODE_PRODUCT||'%' THEN
              1
             else
              0
           END AS IS_LIKE_CODE_PRODUCT
          
            FROM ABT
            jOIN OFFER
              ON ABT.SKP_CLIENT = OFFER.SKP_CLIENT
             AND MONTH_ BETWEEN OFFER.DTIME_CREATION_MONTH AND
                 OFFER.DTIME_DEACTIVATION
             AND NVL(ABT.DATE_DECISION, OFFER.DTIME_CREATION) BETWEEN
                 OFFER.DTIME_CREATION AND TRUNC(OFFER.DTIME_DEACTIVATION) + 1)
        , UN2 AS
        (
        SELECT /*+ MATERIALIZE PARALLEL(4)*/
               SKP_CLIENT,
               MONTH_,
               MAX(DTIME_CREATION)     AS OFFER_DTIME_CREATION,
               MAX(CODE_PRODUCT)       KEEP(DENSE_RANK LAST ORDER BY IS_LIKE_CODE_PRODUCT, DTIME_CREATION) AS OFFER_CODE_PRODUCT,
               MAX(CODE_PRODUCT_GROUP) KEEP(DENSE_RANK LAST ORDER BY IS_LIKE_CODE_PRODUCT, DTIME_CREATION) AS OFFER_CODE_PRODUCT_GROUP
        --      
          FROM UN      
         GROUP BY SKP_CLIENT, MONTH_
         )
         
         SELECT SKP_CLIENT,
                MONTH_,
                OFFER_DTIME_CREATION,
                OFFER_CODE_PRODUCT,
                OFFER_CODE_PRODUCT_GROUP,                
                case when UPPER(OFFER_CODE_PRODUCT_GROUP) like 'REFINANCE%CB%' then 'REFINANCE_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'REFINANCE%' then 'REFINANCE'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_RAP%' then 'BSL_Monitoring'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_FR%' then 'Price_pilot'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_CB%' then 'RBP_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP%' then 'RBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_CASHBACK' then 'CE_RBP_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_1M' then 'CE_RBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_GIFT_XSELL' then 'oRBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_REFINANCE_GIFT_XSELL' then 'oRBP_REFIN'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_FLATPRICE%' then 'oRBP_FR'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'DOS CE CARD' then 'Card_CE' 
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'X-SELL CARD DOS' then 'Card_XS' 
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RESTRUCTURING%' then 'Restructuring' 
                     else 'other' 
                 end as OFFER_CODE_PRODUCT_GROUP2
                
           FROM UN2;
           
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_ATTR_OFFER_MERGE_PREP');
  
    
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'T_MZ_CLIENT_ABT_ATTRIBUTES ' || CUR_MONTH);
    
      For i In (Select p.high_value
                     , lead(o.DATA_OBJECT_ID) over(Order By p.partition_position) As DATA_OBJECT_ID
                  From user_tab_partitions p
                  join user_objects o
                    on o.SUBOBJECT_NAME = p.partition_name
                   and o.OBJECT_NAME    = p.table_name
                 Where p.table_name = 'T_MZ_CLIENT_ABT_ATTRIBUTES') Loop
      
        If i.high_value Like '%'||to_char(CUR_MONTH, 'mm-dd')||'%' Then
                            
    MERGE INTO T_MZ_CLIENT_ABT_ATTRIBUTES partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) T
      using T_ABT_ATTR_OFFER_MERGE_PREP S
            ON (T.MONTH_ = S.MONTH_ AND T.SKP_CLIENT = S.SKP_CLIENT) WHEN
         MATCHED THEN
          UPDATE
             SET T.OFFER_DTIME_CREATION     = S.OFFER_DTIME_CREATION,
                 T.OFFER_CODE_PRODUCT       = S.OFFER_CODE_PRODUCT,
                 T.OFFER_CODE_PRODUCT_GROUP = S.OFFER_CODE_PRODUCT_GROUP,
                 T.OFFER_CODE_PRODUCT_GROUP2= S.OFFER_CODE_PRODUCT_GROUP2
                 
           WHERE T.MONTH_ = CUR_MONTH
             and (nvl(T.OFFER_DTIME_CREATION, DATE '1900-01-01') !=
                 nvl(S.OFFER_DTIME_CREATION, DATE '1900-01-01') OR
                 nvl(T.OFFER_CODE_PRODUCT, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT, '1') OR
                 nvl(T.OFFER_CODE_PRODUCT_GROUP, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT_GROUP, '1') OR
                 nvl(T.OFFER_CODE_PRODUCT_GROUP2, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT_GROUP2, '1')) ;
        End If;
        
      End Loop;
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                          calcStats    => 0);
                          
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES', anCntPartLast => 1);  
    
    CUR_MONTH := ADD_MONTHS(CUR_MONTH, 1);
    EXIT WHEN CUR_MONTH > TRUNC(SYSDATE, 'MM');
    END LOOP;
    
    --PKG_MZ_HINTS.pExec('ALTER INDEX UDX_MZ_ABT_CL_SKPCL_MONTH USABLE');      
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  end;

  
  
  
  PROCEDURE P_RESPONSE_CARD_CLIENT_MERGE IS
  
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_CARD_CLIENT_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(4);
    
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
    
    -- Step 1 Start -------------------------------
    i_step := i_step + 1;
     
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE');
      insert 
        into T_MZ_CLIENT_ABT_CARD_ATTRIBUTE t
             (skp_client, 
              month_, 
              flag_eligibility, 
              flag_eligibility_xsell, 
              flag_eligibility_ce, 
              num_contract_active, 
              risk_grade)
             
       SELECT A.skp_client, 
              A.month_, 
              A.flag_eligibility, 
              A.flag_eligibility_xsell, 
              A.flag_eligibility_ce, 
              A.num_contract_active, 
              A.risk_grade

        FROM T_MZ_CLIENT_ABT_ATTRIBUTES A
        left join T_MZ_CLIENT_ABT_CARD_ATTRIBUTE t
          on t.skp_client = a.skp_client
         and t.month_ = a.month_
       where a.skp_client > 0
         and t.skp_client is null;
    --                   
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE',
                          calcStats    => 1);
    --PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES');  
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      --PKG_MZ_HINTS.pStepErr(fnEmailSend => 1);
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  END;
  

  
  
  
  PROCEDURE P_RESPONSE_CARD_APPL_MERGE( MONTH_START    DATE   DEFAULT TRUNC(SYSDATE, 'MM'),
                                        CNT_MONTH_CALC NUMBER DEFAULT 1 ) IS
    
  CUR_MONTH DATE	         := MONTH_START;
  AC_MODULE VARCHAR2(50)   := 'P_RESPONSE_CARD_APPL_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(8);  

    -- Start Init Log ---------------------------
  PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
  
  
  FOR I IN 1..CNT_MONTH_CALC LOOP
    
  
  I_STEP := I_STEP + 1;     
  PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                          acAction  => 'ABT_ATTR_CARD_APPL_MERGE ' || CUR_MONTH);
  
    
    merge into T_MZ_CLIENT_ABT_CARD_ATTRIBUTE t
    using (
      with appl as
       (SELECT /* MATERIALIZE FULL(S)*/
         s.skp_client,
         s.dtime_proposal,
         TRUNC(S.dtime_proposal, 'MM') AS MONTH_DECISION,
         s.code_product_type,
         s.code_product,
         1 AS FLAG_APPLICATION,
         s.flag_approved,
         s.flag_booked,
         s.flag_box_insurance,
         s.flag_life_insurance,
         s.is_refinance,
         s.is_remote,
         s.amt_credit_total,
         s.amt_credit_request,
         s.amt_box_ins,
         s.amt_life_ins,
         s.cnt_instalment,
         s.rate_interest
        
          FROM AP_CRM.T_AA_NEW_ALL_APPLICATIONS S
         where s.date_decision between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1) + 15
           AND s.dtime_proposal between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1)
           and s.product_channel_general in ('CARD XS'/*, 'REFINANCE'*/)
           and s.is_pos_plus_cc_cl = 0
           )
      
      SELECT /* USE_HASH(ABT APPL) NO_INDEX(ABT UDX_MZ_ABT_CL_SKPCL_MONTH )*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,
       
       MAX(NVL(APPL.FLAG_APPLICATION, 0)) AS FLAG_APPLICATION,
       MAX(NVL(APPL.FLAG_APPROVED, 0))    AS FLAG_APPROVED,
       MAX(NVL(APPL.FLAG_BOOKED, 0))      AS FLAG_BOOKED,
       --
       MAX(APPL.DTIME_PROPOSAL)      KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED) AS DATE_DECISION,
       MAX(APPL.FLAG_BOX_INSURANCE)  KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS FLAG_BOX_INSURANCE,
       MAX(APPL.FLAG_LIFE_INSURANCE) KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS FLAG_LIFE_INSURANCE,
       MAX(APPL.IS_REFINANCE)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS IS_REFINANCE,
       MAX(APPL.IS_REMOTE)           KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS IS_REMOTE,
       MAX(APPL.CODE_PRODUCT)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS CODE_PRODUCT,
       
       MAX(APPL.AMT_CREDIT_TOTAL)    KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_CREDIT_TOTAL,
       MAX(APPL.AMT_CREDIT_REQUEST)  KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_CREDIT_REQUEST,
       MAX(APPL.AMT_BOX_INS)         KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_BOX_INS,
       MAX(APPL.AMT_LIFE_INS)        KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS AMT_LIFE_INS,
       MAX(APPL.CNT_INSTALMENT)      KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS CNT_INSTALMENT,
       MAX(APPL.RATE_INTEREST)       KEEP(DENSE_RANK LAST ORDER BY APPL.FLAG_BOOKED, APPL.FLAG_APPROVED, APPL.DTIME_PROPOSAL) AS RATE_INTEREST
      
        FROM T_MZ_CLIENT_ABT_CARD_ATTRIBUTE ABT
        LEFT JOIN APPL
          ON ABT.SKP_CLIENT = appl.SKP_CLIENT
         AND ABT.MONTH_ = appl.MONTH_DECISION
         
       WHERE ABT.MONTH_ = CUR_MONTH
      
       GROUP BY ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.SKP_CLIENT = S.SKP_CLIENT AND T.MONTH_ = S.MONTH_) WHEN
       MATCHED THEN
        UPDATE
           SET T.FLAG_APPLICATION    = S.FLAG_APPLICATION,
               T.FLAG_APPROVED       = S.FLAG_APPROVED,
               T.FLAG_BOOKED         = S.FLAG_BOOKED,
               T.DATE_DECISION       = S.DATE_DECISION,
               T.FLAG_BOX_INSURANCE  = S.FLAG_BOX_INSURANCE,
               T.FLAG_LIFE_INSURANCE = S.FLAG_LIFE_INSURANCE,
               T.IS_REFINANCE        = S.IS_REFINANCE,
               T.IS_REMOTE           = S.IS_REMOTE,
               T.CODE_PRODUCT        = S.CODE_PRODUCT,
               T.AMT_CREDIT_TOTAL    = S.AMT_CREDIT_TOTAL,
               T.AMT_CREDIT_REQUEST  = S.AMT_CREDIT_REQUEST,
               T.AMT_BOX_INS         = S.AMT_BOX_INS,
               T.AMT_LIFE_INS        = S.AMT_LIFE_INS,
               T.CNT_INSTALMENT      = S.CNT_INSTALMENT,
               T.RATE_INTEREST       = S.RATE_INTEREST
               
         WHERE T.MONTH_ = CUR_MONTH
           AND (nvl(T.FLAG_APPLICATION, 0) != nvl(S.FLAG_APPLICATION, 0)
            OR nvl(T.FLAG_APPROVED, 0) != nvl(S.FLAG_APPROVED, 0)
            OR nvl(T.FLAG_BOOKED, 0) != nvl(S.FLAG_BOOKED, 0)
            OR nvl(T.DATE_DECISION, DATE '1900-01-01') !=
               nvl(S.DATE_DECISION, DATE '1900-01-01')
            OR nvl(T.FLAG_BOX_INSURANCE, 0) != nvl(S.FLAG_BOX_INSURANCE, 0)
            OR nvl(T.FLAG_LIFE_INSURANCE, 0) != nvl(S.FLAG_LIFE_INSURANCE, 0)
            OR nvl(T.IS_REFINANCE, 0) != nvl(S.IS_REFINANCE, 0)
            OR nvl(T.IS_REMOTE, 0) != nvl(S.IS_REMOTE, 0)
            OR nvl(T.CODE_PRODUCT, '1') != nvl(S.CODE_PRODUCT, '1')
            OR nvl(T.AMT_CREDIT_TOTAL, 0) != nvl(S.AMT_CREDIT_TOTAL, 0)
            OR nvl(T.AMT_CREDIT_REQUEST, 0) != nvl(S.AMT_CREDIT_REQUEST, 0)
            OR nvl(T.AMT_BOX_INS, 0) != nvl(S.AMT_BOX_INS, 0)
            OR nvl(T.AMT_LIFE_INS, 0) != nvl(S.AMT_LIFE_INS, 0)
            OR nvl(T.CNT_INSTALMENT, 0) != nvl(S.CNT_INSTALMENT, 0)
            OR nvl(T.RATE_INTEREST, 0) != nvl(S.RATE_INTEREST, 0)
           );    
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE',
                          calcStats    => 0);  
    COMMIT;
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE',anCntPartLast => 1); 
    
    
    
     
    I_STEP := I_STEP + 1;     
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'ABT_ATTR_PRIO_MERGE ' || CUR_MONTH);
 
    
    merge /*+ USE_HASH(T S)*/ into T_MZ_CLIENT_ABT_CARD_ATTRIBUTE t
    using (
      with PRIO as
       (SELECT /*+ MATERIALIZE FULL(S)*/
         S.SKP_CLIENT,
         S.DTIME_PROPOSAL,
         S.DTIME_COMM_FINAL,
         S.LAST_COMM_FINAL,
         S.LAST_COMM_CHANNEL_FINAL
        
          FROM AP_CRM.T_MZ_CRM_PRIORITIZED_SALES_V3 S
         where s.DTIME_PROPOSAL between add_months(CUR_MONTH, 0) and add_months(CUR_MONTH, 1)
           and s.product_channel_general in ('CARD X-SELL'))
      
      SELECT /*+ USE_HASH(ABT PRIO)*/
       ABT.SKP_CLIENT,
       ABT.MONTH_,       
       --
       MAX(PRIO.LAST_COMM_FINAL)         KEEP(DENSE_RANK LAST ORDER BY PRIO.DTIME_COMM_FINAL) AS LAST_COMM_FINAL,
       MAX(PRIO.LAST_COMM_CHANNEL_FINAL) KEEP(DENSE_RANK LAST ORDER BY PRIO.DTIME_COMM_FINAL) AS LAST_COMM_CHANNEL_FINAL
      
        FROM T_MZ_CLIENT_ABT_CARD_ATTRIBUTE ABT
        JOIN PRIO
          ON ABT.SKP_CLIENT = PRIO.SKP_CLIENT
         AND ABT.DATE_DECISION = PRIO.DTIME_PROPOSAL
         
       WHERE ABT.MONTH_ = CUR_MONTH      
       GROUP BY ABT.SKP_CLIENT, ABT.MONTH_) s
          ON (T.MONTH_ = S.MONTH_ AND T.SKP_CLIENT = S.SKP_CLIENT) WHEN
       MATCHED THEN
        UPDATE
           SET T.LAST_COMM_FINAL               = S.LAST_COMM_FINAL,
               T.LAST_COMM_CHANNEL_FINAL       = S.LAST_COMM_CHANNEL_FINAL
               
         WHERE T.MONTH_ = CUR_MONTH
           AND (nvl(T.LAST_COMM_FINAL, '1')        != nvl(S.LAST_COMM_FINAL, '1')
            OR nvl(T.LAST_COMM_CHANNEL_FINAL, '0') != nvl(S.LAST_COMM_CHANNEL_FINAL, '0')
           );    
    
    PKG_MZ_HINTS.pStepEnd( anRowsResult => SQL%ROWCOUNT,
                           acTable      => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE',
                           calcStats    => 0);
    commit;
    
                                 
    CUR_MONTH := ADD_MONTHS(CUR_MONTH, 1);    
    EXIT WHEN CUR_MONTH > TRUNC(SYSDATE, 'MM');
    END LOOP;
    
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_CARD_ATTRIBUTE');    
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  end;
  


  PROCEDURE P_RESPONSE_CARD_OFFER_MERGE( MONTH_START DATE DEFAULT TRUNC(SYSDATE, 'MM'),
                                         CNT_MONTH_CALC NUMBER DEFAULT 1 ) IS
    
  CUR_MONTH DATE	         := MONTH_START;
  AC_MODULE VARCHAR2(30)   := 'P_RESPONSE_CARD_OFFER_MERGE';
  i_step    NUMBER         := 0;
  
  BEGIN
  PKG_MZ_HINTS.pAlterSession(8);
    -- Start Init Log ---------------------------
  PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);
  
  
  --PKG_MZ_HINTS.pExec('ALTER INDEX UDX_MZ_ABT_CL_SKPCL_MONTH UNUSABLE');
  
  FOR I IN 1..CNT_MONTH_CALC LOOP
  
  PKG_MZ_HINTS.pTruncate('T_ABT_ATTR_OFFER_MERGE_PREP');
  I_STEP := I_STEP + 1;
  PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                          acAction  => 'T_ABT_ATTR_OFFER_MERGE_PREP ' || CUR_MONTH);
  
  INSERT /*+ APPEND*/ INTO T_ABT_ATTR_OFFER_MERGE_PREP
        with offer as
         (SELECT /*+ MATERIALIZE USE_HASH(PR O) FULL(PR) FULL(O)*/
           distinct 
           pr.SKP_CLIENT,
           pr.CODE_PRODUCT,
           pr.CODE_PRODUCT_GROUP,
           pr.DTIME_CREATION,
           trunc(pr.DTIME_CREATION, 'mm') as DTIME_CREATION_MONTH,
           pr.DTIME_DEACTIVATION
          
            FROM OWNER_DWH.F_SAS_PARTICIPANT_AT PR
            JOIN OWNER_DWH.DC_SAS_OFFER O
              ON PR.SKF_SAS_PARTICIPANT = O.SKF_SAS_PARTICIPANT
             AND O.CODE_COMM_CHANNEL = 'BSL'
             AND O.DTIME_CREATION BETWEEN ADD_MONTHS(CUR_MONTH, -6) AND ADD_MONTHS(CUR_MONTH, 1)
             
           WHERE PR.DTIME_CREATION BETWEEN ADD_MONTHS(CUR_MONTH, -6) AND ADD_MONTHS(CUR_MONTH, 1)
             and pr.CODE_PRODUCT_GROUP NOT IN ('Виртуальный XP', 'XNA')
             and pr.DTIME_DEACTIVATION > pr.DTIME_CREATION + 3
             
             ),
        abt as
         (SELECT /*+ MATERIALIZE FULL(AT)*/
           SKP_CLIENT, MONTH_, DATE_DECISION, CODE_PRODUCT
            FROM T_MZ_CLIENT_ABT_CARD_ATTRIBUTE AT
           WHERE MONTH_ = CUR_MONTH
             /*and code_product is not null*/),
        un as
         (SELECT /*+ USE_HASH(ABT OFFER) MATERIALIZE*/
           DISTINCT
           ABT.SKP_CLIENT,
           ABT.MONTH_,
           OFFER.DTIME_CREATION,
           OFFER.CODE_PRODUCT,
           OFFER.CODE_PRODUCT_GROUP,
           case
             WHEN OFFER.CODE_PRODUCT LIKE '%'||ABT.CODE_PRODUCT||'%' THEN
              1
             else
              0
           END AS IS_LIKE_CODE_PRODUCT
          
            FROM ABT
            jOIN OFFER
              ON ABT.SKP_CLIENT = OFFER.SKP_CLIENT
             AND MONTH_ BETWEEN OFFER.DTIME_CREATION_MONTH AND
                 OFFER.DTIME_DEACTIVATION
             AND NVL(ABT.DATE_DECISION, OFFER.DTIME_CREATION) BETWEEN
                 OFFER.DTIME_CREATION AND TRUNC(OFFER.DTIME_DEACTIVATION) + 1)
        , UN2 AS
        (
        SELECT /*+ MATERIALIZE PARALLEL(4)*/
               SKP_CLIENT,
               MONTH_,
               MAX(DTIME_CREATION)     AS OFFER_DTIME_CREATION,
               MAX(CODE_PRODUCT)       KEEP(DENSE_RANK LAST ORDER BY IS_LIKE_CODE_PRODUCT, DTIME_CREATION) AS OFFER_CODE_PRODUCT,
               MAX(CODE_PRODUCT_GROUP) KEEP(DENSE_RANK LAST ORDER BY IS_LIKE_CODE_PRODUCT, DTIME_CREATION) AS OFFER_CODE_PRODUCT_GROUP
        --      
          FROM UN      
         GROUP BY SKP_CLIENT, MONTH_
         )
         
         SELECT SKP_CLIENT,
                MONTH_,
                OFFER_DTIME_CREATION,
                OFFER_CODE_PRODUCT,
                OFFER_CODE_PRODUCT_GROUP,                
                case when UPPER(OFFER_CODE_PRODUCT_GROUP) like 'REFINANCE%CB%' then 'REFINANCE_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'REFINANCE%' then 'REFINANCE'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_RAP%' then 'BSL_Monitoring'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_FR%' then 'Price_pilot'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP_CB%' then 'RBP_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RBP%' then 'RBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_CASHBACK' then 'CE_RBP_CB'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_1M' then 'CE_RBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_GIFT_XSELL' then 'oRBP'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_REFINANCE_GIFT_XSELL' then 'oRBP_REFIN'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'ORBP_FLATPRICE%' then 'oRBP_FR'
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'DOS CE CARD' then 'Card_CE' 
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'X-SELL CARD DOS' then 'Card_XS' 
                     WHEN UPPER(OFFER_CODE_PRODUCT_GROUP) like 'RESTRUCTURING%' then 'Restructuring' 
                     else 'other' 
                 end as OFFER_CODE_PRODUCT_GROUP2
                
           FROM UN2;
           
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_ATTR_OFFER_MERGE_PREP');
  
    
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step, 
                            acAction  => 'T_MZ_CLIENT_ABT_ATTRIBUTES ' || CUR_MONTH);
    
      For i In (Select p.high_value
                     , lead(o.DATA_OBJECT_ID) over(Order By p.partition_position) As DATA_OBJECT_ID
                  From user_tab_partitions p
                  join user_objects o
                    on o.SUBOBJECT_NAME = p.partition_name
                   and o.OBJECT_NAME    = p.table_name
                 Where p.table_name = 'T_MZ_CLIENT_ABT_ATTRIBUTES') Loop
      
        If i.high_value Like '%'||to_char(CUR_MONTH, 'mm-dd')||'%' Then
                            
    MERGE INTO T_MZ_CLIENT_ABT_CARD_ATTRIBUTE partition 
               	(DATAOBJ_TO_PARTITION(T_MZ_CLIENT_ABT_ATTRIBUTES, i.DATA_OBJECT_ID)) T
      using T_ABT_ATTR_OFFER_MERGE_PREP S
            ON (T.MONTH_ = S.MONTH_ AND T.SKP_CLIENT = S.SKP_CLIENT) WHEN
         MATCHED THEN
          UPDATE
             SET T.OFFER_DTIME_CREATION     = S.OFFER_DTIME_CREATION,
                 T.OFFER_CODE_PRODUCT       = S.OFFER_CODE_PRODUCT,
                 T.OFFER_CODE_PRODUCT_GROUP = S.OFFER_CODE_PRODUCT_GROUP,
                 T.OFFER_CODE_PRODUCT_GROUP2= S.OFFER_CODE_PRODUCT_GROUP2
                 
           WHERE T.MONTH_ = CUR_MONTH
             and (nvl(T.OFFER_DTIME_CREATION, DATE '1900-01-01') !=
                 nvl(S.OFFER_DTIME_CREATION, DATE '1900-01-01') OR
                 nvl(T.OFFER_CODE_PRODUCT, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT, '1') OR
                 nvl(T.OFFER_CODE_PRODUCT_GROUP, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT_GROUP, '1') OR
                 nvl(T.OFFER_CODE_PRODUCT_GROUP2, '1') !=
                 nvl(S.OFFER_CODE_PRODUCT_GROUP2, '1')) ;
        End If;
        
      End Loop;
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_MZ_CLIENT_ABT_ATTRIBUTES',
                          calcStats    => 0);
                          
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_MZ_CLIENT_ABT_ATTRIBUTES', anCntPartLast => 1);  
    
    CUR_MONTH := ADD_MONTHS(CUR_MONTH, 1);
    EXIT WHEN CUR_MONTH > TRUNC(SYSDATE, 'MM');
    END LOOP;
    
    --PKG_MZ_HINTS.pExec('ALTER INDEX UDX_MZ_ABT_CL_SKPCL_MONTH USABLE');      
  
    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);
        
  EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
      raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
  end;
  
  
  
  -- calculation to prev month --
  PROCEDURE P_MAIN(MONTH_ DATE, cnt_month_ number) IS
       
    P_SUBJECTS  PKG_MZ_HINTS.GT_MVIEW_NAME2; 
    I           NUMBER := 0;
    
    BEGIN

      -- should disable. Because of buffer overflow, if more than 100000 symbols in logs
    DBMS_OUTPUT.DISABLE;
    PKG_MZ_HINTS.pAlterSession(8);
       
     ---- Call Procedures ----
     P_RESPONSE_CASH_CLIENT_MERGE      (MONTH_START => MONTH_);
     P_RESPONSE_ELIGIBILITY_MERGE      (MONTH_START => MONTH_, CNT_MONTH_CALC => cnt_month_);
     P_RESPONSE_CASH_APPL_MERGE        (MONTH_START => MONTH_, CNT_MONTH_CALC => cnt_month_);
     P_RESPONSE_CASH_OFFER_MERGE       (MONTH_START => MONTH_, CNT_MONTH_CALC => cnt_month_);
     
     --------------------------
        
     ---- For Report to Email -----------------
      I := I + 1;
      P_SUBJECTS(I) := 'P_RESPONSE_CLIENT_MERGE';
      I := I + 1;
      P_SUBJECTS(I) := 'P_RESPONSE_ELIGIBILITY_MERGE';
      I := I + 1;
      P_SUBJECTS(I) := 'P_RESPONSE_APPLICATION_MERGE';
      I := I + 1;
      P_SUBJECTS(I) := 'P_RESPONSE_OFFER_MERGE';
     --------------------------------------------
     
     
     
     PKG_MZ_HINTS.pMail(P_SUBJECTS, 
                       'PKG_MZ_ABT_DATAMART_SALE', 
                       1);        

   END;


END;
/
