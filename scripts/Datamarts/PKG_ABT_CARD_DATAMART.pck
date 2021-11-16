CREATE OR REPLACE PACKAGE AP_CRM_ABT.PKG_ABT_CARD_DATAMART authid current_user is

    DATE_CALC DATE := trunc(sysdate,'MM');

    PROCEDURE P_ABT_CARD_ALL_TRX  (date_clc DATE default trunc(sysdate, 'mm'));
    PROCEDURE P_ABT_CARD_MCC_TRX  (date_clc DATE default trunc(sysdate, 'mm'));
    PROCEDURE P_ABT_CARD_CREDIT_INFO    (date_clc DATE default trunc(sysdate, 'mm'));
   
    
    PROCEDURE P_ABT_CARD_DataMart     (date_clc DATE );
    PROCEDURE P_MAIN                  (date_clc DATE );
    
    procedure p_run_proc (ip_start in number, ip_end in number);
    PROCEDURE P_MAIN_PARALLEL_EXEC;


END;
/
CREATE OR REPLACE PACKAGE BODY AP_CRM_ABT.PKG_ABT_CARD_DATAMART IS

  --- 1. T_ABT_CARD_ALL_TRX
  ---
  --- 2. T_ABT_CARD_MCC_TRX AP_CRM_ABT.PKG_ABT_CARD_DATAMART
  ---
  --- 3. T_ABT_CARD_CREDIT_INFO
  ---
  --- AP_CRM_ABT.T_ABT_CARD_DATAMART  -----



    PROCEDURE P_ABT_CARD_ALL_TRX(date_clc DATE default trunc(sysdate, 'mm') ) IS

    i_step    NUMBER         := 0;

    BEGIN

    PKG_MZ_HINTS.pAlterSession(8);
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => 'P_ABT_CARD_ALL_TRX');
    
    DATE_CALC := nvl(date_clc, DATE_CALC);

    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_CARD_ALL_TRX');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_CARD_ALL_TRX ' || DATE_CALC);
    
    INSERT /*+ APPEND*/ INTO T_ABT_CARD_ALL_TRX
    With W$TRX As
     (
     Select /*+ USE_HASH( TT TR ) full(tt) full(tr)*/
       TR.SKF_CARD_TRANSACTION
      ,TR.SKP_CREDIT_CASE
      ,TR.SKP_CLIENT
      ,TR.DATE_TRANSACTION
      ,TR.AMT_BILLING                AS AMT_TRANSACTION
      ,TR.CODE_TERMINAL_MERCHANT_CAT As MCC
      ,lead(TR.DATE_TRANSACTION) over(partition by tr.SKP_CREDIT_CASE order by tr.DATE_TRANSACTION desc) as date_trx_before
      ,case when pt.CODE_PRICELIST_ITEM_TYPE = 'RTL'  then 'RTL' --TR.CODE_TERMINAL_TYPE
            when pt.CODE_PRICELIST_ITEM_TYPE = 'OTEP' then 'ONLINE'
            else                                           'CASH' end as TRX_type    
      
        From OWNER_DWH.F_CARD_ACCOUNT_ITEM_TT TT -- ONLY CARD, ALL BOOKED 
        Join OWNER_DWH.F_CARD_TRANSACTION_TT tr
          On tt.SKP_CREDIT_CASE = tr.SKP_CREDIT_CASE
         And TT.SKP_CREDIT_TYPE = TR.SKP_CREDIT_TYPE
         And TT.DATE_DECISION = TR.DATE_DECISION
         And tt.SOURCE_EXTERNAL_ID = tr.SOURCE_EXTERNAL_ID
        Join OWNER_DWH.CL_PRICELIST_ITEM_TYPE PT
          On pt.SKP_PRICELIST_ITEM_TYPE = tt.SKP_PRICELIST_ITEM_TYPE
      
       Where tt.CODE_STATUS = 'a'
         And tt.DATE_DECISION < DATE_CALC
         AND TR.DATE_TRANSACTION < DATE_CALC
         And pt.CODE_PRICELIST_ITEM_TYPE In
             ('RTL',
              'REL_INPRE',
              'REWARD_SETTLEMENT',
              'ATM',
              'CSD',
              'CWCD',
              'CWKI',
              'ICD',
              'IIS',
              'INS',
              'IPD',
              'OTEP',
              'OTSE'))

    select /*+ PARALLEL(4)*/
           z1.SKP_CREDIT_CASE
          ,z1.skp_client   
          ,DATE_CALC    as month_                
         ,trunc(DATE_CALC) - max(z1.DATE_TRANSACTION) as CNT_D_LAST_TRX
         ,max(z1.DATE_TRANSACTION) - max(z1.date_trx_before)  as CNT_DB_LAST_2_TRX -- Count Days Between LAST 2 Transaction 
         
         ,round(avg(z1.DATE_TRANSACTION - z1.date_trx_before),1) as AVG_DB
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_1M
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_3M
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_6M  
         
         ,sum(z1.AMT_TRANSACTION) SUM_AMT_TRX
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_1M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_3M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_6M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_12M
         
         ,sum(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_0W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-7,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_1W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_2W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-21,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_3W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_4W


         ,COUNT(z1.AMT_TRANSACTION) CNT_AMT_TRX
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_1M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_3M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_6M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_12M
         
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_0W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-7,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_1W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_2W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-21,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_3W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_4W

         ,round(AVG(z1.AMT_TRANSACTION),1) AVG_AMT_TRX
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_1M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_3M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_6M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_12M
         
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_0W
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_2W
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_4W

         ,MEDIAN(z1.AMT_TRANSACTION) MED_AMT_TRX
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end) MED_AMT_TRX_1M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end) MED_AMT_TRX_3M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end) MED_AMT_TRX_6M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end) MED_AMT_TRX_12M
         
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_0W
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_2W
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_4W


         ,MAX(z1.AMT_TRANSACTION) MAX_AMT_TRX
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_1M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_3M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_6M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_12M
         
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_0W
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_2W
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_4W


         ,MIN(z1.AMT_TRANSACTION) MIN_AMT_TRX
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-1) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_1M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-3) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_3M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-6) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_6M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(DATE_CALC,-12) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_12M
         
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC,'IW') then z1.AMT_TRANSACTION end) MIN_AMT_TRX_0W
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-14,'IW') then z1.AMT_TRANSACTION end) MIN_AMT_TRX_2W
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(DATE_CALC-28,'IW') then z1.AMT_TRANSACTION end) MIN_AMT_TRX_4W
         
    from W$TRX z1

    group by z1.skp_client
            ,z1.SKP_CREDIT_CASE;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_CARD_ALL_TRX');


      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;


    PROCEDURE P_ABT_CARD_MCC_TRX(date_clc DATE  default trunc(sysdate, 'mm') ) IS

    i_step    NUMBER         := 0;

    BEGIN

    PKG_MZ_HINTS.pAlterSession(8);
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => 'P_ABT_CARD_MCC_TRX');
    
    DATE_CALC := nvl(date_clc, DATE_CALC);

    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_CARD_MCC_TRX');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_CARD_MCC_TRX ' || DATE_CALC);
    
    INSERT /*+ APPEND*/ INTO T_ABT_CARD_MCC_TRX
    With W$TRX As
     (
     Select /*+ USE_HASH( TT TR ) full(tt) full(tr)*/
       TR.SKF_CARD_TRANSACTION
      ,TR.SKP_CREDIT_CASE
      ,TR.SKP_CLIENT
      ,TR.DATE_TRANSACTION
      ,TR.AMT_BILLING                AS AMT_TRANSACTION
      ,TR.CODE_TERMINAL_MERCHANT_CAT As MCC
      ,lead(TR.DATE_TRANSACTION) over(partition by tr.SKP_CREDIT_CASE order by tr.DATE_TRANSACTION desc) as date_trx_before
      ,case when pt.CODE_PRICELIST_ITEM_TYPE = 'RTL'  then 'RTL' --TR.CODE_TERMINAL_TYPE
            when pt.CODE_PRICELIST_ITEM_TYPE = 'OTEP' then 'ONLINE'
            else                                           'CASH' end as TRX_type    
      
        From OWNER_DWH.F_CARD_ACCOUNT_ITEM_TT TT -- ONLY CARD, ALL BOOKED 
        Join OWNER_DWH.F_CARD_TRANSACTION_TT tr
          On tt.SKP_CREDIT_CASE = tr.SKP_CREDIT_CASE
         And TT.SKP_CREDIT_TYPE = TR.SKP_CREDIT_TYPE
         And TT.DATE_DECISION = TR.DATE_DECISION
         And tt.SOURCE_EXTERNAL_ID = tr.SOURCE_EXTERNAL_ID
        Join OWNER_DWH.CL_PRICELIST_ITEM_TYPE PT
          On pt.SKP_PRICELIST_ITEM_TYPE = tt.SKP_PRICELIST_ITEM_TYPE
      
       Where tt.CODE_STATUS = 'a'
         And tt.DATE_DECISION < date_clc
         AND TR.DATE_TRANSACTION < date_clc
         And pt.CODE_PRICELIST_ITEM_TYPE In
             ('RTL',
              'REL_INPRE',
              'REWARD_SETTLEMENT',
              'ATM',
              'CSD',
              'CWCD',
              'CWKI',
              'ICD',
              'IIS',
              'INS',
              'IPD',
              'OTEP',
              'OTSE'))
              
    select /*+ PARALLEL(4)*/
           z1.skp_client
          ,z1.SKP_CREDIT_CASE
          ,DATE_CALC                                               as month_ 
          ,z1.TRX_type
          ,case when z1.TRX_type = 'RTL' then m.mcc_group_code end as mcc_group_code    --!  
          
         ,round(avg(z1.DATE_TRANSACTION - z1.date_trx_before),1) as AVG_DB
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_1M
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_3M
         ,round(avg(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.DATE_TRANSACTION - z1.date_trx_before end),1) as AVG_DB_6M
         
         
         ,sum(z1.AMT_TRANSACTION) SUM_AMT_TRX
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_1M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_3M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_6M
         ,sum(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end) SUM_AMT_TRX_12M
         
         ,sum(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_0W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(date_clc-7,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_1W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_2W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(date_clc-21,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_3W
         ,sum(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end) SUM_AMT_TRX_4W


         ,COUNT(z1.AMT_TRANSACTION) CNT_AMT_TRX
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_1M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_3M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_6M
         ,COUNT(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end) CNT_AMT_TRX_12M
         
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_0W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(date_clc-7,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_1W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_2W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(date_clc-21,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_3W
         ,COUNT(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end) CNT_AMT_TRX_4W


         ,round(AVG(z1.AMT_TRANSACTION),1) AVG_AMT_TRX
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_1M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_3M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_6M
         ,round(AVG(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_12M
         
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_0W
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_2W
         ,round(AVG(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end),1) AVG_AMT_TRX_4W


         ,MEDIAN(z1.AMT_TRANSACTION) MED_AMT_TRX
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end) MED_AMT_TRX_1M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end) MED_AMT_TRX_3M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end) MED_AMT_TRX_6M
         ,MEDIAN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end) MED_AMT_TRX_12M
         
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_0W
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_2W
         ,MEDIAN(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end) MED_AMT_TRX_4W


         ,MAX(z1.AMT_TRANSACTION) MAX_AMT_TRX
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_1M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_3M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_6M
         ,MAX(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end) MAX_AMT_TRX_12M
         
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_0W
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_2W
         ,MAX(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end) MAX_AMT_TRX_4W


         ,MIN(z1.AMT_TRANSACTION) MIN_AMT_TRX
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-1) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_1M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-3) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_3M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-6) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_6M
         ,MIN(case when z1.DATE_TRANSACTION >= ADD_MONTHS(date_clc,-12) then z1.AMT_TRANSACTION end) MIN_AMT_TRX_12M
         
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(date_clc,'IW')    then z1.AMT_TRANSACTION end) MIN_AMT_TRX_0W
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(date_clc-14,'IW') then z1.AMT_TRANSACTION end) MIN_AMT_TRX_2W
         ,MIN(case when z1.DATE_TRANSACTION >= trunc(date_clc-28,'IW') then z1.AMT_TRANSACTION end) MIN_AMT_TRX_4W
         
        -- ,RANK() OVER(PARTITION BY z1.SKP_CREDIT_CASE ORDER BY case when z1.TRX_type = 'RTL' and m.mcc_group_code != '00' then COUNT(z1.AMT_TRANSACTION) end desc) rnk_favor -- CNT_AMT_TRX_01
    from W$TRX z1
    left join AP_CRM.T_YT_MCC_GROUP_CRM m on to_char(m.mcc_code) = z1.MCC  --MCC_group                                

    group by  z1.skp_client
          ,z1.SKP_CREDIT_CASE
          ,z1.TRX_type
          ,case when z1.TRX_type = 'RTL' then m.mcc_group_code end;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_CARD_MCC_TRX');


      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;



    PROCEDURE P_ABT_CARD_CREDIT_INFO(date_clc DATE default trunc(sysdate, 'mm') ) IS

    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC := nvl(date_clc, DATE_CALC);

    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => 'P_ABT_CARD_CREDIT_INFO');

    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_CARD_CREDIT_INFO');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_CARD_CREDIT_INFO ' || DATE_CALC);

	  INSERT /*+ APPEND*/ INTO T_ABT_CARD_CREDIT_INFO
    
    with W$DPD as ( -- DPD history
    select /*+ MATERIALIZE*/
           dp.SKP_CREDIT_CASE
          ,max(dp.DPD) as CON_CNT_DPD
          ,max(case when dp.REPORT_DATE > ADD_MONTHS(DATE_CALC,-1) then dp.DPD end)  as CON_CNT_DPD_1m
          ,max(case when dp.REPORT_DATE > ADD_MONTHS(DATE_CALC,-3) then dp.DPD end)  as CON_CNT_DPD_3m
          ,max(case when dp.REPORT_DATE > ADD_MONTHS(DATE_CALC,-6) then dp.DPD end)  as CON_CNT_DPD_6m
          ,max(case when dp.REPORT_DATE > ADD_MONTHS(DATE_CALC,-12) then dp.DPD end) as CON_CNT_DPD_12m
     from AP_FIN.V_AS_CRED_PORTF_FS dp
     join owner_dwh.dc_credit_case cc 
       on cc.skp_credit_case = dp.SKP_CREDIT_CASE
      and cc.skp_credit_type = 3
      AND CC.date_decision < DATE_CALC
    where dp.REPORT_DATE < DATE_CALC
    group by dp.SKP_CREDIT_CASE
    ),
    W$ASH as ( -- BALANCE(DEBT) history 
    select /*+ MATERIALIZE*/
           dtt.skp_credit_case
          ,max(dtt.is_clip) as CON_FLAG_CLIPPED
          ,max(dtt.is_declip) as CON_FLAG_DECLIPPED
          ,min(case when dtt.is_used_client = 1 then dtt.month_ end) min_trx_month
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-2) then dtt.amt_debt_start end) as CON_AMT_DEBT_1M
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-3) then dtt.amt_debt_start end) as CON_AMT_DEBT_2M
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-4) then dtt.amt_debt_start end) as CON_AMT_DEBT_3M
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-5) then dtt.amt_debt_start end) as CON_AMT_DEBT_4M
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-6) then dtt.amt_debt_start end) as CON_AMT_DEBT_5M
          ,max(case when dtt.month_ = ADD_MONTHS(DATE_CALC,-7) then dtt.amt_debt_start end) as CON_AMT_DEBT_6M
          ,max(case when dtt.month_ >= ADD_MONTHS(DATE_CALC,-4) then dtt.amt_debt_start end) as CON_max_AMT_DEBT_3M
          ,max(case when dtt.month_ >= ADD_MONTHS(DATE_CALC,-7) then dtt.amt_debt_start end) as CON_max_AMT_DEBT_6M
          
    from ap_crm.t_Ash_Card_Datamart dtt
    where dtt.month_ <= add_months(DATE_CALC , -1)
      AND DTT.NAME_CARD_STATUS NOT IN ('LOST', 'STOLEN')
    group by dtt.skp_credit_case
    ),
    W$VALID as ( -- card valid to
    select /*+ MATERIALIZE*/
           cd.SKP_CREDIT_CASE
          ,max(cd.DTIME_PLASTIC_VALID_TO) date_valid_to
    from OWNER_DWH.DC_CARD cd
    where /*cd.CODE_CARD_PLASTIC_STATUS in ('A','I','N')
    and */cd.date_decision < DATE_CALC
    group by cd.SKP_CREDIT_CASE
    ),
    W$CREDIT as ( -- MAIN
    select /*+ MATERIALIZE*/
            cc.skp_credit_case
           ,cc.skp_client
           ,CC.date_decision
           ,CC.skp_credit_type
           ,CC.name_product           as CON_PRD_NAME
           ,CC.rate_interest_initial  as CON_IR
           ,CASE WHEN CC.CODE_PRODUCT_PURPOSE = 'BOUND' or CC.code_product = 'MI_X-S_LTC14_RC' THEN 'CARD XS' -- XS with cash existing
                 WHEN CC.SKP_ACCOUNTING_METHOD = 12 and CC.CODE_PRODUCT_PROFILE = 'REL_CC_NO_W_KZP' or CC.code_product = 'MI_REL_W_KPCB_RW' THEN 'CARD KP'
                 WHEN CC.SKP_ACCOUNTING_METHOD = 12 THEN 'CARD WI'
                 ELSE 'CARD POS' END  as CON_PRD_GROUP
           ,case when lower(CC.NAME_SALESROOM) like '%mcrbranch%' then 'MCRBRANCH'
                    when lower(CC.NAME_SALESROOM) like '%branch%' or CC.CODE_SELLER='010172' then 'BRANCH'
                    when CC.CODE_SELLER='050007'                                             then 'KAZPOST'
                    else 'POS' end    as CON_SALE_CHANNEL
           ,MONTHS_BETWEEN( trunc(DATE_CALC,'mm'), trunc(cc.date_decision,'mm') ) as CON_MOB -- trunc = zashita of duraka   
           
           ,dpd.CON_CNT_DPD
           ,dpd.CON_CNT_DPD_1m
           ,dpd.CON_CNT_DPD_3m
           ,dpd.CON_CNT_DPD_6m
           ,dpd.CON_CNT_DPD_12m
           ,dt.segment_type_1   as CON_SEGMENT
           ,dt.amt_limit_actual as CON_AMT_LIMIT_ACTUAL
           ,dt.amt_limit_start  as CON_AMT_LIMIT_START
           ,dt.amt_debt_start   as CON_AMT_DEBT
           ,t0.CON_FLAG_CLIPPED
           ,t0.CON_FLAG_DECLIPPED
           ,t0.CON_AMT_DEBT_1M
           ,t0.CON_AMT_DEBT_2M
           ,t0.CON_AMT_DEBT_3M
           ,t0.CON_AMT_DEBT_4M
           ,t0.CON_AMT_DEBT_5M
           ,t0.CON_AMT_DEBT_6M
           ,(dt.amt_debt_start)/nullif(dt.amt_limit_actual, 0)               as CON_UTIL_RATE
           ,MONTHS_BETWEEN( t0.min_trx_month ,trunc(cc.date_decision,'mm') ) as CON_CNT_M_FIRST_TRX_FROM_SIGN
           ,dt.amt_debt_start/nullif(t0.CON_max_AMT_DEBT_6M, 0)              as CON_SHARE_DEBT_TO_MAX_6M
           ,(dt.amt_limit_start - dt.amt_initial)                            as CON_AMT_EXTRA_LIMIT
           ,MONTHS_BETWEEN(t1.date_valid_to, DATE_CALC)                      as CON_CNT_M_CARD_VALIDITY
           ,t0.CON_max_AMT_DEBT_3M
           ,t0.CON_max_AMT_DEBT_6M
           
    from AP_PUBLIC.MV_V_CRR_DISCHANNEL_KZ cc 
    join W$ASH t0                        on t0.skp_credit_case = cc.skp_credit_case
    join ap_crm.t_Ash_Card_Datamart dt   on dt.skp_credit_case = cc.skp_credit_case
                                        and dt.month_ = add_months(DATE_CALC , -1)-- correct
    left join W$VALID T1                 on t1.skp_credit_case = cc.skp_credit_case -- dc_card
    left join W$DPD dpd                  on cc.skp_credit_case = dpd.SKP_CREDIT_CASE

    where cc.date_decision < DATE_CALC
      AND cc.skp_credit_type = 3
      and cc.FLAG_BOOKED     = 1
      and CC.FLAG_IS_DEBIT  != 'Y'
    ),
    W$PAYM as ( -- payments
    SELECT /* MATERIALIZE FULL(FP)*/
           z0.skp_credit_case
          ,SUM(fp.amt_payment) as CON_AMT_PAY_ALL
          ,SUM(case when fp.DTIME_PAYMENT >= ADD_MONTHS(DATE_CALC,-1) then fp.amt_payment end)  as CON_AMT_PAY_ALL_1M
          ,SUM(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') then fp.amt_payment end) as CON_AMT_PAY_PRIN
          ,SUM(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') and fp.DTIME_PAYMENT >= ADD_MONTHS(DATE_CALC,-1) then fp.amt_payment end) as CON_AMT_PAY_PRIN_1M
          ,SUM(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') and fp.DTIME_PAYMENT >= ADD_MONTHS(DATE_CALC,-3) then fp.amt_payment end) as CON_AMT_PAY_PRIN_3M
          ,SUM(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') and fp.DTIME_PAYMENT >= ADD_MONTHS(DATE_CALC,-6) then fp.amt_payment end) as CON_AMT_PAY_PRIN_6M
          
          ,MAX(fp.amt_payment) as CON_MAX_AMT_PAY
          ,MIN(fp.amt_payment) as CON_MIN_AMT_PAY
          ,MAX(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') then fp.amt_payment end) as CON_MAX_AMT_PAY_PRIN
          ,MIN(case when cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL','INSURANCE') then fp.amt_payment end) as CON_MIN_AMT_PAY_PRIN

    FROM W$CREDIT z0
    JOIN OWNER_DWH.F_INSTALMENT_PAYMENT_AD fp ON fp.skp_credit_case  = z0.skp_credit_case
                                             AND FP.SKP_CREDIT_TYPE  = Z0.SKP_CREDIT_TYPE
                                             AND FP.DATE_DECISION    = Z0.DATE_DECISION
    JOIN owner_dwh.cl_instalment_line_type cl ON cl.skp_instalment_line_type = fp.SKP_INSTALMENT_LINE_TYPE
    where fp.skp_credit_type = 3
      AND fp.flag_deleted = 'N'
      AND fp.code_instalment_payment_status = 'a'
      AND fp.SKP_INSTALMENT_REGULARITY in (1,2,5)
      and fp.DTIME_PAYMENT < DATE_CALC
    GROUP BY z0.skp_credit_case
    )

    select     
           z1.skp_client
          ,z1.SKP_CREDIT_CASE
          ,DATE_CALC                     as MONTH_ 
          
          ,max(z1.CON_PRD_NAME)          as CON_PRD_NAME
          ,max(z1.CON_IR)                as CON_IR
          ,max(z1.CON_PRD_GROUP)         as CON_PRD_GROUP
          ,max(z1.CON_SALE_CHANNEL)      as CON_SALE_CHANNEL
          ,max(z1.CON_MOB)               as CON_MOB
          ,max(z1.CON_CNT_DPD)           as CON_CNT_DPD
          ,max(z1.CON_CNT_DPD_1m)        as CON_CNT_DPD_1m
          ,max(z1.CON_CNT_DPD_3m)        as CON_CNT_DPD_3m
          ,max(z1.CON_CNT_DPD_6m)        as CON_CNT_DPD_6m
          ,max(z1.CON_CNT_DPD_12m)       as CON_CNT_DPD_12m
          ,max(z1.CON_SEGMENT)           as CON_SEGMENT
          ,max(z1.CON_AMT_LIMIT_ACTUAL)  as CON_AMT_LIMIT_ACTUAL
          ,max(z1.CON_AMT_LIMIT_START)   as CON_AMT_LIMIT_START
          ,max(z1.CON_AMT_DEBT)          as CON_AMT_DEBT
          ,max(z1.CON_FLAG_CLIPPED)      as CON_FLAG_CLIPPED
          ,max(z1.CON_FLAG_DECLIPPED)    as CON_FLAG_DECLIPPED
          ,max(z1.CON_AMT_DEBT_1M)       as CON_AMT_DEBT_1M
          ,max(z1.CON_AMT_DEBT_2M)       as CON_AMT_DEBT_2M
          ,max(z1.CON_AMT_DEBT_3M)       as CON_AMT_DEBT_3M
          ,max(z1.CON_AMT_DEBT_4M)       as CON_AMT_DEBT_4M
          ,max(z1.CON_AMT_DEBT_5M)       as CON_AMT_DEBT_5M
          ,max(z1.CON_AMT_DEBT_6M)       as CON_AMT_DEBT_6M
          ,max(z1.CON_UTIL_RATE)         as CON_UTIL_RATE
          ,max(z1.CON_CNT_M_FIRST_TRX_FROM_SIGN) as CON_CNT_M_FIRST_TRX_FROM_SIGN
          ,max(z1.CON_SHARE_DEBT_TO_MAX_6M)      as CON_SHARE_DEBT_TO_MAX_6M
          ,max(z1.CON_AMT_EXTRA_LIMIT)           as CON_AMT_EXTRA_LIMIT
          ,max(z1.CON_CNT_M_CARD_VALIDITY)       as CON_CNT_M_CARD_VALIDITY
          ,max(z1.CON_max_AMT_DEBT_3m)           as CON_MAX_AMT_DEBT_3M
          ,max(z1.CON_max_AMT_DEBT_6m)           as CON_MAX_AMT_DEBT_6M
          
     FROM W$CREDIT Z1
     GROUP BY   
           z1.skp_client
          ,z1.SKP_CREDIT_CASE;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_CARD_CREDIT_INFO');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;




    PROCEDURE P_ABT_Card_DataMart(date_clc DATE ) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_CARD_DATAMART';
    i_step    NUMBER         := 0;
    IS_CUR_MONTH             number;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);

    PKG_MZ_HINTS.pAlterSession(8);
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);


    ----- Check if month already exists
    Select Count(1)
      Into IS_CUR_MONTH
      From T_ABT_CARD_DATAMART T
     Where T.MONTH_ = DATE_CALC;

    IF IS_CUR_MONTH = 0 THEN

    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_CARD_DATAMART ' || DATE_CALC);

    insert /*+ append*/ into T_ABT_CARD_DATAMART
    select 
           /*+ USE_HASH(INF Z2 Z3)*/
           INF.SKP_CREDIT_CASE
          ,INF.skp_client
          ,INF.MONTH_
          ,max(case when z3.TRX_type = 'CASH' and z3.SUM_AMT_TRX < Z2.SUM_AMT_TRX/2 then 1 else 0 end) as CON_FLAG_RTL_AMT
          ,max(case when z3.TRX_type = 'CASH' and z3.CNT_AMT_TRX < Z2.CNT_AMT_TRX/2 then 1 else 0 end) as CON_FLAG_RTL_CNT
         
          ,max(INF.CON_PRD_NAME)          as CON_PRD_NAME
          ,max(INF.CON_IR)                as CON_IR
          ,max(INF.CON_PRD_GROUP)         as CON_PRD_GROUP
          ,max(INF.CON_SALE_CHANNEL)      as CON_SALE_CHANNEL
          ,max(INF.CON_MOB)               as CON_MOB
          ,max(INF.CON_CNT_DPD)           as CON_CNT_DPD
          ,max(INF.CON_CNT_DPD_1m)        as CON_CNT_DPD_1m
          ,max(INF.CON_CNT_DPD_3m)        as CON_CNT_DPD_3m
          ,max(INF.CON_CNT_DPD_6m)        as CON_CNT_DPD_6m
          ,max(INF.CON_CNT_DPD_12m)       as CON_CNT_DPD_12m
          ,max(INF.CON_SEGMENT)           as CON_SEGMENT
          ,max(INF.CON_FLAG_CLIPPED)      as CON_FLAG_CLIPPED
          ,max(INF.CON_FLAG_DECLIPPED)    as CON_FLAG_DECLIPPED
          ,max(INF.CON_AMT_LIMIT_ACTUAL)  as CON_AMT_LIMIT_ACTUAL
          ,max(INF.CON_AMT_LIMIT_START)   as CON_AMT_LIMIT_START
          ,max(INF.CON_AMT_DEBT)          as CON_AMT_DEBT
          ,max(INF.CON_AMT_DEBT_1M)       as CON_AMT_DEBT_1M
          ,max(INF.CON_AMT_DEBT_2M)       as CON_AMT_DEBT_2M
          ,max(INF.CON_AMT_DEBT_3M)       as CON_AMT_DEBT_3M
          ,max(INF.CON_AMT_DEBT_4M)       as CON_AMT_DEBT_4M
          ,max(INF.CON_AMT_DEBT_5M)       as CON_AMT_DEBT_5M
          ,max(INF.CON_AMT_DEBT_6M)       as CON_AMT_DEBT_6M
          ,max(INF.CON_MAX_AMT_DEBT_3M)   as CON_MAX_AMT_DEBT_3M ---- 2019.08.05
          ,max(INF.CON_MAX_AMT_DEBT_6M)   as CON_MAX_AMT_DEBT_6M ---- 2019.08.05
          ,max(INF.CON_UTIL_RATE)         as CON_UTIL_RATE
          ,max(INF.CON_CNT_M_FIRST_TRX_FROM_SIGN) as CON_CNT_M_FIRST_TRX_FROM_SIGN
          ,max(INF.CON_SHARE_DEBT_TO_MAX_6M)      as CON_SHARE_DEBT_TO_MAX_6M
          ,max(INF.CON_AMT_EXTRA_LIMIT)           as CON_AMT_EXTRA_LIMIT
          ,max(INF.CON_CNT_M_CARD_VALIDITY)       as CON_CNT_M_CARD_VALIDITY

          ,max(z2.CNT_D_LAST_TRX)    as CNT_D_LAST_TRX
          ,max(z2.CNT_DB_LAST_2_TRX) as CNT_DB_LAST_2_TRX

          ,max(z2.AVG_DB)            as AVG_DB
          ,max(z2.AVG_DB_1M)         as AVG_DB_1M
          ,max(z2.AVG_DB_3M)         as AVG_DB_3M
          ,max(z2.AVG_DB_6M)         as AVG_DB_6M

          ,max(z2.SUM_AMT_TRX)       as SUM_AMT_TRX
          ,max(z2.SUM_AMT_TRX_1M)    as SUM_AMT_TRX_1M
          ,max(z2.SUM_AMT_TRX_3M)    as SUM_AMT_TRX_3M
          ,max(z2.SUM_AMT_TRX_6M)    as SUM_AMT_TRX_6M
          ,max(z2.SUM_AMT_TRX_12M)   as SUM_AMT_TRX_12M
          ,max(z2.SUM_AMT_TRX_0W)    as SUM_AMT_TRX_0W
          ,max(z2.SUM_AMT_TRX_1W)    as SUM_AMT_TRX_1W
          ,max(z2.SUM_AMT_TRX_2W)    as SUM_AMT_TRX_2W

          ,max(z2.CNT_AMT_TRX)       as CNT_AMT_TRX
          ,max(z2.CNT_AMT_TRX_1M)    as CNT_AMT_TRX_1M
          ,max(z2.CNT_AMT_TRX_3M)    as CNT_AMT_TRX_3M
          ,max(z2.CNT_AMT_TRX_6M)    as CNT_AMT_TRX_6M
          ,max(z2.CNT_AMT_TRX_12M)   as CNT_AMT_TRX_12M
          ,max(z2.CNT_AMT_TRX_0W)    as CNT_AMT_TRX_0W
          ,max(z2.CNT_AMT_TRX_1W)    as CNT_AMT_TRX_1W
          ,max(z2.CNT_AMT_TRX_2W)    as CNT_AMT_TRX_2W

          ,max(z2.AVG_AMT_TRX)       as AVG_AMT_TRX
          ,max(z2.AVG_AMT_TRX_1M)    as AVG_AMT_TRX_1M
          ,max(z2.AVG_AMT_TRX_3M)    as AVG_AMT_TRX_3M
          ,max(z2.AVG_AMT_TRX_6M)    as AVG_AMT_TRX_6M
          ,max(z2.AVG_AMT_TRX_12M)   as AVG_AMT_TRX_12M
          ,max(z2.AVG_AMT_TRX_0W)    as AVG_AMT_TRX_0W
          ,max(z2.AVG_AMT_TRX_2W)    as AVG_AMT_TRX_2W

          ,max(z2.MED_AMT_TRX)       as MED_AMT_TRX
          ,max(z2.MED_AMT_TRX_1M)    as MED_AMT_TRX_1M
          ,max(z2.MED_AMT_TRX_3M)    as MED_AMT_TRX_3M
          ,max(z2.MED_AMT_TRX_6M)    as MED_AMT_TRX_6M
          ,max(z2.MED_AMT_TRX_12M)   as MED_AMT_TRX_12M
          ,max(z2.MED_AMT_TRX_0W)    as MED_AMT_TRX_0W
          ,max(z2.MED_AMT_TRX_2W)    as MED_AMT_TRX_2W

          ,max(z2.MAX_AMT_TRX)       as MAX_AMT_TRX
          ,max(z2.MAX_AMT_TRX_1M)    as MAX_AMT_TRX_1M
          ,max(z2.MAX_AMT_TRX_3M)    as MAX_AMT_TRX_3M
          ,max(z2.MAX_AMT_TRX_6M)    as MAX_AMT_TRX_6M
          ,max(z2.MAX_AMT_TRX_12M)   as MAX_AMT_TRX_12M
          ,max(z2.MAX_AMT_TRX_0W)    as MAX_AMT_TRX_0W
          ,max(z2.MAX_AMT_TRX_2W)    as MAX_AMT_TRX_2W

          ,max(z2.MIN_AMT_TRX)       as MIN_AMT_TRX
          ,max(z2.MIN_AMT_TRX_1M)    as MIN_AMT_TRX_1M
          ,max(z2.MIN_AMT_TRX_3M)    as MIN_AMT_TRX_3M
          ,max(z2.MIN_AMT_TRX_6M)    as MIN_AMT_TRX_6M
          ,max(z2.MIN_AMT_TRX_12M)   as MIN_AMT_TRX_12M
          ,max(z2.MIN_AMT_TRX_0W)    as MIN_AMT_TRX_0W
          ,max(z2.MIN_AMT_TRX_2W)    as MIN_AMT_TRX_2W
          
               
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max(case when z3.TRX_type = 'ONLINE' then z2.AVG_DB end)    as AVG_DB_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z2.AVG_DB_1M end) as AVG_DB_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_DB_3M end) as AVG_DB_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_DB_6M end) as AVG_DB_6M_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_ON
          
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_ON
          ,max(case when z3.TRX_type = 'ONLINE' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_ON
     
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_DB end)    as AVG_DB_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_DB_1M end) as AVG_DB_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_DB_3M end) as AVG_DB_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_DB_6M end) as AVG_DB_6M_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_CASH
          
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_CASH
          ,max( case when z3.TRX_type = 'CASH' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_CASH

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_DB end)    as AVG_DB_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_DB_1M end) as AVG_DB_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_DB_3M end) as AVG_DB_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_DB_6M end) as AVG_DB_6M_00
          
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_00
          ,max( case when z3.mcc_group_code = '00' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_00
          
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_00
          ,max( case when z3.mcc_group_code = '00' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_00
          
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_00
          ,max( case when z3.mcc_group_code = '00' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_00

          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_00
          ,max( case when z3.mcc_group_code = '00' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_00
          
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_00
          ,max( case when z3.mcc_group_code = '00' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_00

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
       
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_DB end)    as AVG_DB_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_DB_1M end) as AVG_DB_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_DB_3M end) as AVG_DB_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_DB_6M end) as AVG_DB_6M_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_01
          ,max( case when z3.mcc_group_code = '01' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_01
          ,max( case when z3.mcc_group_code = '01' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_01
          
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_01
          ,max( case when z3.mcc_group_code = '01' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_01

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_DB end)    as AVG_DB_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_DB_1M end) as AVG_DB_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_DB_3M end) as AVG_DB_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_DB_6M end) as AVG_DB_6M_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_02
          ,max( case when z3.mcc_group_code = '02' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_02
          ,max( case when z3.mcc_group_code = '02' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_02
          
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_02
          ,max( case when z3.mcc_group_code = '02' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_02

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_DB end)    as AVG_DB_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_DB_1M end) as AVG_DB_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_DB_3M end) as AVG_DB_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_DB_6M end) as AVG_DB_6M_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_03
          ,max( case when z3.mcc_group_code = '03' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_03
          ,max( case when z3.mcc_group_code = '03' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_03
          
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_03
          ,max( case when z3.mcc_group_code = '03' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_03
          
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_DB end)    as AVG_DB_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_DB_1M end) as AVG_DB_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_DB_3M end) as AVG_DB_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_DB_6M end) as AVG_DB_6M_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_04
          ,max( case when z3.mcc_group_code = '04' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_04
          ,max( case when z3.mcc_group_code = '04' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_04
          
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_04
          ,max( case when z3.mcc_group_code = '04' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_04
     
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_DB end)    as AVG_DB_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_DB_1M end) as AVG_DB_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_DB_3M end) as AVG_DB_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_DB_6M end) as AVG_DB_6M_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_05
          ,max( case when z3.mcc_group_code = '05' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_05
          ,max( case when z3.mcc_group_code = '05' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_05
          
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_05
          ,max( case when z3.mcc_group_code = '05' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_05
          
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_DB end)    as AVG_DB_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_DB_1M end) as AVG_DB_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_DB_3M end) as AVG_DB_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_DB_6M end) as AVG_DB_6M_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_06
          ,max( case when z3.mcc_group_code = '06' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_06
          ,max( case when z3.mcc_group_code = '06' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_06
          
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_06
          ,max( case when z3.mcc_group_code = '06' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_06

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_DB end)    as AVG_DB_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_DB_1M end) as AVG_DB_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_DB_3M end) as AVG_DB_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_DB_6M end) as AVG_DB_6M_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_07
          ,max( case when z3.mcc_group_code = '07' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_07
          ,max( case when z3.mcc_group_code = '07' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_07
          
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_07
          ,max( case when z3.mcc_group_code = '07' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_07
          
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_DB end)    as AVG_DB_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_DB_1M end) as AVG_DB_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_DB_3M end) as AVG_DB_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_DB_6M end) as AVG_DB_6M_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_08
          ,max( case when z3.mcc_group_code = '08' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_08
          ,max( case when z3.mcc_group_code = '08' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_08
          
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_08
          ,max( case when z3.mcc_group_code = '08' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_08

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_DB end)    as AVG_DB_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_DB_1M end) as AVG_DB_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_DB_3M end) as AVG_DB_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_DB_6M end) as AVG_DB_6M_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_09
          ,max( case when z3.mcc_group_code = '09' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_09
          ,max( case when z3.mcc_group_code = '09' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_09
          
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_09
          ,max( case when z3.mcc_group_code = '09' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_09

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_DB end)    as AVG_DB_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_DB_1M end) as AVG_DB_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_DB_3M end) as AVG_DB_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_DB_6M end) as AVG_DB_6M_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_10
          ,max( case when z3.mcc_group_code = '10' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_10
          ,max( case when z3.mcc_group_code = '10' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_10
          
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_10
          ,max( case when z3.mcc_group_code = '10' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_10

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_DB end)    as AVG_DB_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_DB_1M end) as AVG_DB_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_DB_3M end) as AVG_DB_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_DB_6M end) as AVG_DB_6M_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_11
          ,max( case when z3.mcc_group_code = '11' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_11
          ,max( case when z3.mcc_group_code = '11' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_11
          
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_11
          ,max( case when z3.mcc_group_code = '11' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_11

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_DB end)    as AVG_DB_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_DB_1M end) as AVG_DB_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_DB_3M end) as AVG_DB_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_DB_6M end) as AVG_DB_6M_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_12
          ,max( case when z3.mcc_group_code = '12' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_12
          ,max( case when z3.mcc_group_code = '12' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_12
          
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_12
          ,max( case when z3.mcc_group_code = '12' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_12
          
          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_DB end)    as AVG_DB_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_DB_1M end) as AVG_DB_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_DB_3M end) as AVG_DB_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_DB_6M end) as AVG_DB_6M_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_13
          ,max( case when z3.mcc_group_code = '13' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_13
          ,max( case when z3.mcc_group_code = '13' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_13
          
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_13
          ,max( case when z3.mcc_group_code = '13' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_13

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_DB end)    as AVG_DB_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_DB_1M end) as AVG_DB_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_DB_3M end) as AVG_DB_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_DB_6M end) as AVG_DB_6M_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_14
          ,max( case when z3.mcc_group_code = '14' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_14
          ,max( case when z3.mcc_group_code = '14' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_14
          
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_14
          ,max( case when z3.mcc_group_code = '14' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_14

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_DB end)    as AVG_DB_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_DB_1M end) as AVG_DB_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_DB_3M end) as AVG_DB_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_DB_6M end) as AVG_DB_6M_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_15
          ,max( case when z3.mcc_group_code = '15' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_15
          ,max( case when z3.mcc_group_code = '15' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_15
          
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_15
          ,max( case when z3.mcc_group_code = '15' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_15

          ------------------------------------------------------------------------------------
          ------------------------------------------------------------------------------------
          
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_DB end)    as AVG_DB_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_DB_1M end) as AVG_DB_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_DB_3M end) as AVG_DB_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_DB_6M end) as AVG_DB_6M_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX end)     as SUM_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_1M end)  as SUM_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_3M end)  as SUM_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_6M end)  as SUM_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_12M end) as SUM_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_0W end)  as SUM_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_1W end)  as SUM_AMT_TRX_1W_16
          ,max( case when z3.mcc_group_code = '16' then z3.SUM_AMT_TRX_2W end)  as SUM_AMT_TRX_2W_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX end)     as CNT_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_1M end)  as CNT_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_3M end)  as CNT_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_6M end)  as CNT_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_12M end) as CNT_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_0W end)  as CNT_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_1W end)  as CNT_AMT_TRX_1W_16
          ,max( case when z3.mcc_group_code = '16' then z3.CNT_AMT_TRX_2W end)  as CNT_AMT_TRX_2W_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX end)     as AVG_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_1M end)  as AVG_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_3M end)  as AVG_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_6M end)  as AVG_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_12M end) as AVG_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_0W end)  as AVG_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.AVG_AMT_TRX_2W end)  as AVG_AMT_TRX_2W_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX end)     as MED_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_1M end)  as MED_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_3M end)  as MED_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_6M end)  as MED_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_12M end) as MED_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_0W end)  as MED_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.MED_AMT_TRX_2W end)  as MED_AMT_TRX_2W_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX end)     as MAX_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_1M end)  as MAX_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_3M end)  as MAX_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_6M end)  as MAX_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_12M end) as MAX_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_0W end)  as MAX_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.MAX_AMT_TRX_2W end)  as MAX_AMT_TRX_2W_16
          
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX end)     as MIN_AMT_TRX_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_1M end)  as MIN_AMT_TRX_1M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_3M end)  as MIN_AMT_TRX_3M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_6M end)  as MIN_AMT_TRX_6M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_12M end) as MIN_AMT_TRX_12M_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_0W end)  as MIN_AMT_TRX_0W_16
          ,max( case when z3.mcc_group_code = '16' then z3.MIN_AMT_TRX_2W end)  as MIN_AMT_TRX_2W_16

    from T_ABT_CARD_CREDIT_INFO INF
    LEFT JOIN T_ABT_CARD_ALL_TRX z2
      ON INF.SKP_CREDIT_CASE = Z2.SKP_CREDIT_CASE
     AND INF.SKP_CLIENT      = Z2.SKP_CLIENT
     AND INF.MONTH_          = Z2.MONTH_
    LEFT JOIN T_ABT_CARD_MCC_TRX Z3 
      ON Z2.SKP_CREDIT_CASE  = Z3.SKP_CREDIT_CASE 
     AND Z2.SKP_CLIENT       = Z3.SKP_CLIENT
     AND Z2.MONTH_           = Z3.MONTH_
     
    group by INF.SKP_CREDIT_CASE
            ,INF.skp_client
            ,INF.MONTH_
     ;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_CARD_DATAMART',
                          calcStats    => 0);
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_ABT_CARD_DATAMART', anCntPartLast => 1);

    END IF;



    -- Finish Log  ------------------------------
    PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;




    PROCEDURE P_MAIN(date_clc DATE ) IS

    P_SUBJECTS  PKG_MZ_HINTS.GT_MVIEW_NAME2;
    I           NUMBER := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      -- should disable. Because of buffer overflow, if more than 100000 symbols in logs
    --DBMS_OUTPUT.DISABLE;
    PKG_MZ_HINTS.pAlterSession(8);

     ---- Call Procedures ----
     P_ABT_CARD_ALL_TRX            (DATE_CALC);
     P_ABT_CARD_MCC_TRX            (DATE_CALC);
     P_ABT_CARD_CREDIT_INFO        (DATE_CALC);
    
     P_ABT_CARD_DATAMART           (DATE_CALC);
     --------------------------

     ---- For Report to Email -----------------
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_CARD_ALL_TRX';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_CARD_MCC_TRX';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_CARD_CREDIT_INFO';      
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_CARD_DATAMART';
     --------------------------------------------



     /*PKG_MZ_HINTS.pMail(P_SUBJECTS,
                       'PKG_ABT_CARD_DATAMART',
                       1);*/

   END;
   
   
   procedure p_run_proc (ip_start in number, ip_end in number)  is
      v_proc_name proc_map.proc_name%type;
      v varchar2(4000);
    begin
      begin
        Select t.proc_name
          Into v_proc_name
          From proc_map t
         Where t.proc_id = ip_start;
      exception
        when no_data_found then null;
        when too_many_rows then null;
      end;

      if v_proc_name is not null
        then
          --execute immediate 'begin ' || v_proc_name || '(to_date(''' || dd || ''', ''yyyy-mm-dd'') ); end;';
          execute immediate 'begin ' || v_proc_name || '; end;';
         /* v:='begin ' || v_proc_name || '(); end;';
          dbms_output.put_line(v);*/
      end if;
    end;
   
   
   
   PROCEDURE P_MAIN_PARALLEL_EXEC IS
    v_task_name     varchar2(4000) := 'ABT_CARD_DM';
    v_sql           varchar2(4000);
    v_run           varchar2(4000);
    v_thread_count  number;
    v_task_status   number;
    
    begin
      --
      v_sql :='select t.proc_id as num_col ,t.proc_id as num_col
                 from proc_map t 
                where t.is_active = ''Y'' 
                order by t.proc_id';
      --          
      v_run := 'begin p_run_proc(ip_start => :start_id, ip_end => :end_id); end;';
      --
      select count(*) into v_thread_count 
      from proc_map t
      where t.is_active = 'Y';
      --
       
      dbms_parallel_execute.create_task(task_name => v_task_name);
      dbms_parallel_execute.create_chunks_by_SQL (task_name => v_task_name, sql_stmt => v_sql, by_rowid => false);
      --
      pkg_mz_hints.pAppInfo(acAction => 'run_task');
      --
      dbms_parallel_execute.run_task (task_name      => v_task_name
                                     ,sql_stmt       => v_run
                                     ,language_flag  => dbms_sql.native
                                     ,parallel_level => v_thread_count);

      v_task_status := dbms_parallel_execute.task_status (task_name => v_task_name);
      
      if v_task_status = dbms_parallel_execute.FINISHED
        then
          dbms_parallel_execute.drop_task (task_name => v_task_name);
        else
          dbms_parallel_execute.drop_task (task_name => v_task_name);
          raise_application_error (-20001, 'ORA in task ' || v_task_status);
      end if;
      
      --P_ABT_Card_DataMart(date_clc);
      
   END;

END;
/
