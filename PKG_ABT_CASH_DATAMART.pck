CREATE OR REPLACE PACKAGE AP_CRM_ABT.PKG_ABT_CASH_DATAMART authid current_user is

    DATE_CALC DATE := trunc(sysdate,'MM');

    PROCEDURE P_ABT_Data_Preparation  (date_clc DATE);
    PROCEDURE P_ABT_PART_1_Soc_Dem    (date_clc DATE);
    PROCEDURE P_ABT_PART_2_Offer      (date_clc DATE);
    PROCEDURE P_ABT_PART_3_Application(date_clc DATE);
    PROCEDURE P_ABT_PART_4_Last_Appl  (date_clc DATE);
    PROCEDURE P_ABT_PART_5_Last_Contr (date_clc DATE);
    PROCEDURE P_ABT_PART_6_Contracts  (date_clc DATE);
    PROCEDURE P_ABT_PART_7_Comm       (date_clc DATE);
    PROCEDURE P_ABT_PART_8_Appeal     (date_clc DATE);
    PROCEDURE P_ABT_PART_9_Deposit    (date_clc DATE);
    PROCEDURE P_ABT_PART_10_Mapp      (date_clc DATE);
    PROCEDURE P_ABT_PART_11_Payments  (date_clc DATE);
    PROCEDURE P_ABT_PART_12_FCB       (date_clc DATE);
    PROCEDURE P_ABT_Cash_DataMart     (date_clc DATE );
    PROCEDURE P_MAIN                  (date_clc DATE );


END;
/
CREATE OR REPLACE PACKAGE BODY AP_CRM_ABT.PKG_ABT_CASH_DATAMART IS

  --- 1. T_ABT_PART0_CLIENT
  --- 1. T_ABT_PART0_APPLICATION
  --- 1. T_ABT_PART1_SOC_DEM          
  ---
  --- 2. T_ABT_PART2_ACTIVE_OFFER
  --- 2. T_ABT_PART2_OFFER    
  ---
  --- 3. T_ABT_PART3_APPLICATION
  --- 4. T_ABT_PART4_LAST_APPL
  --- 5. T_ABT_PART5_LAST_CONTRACT
  --- 6. T_ABT_PART6_CONTRACTS
  ---
  --- 7. T_ABT_PART7_ALL_SMS_CLIENT
  --- 7. T_ABT_PART7_COMM_SMS
  --- 7. T_ABT_PART7_COM_LCS
  --- 7. T_ABT_PART7_COMM
  ---
  --- 8. T_ABT_PART8_APPEAL
  --- 9. T_ABT_PART9_DEP
  --- 10. T_ABT_PART10_WEB
  --- 11. T_ABT_PART11_PAYMENTS_CHANNEL
  --- 11. T_ABT_PART11_PAYMENTS
  ---
  --- 12. T_ABT_PART12_FCB
  ---
  --- T_ABT_CASH_DATAMART  -----



    PROCEDURE P_ABT_Data_Preparation(date_clc DATE ) IS

    i_step    NUMBER         := 0;

    BEGIN
    
    PKG_MZ_HINTS.pAlterSession(8);
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => 'P_ABT_DATA_PREPARATION');    
    DATE_CALC := nvl(date_clc, DATE_CALC);


    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART0_CLIENT');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART0_CLIENT');

    INSERT /*+ APPEND*/
    INTO T_ABT_PART0_CLIENT
    
      SELECT /*+ PARALLEL(4)*/
       SKP_CLIENT,
       ID_CUID,
       NAME_FULL,
       -- DUE TO DTIME_INSERTED MAY BE GREATER THAN DATE_1ST_CONTRACT
       LEAST(DATE_1ST_CONTRACT, DTIME_INSERTED) AS DTIME_INSERTED,
       MONTH_,
       DATE_1ST_CONTRACT
       
        FROM (SELECT CL.SKP_CLIENT,
                     CL.ID_CUID,
                     CL.NAME_FULL,
                     CL.DTIME_INSERTED,
                     DATE_CALC AS MONTH_,
                     MIN(CC.DATE_DECISION) AS DATE_1ST_CONTRACT
                
                FROM OWNER_DWH.DC_CLIENT CL
                --LEFT JOIN T_ABT_PART0_CLIENT ABT          ON CL.SKP_CLIENT = ABT.SKP_CLIENT
                JOIN OWNER_DWH.DC_CREDIT_CASE CC          ON CC.SKP_CLIENT = CL.SKP_CLIENT
                JOIN OWNER_DWH.DC_PRODUCT PR              ON PR.SKP_PRODUCT = CC.SKP_PRODUCT
                JOIN OWNER_DWH.DC_PRODUCT_PROFILE PP      ON PP.SKP_PRODUCT_PROFILE = PR.SKP_PRODUCT_PROFILE
                
               WHERE CL.DTIME_INSERTED /*BETWEEN ADD_MONTHS(DATE_CALC, -1) AND*/ < DATE_CALC
                 AND CC.DATE_DECISION /*BETWEEN ADD_MONTHS(DATE_CALC, -1) AND*/ < DATE_CALC
                 AND CC.SKP_CREDIT_STATUS NOT IN (1, 3, 5, 6, 10)
                 AND PP.FLAG_IS_DEBIT != 'Y'
                 --AND ABT.SKP_CLIENT IS NULL
                 AND CL.SKP_CLIENT > 0
                
               GROUP BY CL.SKP_CLIENT,
                        CL.ID_CUID,
                        CL.NAME_FULL,
                        CL.DTIME_INSERTED);

    PKG_MZ_HINTS.PSTEPEND(ANROWSRESULT => SQL%ROWCOUNT,
                          ACTABLE      => 'T_ABT_PART0_CLIENT');



    ---------- STEP 1 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART0_APPLICATION');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART0_APPLICATION');

    INSERT /*+ APPEND*/
    INTO T_ABT_PART0_APPLICATION T
    WITH W$APPLICATION AS
    (
    SELECT /*+ materializE FULL(CC) PARALLEL(2)*/
           CC.SKP_CREDIT_CASE,
           CC.SKP_APPLICATION_LAST,
           CC.SKP_CLIENT,
           CC.DATE_DECISION,
           CC.DTIME_PROPOSAL,
           CC.DTIME_CLOSE,
           CC.SKP_CREDIT_TYPE,
           CC.SKP_CREDIT_STATUS,
           ST.CODE_CREDIT_STATUS,
           CC.AMT_CREDIT_TOTAL,
           CC.AMT_CREDIT,
           CC.AMT_ANNUITY,--
           CC.FLAG_APPROVE,
           CC.FLAG_BOOKED,
           CC.FLAG_EARLY_REPAID,
           DENSE_RANK() OVER(PARTITION BY CC.SKP_CLIENT ORDER BY CC.DTIME_PROPOSAL) AS RANK_ALL,
           DENSE_RANK() OVER(PARTITION BY CC.SKP_CLIENT ORDER BY CC.DTIME_PROPOSAL DESC) AS RANK_ALL_DESC,
           --CC.FLAG_LAST_APPLICATION_ON_CLNT,
           CC.TEXT_CANCELLATION_REASON,
           CC.TEXT_CONTRACT_NUMBER,
           CC.TEXT_IDENTIFICATION_NUMBER,
           CC.CODE_PRODUCT_PURPOSE,
           CC.SKP_ACCOUNTING_METHOD,
           CC.RATE_INTEREST,
           CC.CNT_INSTALMENT,
           CC.HOMER_PROD_NAME,
           CC.PRODUCT_CLASSIFICATION -- ZERO PROMO ETC.
           ,CC.FLAG_IS_DEBIT
           ,CC.CODE_PRODUCT
           ,CC.CODE_PRODUCT_PROFILE
           ,CC.NAME_GOODS_CATEGORY
           ,CC.CODE_SALESROOM
           ,GR.CODE_SALESROOM_GROUP
           ,CC.CODE_SELLER
           ,CC.SIGN_GIFT
           
      FROM AP_PUBLIC.MV_V_CRR_DISCHANNEL_KZ CC
      JOIN OWNER_DWH.CL_CREDIT_STATUS       ST  ON CC.SKP_CREDIT_STATUS   = ST.SKP_CREDIT_STATUS
      JOIN OWNER_DWH.DC_SALESROOM           SR  ON CC.SKP_SALESROOM       = SR.SKP_SALESROOM
      JOIN OWNER_DWH.CL_SALESROOM_GROUP     GR  ON GR.SKP_SALESROOM_GROUP = SR.SKP_SALESROOM_GROUP
      
     WHERE CC.DATE_DECISION < DATE_CALC
       AND CC.SKP_CLIENT > 0
       AND CC.SKP_CREDIT_CASE > 0
       --AND CC.FLAG_IS_DEBIT = 'Y'
    ),
    W$CARD_ACT AS
    (
    select /*+ materializE FULL(C) PARALLEL(2)*/
     c.SKP_CREDIT_CASE,
     MIN(trunc(c.DTIME_ACTIVATION)) AS date_first_act,
     MAX(trunc(c.DTIME_ACTIVATION)) AS date_last_act

      from OWNER_DWH.DC_CARD c
     where c.ID_SOURCE not in ('XNA', 'XAP')
       and c.DATE_DECISION < DATE_CALC
       AND c.DTIME_ACTIVATION != date '1000-01-01'
       and c.SKP_CREDIT_TYPE = 3
     GROUP BY c.SKP_CREDIT_CASE
    ),
    W$CARD_TRX as -- trx with right type , which decrease O2B, only for CARDS
    (
    SELECT /*+ materializE */
           SKP_CREDIT_CASE
          ,AMT_TRANSACTION    -- ALL HISTORICAL TRANSACTIONS
          ,CNT_TRANSACTION --
          ,DATE_FIRST_TRX --
          ,DATE_LAST_TRX --
          ,DATE_FIRST_ACT --
          ,DATE_LAST_ACT --
          ,NVL2(DATE_FIRST_ACT, 1, 0) AS IS_PIN_CARD -- AS CNT_CARD_PIN
          ,NVL2(DATE_FIRST_TRX, 1, 0) AS IS_USE_CARD -- AS CNT_CARD_USE
          ,AMT_PRINCIPAL_DEBT -- Current Debt without %
          ,AMT_TOTAL_DEBT     -- Current Debt with %
          ,AMT_LEDGER_BALANCE -- Current Sum of amt debit or credit
          ,AMT_CREDIT_TOTAL - AMT_PRINCIPAL_DEBT + case when AMT_LEDGER_BALANCE < AMT_TOTAL_DEBT
                                                        then AMT_TOTAL_DEBT - AMT_LEDGER_BALANCE
                                                        else 0
                                                    end as AMT_OPEN_TO_BUY
            
      FROM (
    select /*+ materializE FULL(TT) PARALLEL(2)*/
           tt.SKP_CREDIT_CASE
          ,MAX(CONTRACTS.AMT_CREDIT_TOTAL) AS AMT_CREDIT_TOTAL
          ,sum(case when tt.CODE_AMT_DIRECTION = 'D' then tt.AMT_ACCOUNT_ITEM
                    when tt.CODE_AMT_DIRECTION = 'C' then tt.AMT_ACCOUNT_ITEM*(-1)
               end) as AMT_LEDGER_BALANCE
          ,sum(case when tt.CODE_AMT_DIRECTION = 'D'
                    then tt.AMT_ACCOUNT_ITEM - tt.AMT_PAIRED
               end) AMT_TOTAL_DEBT
          ,sum(case when pt.CODE_PRICELIST_ITEM_TYPE in ('REL_INPRE','REWARD_SETTLEMENT','RTL','ATM','CSD'
                                                        ,'CWCD','CWKI','ICD','IIS','INS','IPD','OTEP','OTSE')
                    then tt.AMT_ACCOUNT_ITEM - tt.AMT_PAIRED
               end) as AMT_PRINCIPAL_DEBT
          ,SUM(case when pt.CODE_PRICELIST_ITEM_TYPE in ('REL_INPRE','REWARD_SETTLEMENT','RTL','ATM','CSD'
                                                        ,'CWCD','CWKI','ICD','IIS','INS','IPD','OTEP','OTSE')
                    then tt.AMT_TRANSACTION_BILLING
               end) as AMT_TRANSACTION
          ,COUNT(case when pt.CODE_PRICELIST_ITEM_TYPE in ('REL_INPRE','REWARD_SETTLEMENT','RTL','ATM','CSD'
                                                        ,'CWCD','CWKI','ICD','IIS','INS','IPD','OTEP','OTSE')
                    then tt.SKF_CARD_ACCOUNT_ITEM
               end) as CNT_TRANSACTION
          ,MIN(case when pt.CODE_PRICELIST_ITEM_TYPE in ('REL_INPRE','REWARD_SETTLEMENT','RTL','ATM','CSD'
                                                        ,'CWCD','CWKI','ICD','IIS','INS','IPD','OTEP','OTSE')
                    then tt.DTIME_TRANSACTION
               end) as DATE_FIRST_TRX
          ,MAX(case when pt.CODE_PRICELIST_ITEM_TYPE in ('REL_INPRE','REWARD_SETTLEMENT','RTL','ATM','CSD'
                                                        ,'CWCD','CWKI','ICD','IIS','INS','IPD','OTEP','OTSE')
                    then tt.DTIME_TRANSACTION
               end) as DATE_LAST_TRX
               
          ,MIN(W$CARD_ACT.date_first_act) AS date_first_act
          ,MAX(W$CARD_ACT.date_LAST_act)  AS date_LAST_act

      from OWNER_DWH.F_CARD_ACCOUNT_ITEM_TT TT
      JOIN W$CARD_ACT
        ON TT.SKP_CREDIT_CASE = W$CARD_ACT.SKP_CREDIT_CASE
      join W$APPLICATION CONTRACTS
        on tt.SKP_CREDIT_CASE = CONTRACTS.skp_credit_case
       and tt.skp_credit_type = CONTRACTS.SKP_CREDIT_TYPE
       and tt.DATE_DECISION   = CONTRACTS.DATE_DECISION
       and CONTRACTS.skp_credit_type = 3
       and CONTRACTS.flag_booked     = 1
      JOIN OWNER_DWH.CL_PRICELIST_ITEM_TYPE PT
        on pt.SKP_PRICELIST_ITEM_TYPE = tt.SKP_PRICELIST_ITEM_TYPE

     where tt.CODE_STATUS = 'a'
       and tt.DTIME_TRANSACTION < DATE_CALC
       and tt.DATE_DECISION < DATE_CALC

    group by  tt.SKP_CREDIT_CASE
             )
    )
    , W$PAYMENTS as ( 
    -- payment to principal debt
    SELECT /*+ MATERIALIZE FULL(FL) PARALLEL(2)*/
     FL.SKP_CREDIT_CASE,
     MAX(FL.DATE_INSTALMENT) AS DATE_LAST_PAYMENT,
     SUM(FL.AMT_PAYMENT)     AS AMT_PAYMENTS
    ,SUM(DECODE(cl.CODE_INSTALMENT_LINE_GROUP, 'INTEREST', FL.amt_payment))      as AMT_PAYMENT_INTEREST
    ,MIN(DECODE(cl.CODE_INSTALMENT_LINE_GROUP, 'PRINCIPAL', FL.DATE_INSTALMENT)) as DATE_FIRST_PAYM_PRINCIPAL
    ,MAX(DECODE(cl.CODE_INSTALMENT_LINE_GROUP, 'PRINCIPAL', FL.DATE_INSTALMENT)) as DATE_LAST_PAYM_PRINCIPAL

      FROM owner_dwh.f_instalment_line_ad fl
      join W$APPLICATION CONTRACTS
        on FL.SKP_CREDIT_CASE = CONTRACTS.skp_credit_case
       and FL.skp_credit_type = CONTRACTS.SKP_CREDIT_TYPE
       and FL.DATE_DECISION   = CONTRACTS.DATE_DECISION
       and CONTRACTS.flag_booked     = 1
      JOIN owner_dwh.cl_instalment_line_type cl
        ON cl.skp_instalment_line_type = fl.skp_instalment_line_type

     where fl.SKP_CREDIT_TYPE in (1, 2, 3)
       and fl.FLAG_DELETED = 'N'
       AND fl.CODE_INSTALMENT_LINE_STATUS = 'a'
       AND fl.SKP_INSTALMENT_REGULARITY in (1, 2, 5) --?
       and cl.CODE_INSTALMENT_LINE_GROUP in ('PRINCIPAL', 'INTEREST')
       and fl.DATE_INSTALMENT < DATE_CALC

     GROUP BY fl.skp_credit_case
    ),
    W$BOX_INS AS (
    SELECT /*+ materializE FULL(INS) FULL(DI) PARALLEL(2)*/
     INS.SKP_CREDIT_CASE,
     SUM(INS.AMT_PREMIUM) AS AMT,
     COUNT(DISTINCT INS.SKP_INSURANCE) AS CNT
     
      FROM OWNER_DWH.F_INSURANCE_AT INS,
           OWNER_DWH.DC_INSURANCE   DI,
           OWNER_DWH.DC_SERVICE     S
     WHERE DI.SKP_INSURANCE = INS.SKP_INSURANCE
       AND S.SKP_SERVICE = INS.SKP_SERVICE
       AND S.CODE_SERVICE LIKE 'BX%'
       AND DI.SKP_INSURANCE_STATUS != 2
       and ins.DATE_DECISION < DATE_CALC
     GROUP BY INS.SKP_CREDIT_CASE
    ),
    W$LIFE_INS AS (
    SELECT /*+ materializE FULL(INS) FULL(DI) PARALLEL(2)*/
     INS.SKP_CREDIT_CASE, SUM(INS.AMT_PREMIUM) AS AMT
      FROM OWNER_DWH.F_INSURANCE_AT INS,
           OWNER_DWH.DC_INSURANCE   DI,
           OWNER_DWH.DC_SERVICE     S
     WHERE DI.SKP_INSURANCE = INS.SKP_INSURANCE
       AND S.SKP_SERVICE = INS.SKP_SERVICE
       AND S.CODE_SERVICE NOT LIKE 'BX%'
       AND DI.SKP_INSURANCE_STATUS != 2
       and ins.DATE_DECISION < DATE_CALC
    -- and INS.SKP_CREDIT_TYPE IN (2, 3)
     GROUP BY INS.SKP_CREDIT_CASE
    ),
    W$GIFTS AS
    (
    select /*+ materializE FULL(contr_serv) FULL(serv) FULL(servt) PARALLEL(2)*/
           contr_serv.skP_credit_case,
           MAX(REGEXP_replace(serv.CODE_SERVICE, '\D')) AS num_gift -- Gives Number of Gifts

      from owner_dwh.dc_contract_service contr_serv
      join owner_Dwh.Dc_Service serv
        on serv.SKP_SERVICE = contr_Serv.skp_service
      join owner_dwh.dc_service_type servt
        on servt.SKP_SERVICE_TYPE = serv.SKP_SERVICE_TYPE
       and servt.CODE_SERVICE_TYPE = 'GIFTP'
     GROUP BY contr_serv.skP_credit_case
    ),
    W$APPL_CLIENT AS
    (
    SELECT /*+ materializE FULL(AC) PARALLEL(2)*/
           AC.SKP_CREDIT_CASE
         , AC.SKP_APPLICATION
         , AC.DATE_DECISION
         , AC.AMT_INCOME_MAIN
         , AC.AMT_INCOME_OTHER
         , AC.NAME_HOUSING_TYPE
         , AC.NAME_EDUCATION_TYPE
         , AC.CODE_GENDER
         , AC.DATE_BIRTH
         , AC.CNT_CHILDREN
         , AC.CODE_INCOME_TYPE
         , AC.NAME_INCOME_TYPE
         , AC.DATE_EMPLOYED_FROM
         , DENSE_RANK() OVER(PARTITION BY AC.SKP_CREDIT_CASE ORDER BY AC.DATE_DECISION DESC, AC.SKP_APPLICATION DESC, 
                                       AC.CODE_GENDER, AC.DATE_EMPLOYED_FROM DESC, AC.DATE_BIRTH DESC) AS RN
         
      FROM OWNER_DWH.F_APPLICATION_CLIENT_TT AC
     WHERE AC.DATE_DECISION < DATE_CALC
    ),
    W$APPL_ADDRES AS
    (
    SELECT /*+ materializE FULL(ADS) PARALLEL(2)*/
           ADS.SKP_APPLICATION
         , ADS.SKP_CREDIT_CASE
         , ADS.NAME_TOWN
         , ADS.NAME_REGION
         , DENSE_RANK() OVER(PARTITION BY ADS.SKP_CREDIT_CASE ORDER BY ADS.DTIME_MODIFIED DESC, ADS.SKF_APPLICATION_ADDRESS DESC) AS RN
         
      FROM OWNER_DWH.F_APPLICATION_ADDRESS_TT ADS
     WHERE ADS.DATE_DECISION < DATE_CALC
    )

    SELECT /*+ USE_HASH(W$APPLICATION W$CARD_TRX W$PAYMENTS W$BOX_INS W$LIFE_INS W$GIFTS W$APPL_CLIENT W$APPL_ADDRES) PARALLEL(8)*/
            W$APPLICATION.SKP_CREDIT_CASE
           ,W$APPLICATION.SKP_APPLICATION_LAST
           ,W$APPLICATION.SKP_CLIENT
           ,W$APPLICATION.DATE_DECISION
           ,W$APPLICATION.DTIME_PROPOSAL
           ,W$APPLICATION.DTIME_CLOSE
           ,W$APPLICATION.SKP_CREDIT_TYPE
           ,W$APPLICATION.SKP_CREDIT_STATUS
           ,W$APPLICATION.CODE_CREDIT_STATUS
           ,W$APPLICATION.AMT_CREDIT_TOTAL
           ,W$APPLICATION.AMT_CREDIT
           ,W$APPLICATION.AMT_ANNUITY
           ,W$APPLICATION.FLAG_APPROVE
           ,W$APPLICATION.FLAG_BOOKED
           ,W$APPLICATION.FLAG_EARLY_REPAID
           ,W$APPLICATION.RANK_ALL
           ,W$APPLICATION.RANK_ALL_DESC
           --,W$APPLICATION.FLAG_LAST_APPLICATION_ON_CLNT
           ,W$APPLICATION.TEXT_CANCELLATION_REASON
           ,W$APPLICATION.TEXT_CONTRACT_NUMBER
           ,W$APPLICATION.TEXT_IDENTIFICATION_NUMBER
           ,W$APPLICATION.CODE_PRODUCT_PURPOSE
           ,W$APPLICATION.SKP_ACCOUNTING_METHOD
           ,W$APPLICATION.RATE_INTEREST
           ,W$APPLICATION.CNT_INSTALMENT
           ,W$APPLICATION.HOMER_PROD_NAME
           ,W$APPLICATION.PRODUCT_CLASSIFICATION -- ZERO PROMO ETC.
           ,W$APPLICATION.FLAG_IS_DEBIT
           ,W$APPLICATION.CODE_PRODUCT
           ,W$APPLICATION.CODE_PRODUCT_PROFILE
           ,W$APPLICATION.NAME_GOODS_CATEGORY
           ,W$APPLICATION.CODE_SALESROOM
           ,W$APPLICATION.CODE_SALESROOM_GROUP
           ,W$APPLICATION.CODE_SELLER
           ,W$APPLICATION.SIGN_GIFT
           
           ,W$CARD_TRX.DATE_FIRST_TRX --
           ,W$CARD_TRX.DATE_LAST_TRX --
           ,W$CARD_TRX.DATE_FIRST_ACT --
           ,W$CARD_TRX.DATE_LAST_ACT --
           ,W$CARD_TRX.IS_PIN_CARD --
           ,W$CARD_TRX.IS_USE_CARD --
           ,W$CARD_TRX.CNT_TRANSACTION --
           ,W$CARD_TRX.AMT_TRANSACTION    -- ALL HISTORICAL TRANSACTIONS
           ,W$CARD_TRX.AMT_PRINCIPAL_DEBT -- Current Debt without %
           ,W$CARD_TRX.AMT_TOTAL_DEBT     -- Current Debt with %
           ,W$CARD_TRX.AMT_LEDGER_BALANCE -- Current Sum of amt debit or credit
           ,W$CARD_TRX.AMT_OPEN_TO_BUY
           ,NVL(W$CARD_TRX.AMT_TOTAL_DEBT, W$APPLICATION.AMT_CREDIT_TOTAL - W$PAYMENTS.AMT_PAYMENTS) AS AMT_CURRENT_DEBT
          
          ,W$PAYMENTS.AMT_PAYMENTS
          ,W$PAYMENTS.DATE_LAST_PAYMENT
          ,W$PAYMENTS.AMT_PAYMENT_INTEREST
          ,W$PAYMENTS.DATE_FIRST_PAYM_PRINCIPAL
          ,W$PAYMENTS.DATE_LAST_PAYM_PRINCIPAL
             
          ,NVL2(W$BOX_INS.SKP_CREDIT_CASE, 1, 0)  AS FLAG_BOX_INSURANCE
          ,NVL(W$BOX_INS.AMT,0)                   AS AMT_BOX_INSURANCE
          ,NVL(W$BOX_INS.CNT,0)                   AS CNT_BOX_INSURANCE
          ,NVL2(W$LIFE_INS.SKP_CREDIT_CASE, 1, 0) AS FLAG_LIFE_INSURANCE
          ,NVL(W$LIFE_INS.AMT, 0)                 AS AMT_LIFE_INSURANCE
          ,W$GIFTS.NUM_GIFT
          
         , W$APPL_CLIENT.AMT_INCOME_MAIN
         , W$APPL_CLIENT.AMT_INCOME_OTHER
         , W$APPL_CLIENT.NAME_HOUSING_TYPE
         , W$APPL_CLIENT.NAME_EDUCATION_TYPE
         , W$APPL_CLIENT.CODE_GENDER
         , W$APPL_CLIENT.DATE_BIRTH
         , W$APPL_CLIENT.CNT_CHILDREN
         , W$APPL_CLIENT.CODE_INCOME_TYPE
         , W$APPL_CLIENT.NAME_INCOME_TYPE
         , W$APPL_CLIENT.DATE_EMPLOYED_FROM
         
         , W$APPL_ADDRES.NAME_TOWN
         , W$APPL_ADDRES.NAME_REGION
           
      FROM W$APPLICATION
      LEFT JOIN W$CARD_TRX
        ON W$CARD_TRX.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
       AND W$APPLICATION.SKP_CREDIT_TYPE = 3
      LEFT JOIN W$PAYMENTS
        ON W$PAYMENTS.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
      LEFT JOIN W$BOX_INS
        ON W$BOX_INS.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
      LEFT JOIN W$LIFE_INS
        ON W$LIFE_INS.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
      LEFT JOIN W$GIFTS
        ON W$GIFTS.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
      LEFT JOIN W$APPL_CLIENT
        ON W$APPL_CLIENT.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
       AND W$APPL_CLIENT.SKP_APPLICATION = W$APPLICATION.SKP_APPLICATION_LAST
       AND W$APPL_CLIENT.RN = 1
      LEFT JOIN W$APPL_ADDRES
        ON W$APPL_ADDRES.SKP_CREDIT_CASE = W$APPLICATION.SKP_CREDIT_CASE
       AND W$APPL_ADDRES.SKP_APPLICATION = W$APPLICATION.SKP_APPLICATION_LAST
       AND W$APPL_ADDRES.RN = 1;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART0_APPLICATION');

    
      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;

    
    
    
    
    PROCEDURE P_ABT_PART_1_Soc_Dem(date_clc DATE ) IS

    i_step    NUMBER         := 0;

    BEGIN     
    DATE_CALC := nvl(date_clc, DATE_CALC);
    
    -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => 'P_ABT_PART_1_SOC_DEM');

    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART1_SOC_DEM');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART1_SOC_DEM');
                            
    INSERT /*+ APPEND*/ INTO T_ABT_PART1_SOC_DEM
      WITH W$CLIENT AS
      (
      SELECT /*+ MATERIALIZE*/
             T.SKP_APPLICATION_LAST AS SKP_APPLICATION     
            ,T.SKP_CLIENT
            ,T.DATE_DECISION
            ,T.NAME_TOWN
            ,case when t.NAME_REGION in (
                                         'Северо-Казахстанская'
                                        ,'Мангистауская'
                                        ,'Западно-Казахстанс.'
                                        ,'Акмолинская область'
                                        ,'Атырауская область'
                                        ,'Алматы'
                                        ,'Кызылординская'
                                        ,'Костанайская'
                                        ,'Жамбылская'
                                        ,'Павлодарская'
                                        ,'Астана'
                                        ,'Актюбинская область'
                                        ,'Алматинская область'
                                        ,'Южно-Казахстанская'
                                        ,'Карагандинская'
                                        ,'Восточно-Казахстанс.'
                                        )
                  then t.NAME_REGION else 'XNA' end as NAME_REGION     
             , case when T.DATE_BIRTH > date '1900-01-01' then round(MONTHS_BETWEEN(DATE_CALC, T.DATE_BIRTH)/12,1) end as AGE_years
             , case when T.DATE_BIRTH > date '1900-01-01' then round(MONTHS_BETWEEN(DATE_CALC, T.DATE_BIRTH),2)    end as AGE_month   
             , T.AMT_INCOME_MAIN
             , T.AMT_INCOME_OTHER
             , T.NAME_HOUSING_TYPE
             , T.NAME_EDUCATION_TYPE
             , T.CODE_GENDER
             , T.CNT_CHILDREN
             , T.CODE_INCOME_TYPE
             , T.NAME_INCOME_TYPE

         from T_ABT_PART0_APPLICATION T
        WHERE --T.FLAG_LAST_APPLICATION_ON_CLNT = 'Y'
              RANK_ALL_DESC = 1
      ),
      W$REGION_INCOME as 
      (
           ------ AVG Income by region last 6 month -----------------
       select /*+ MATERIALIZE*/
        NAME_REGION,
        AVG(AMT_INCOME_MAIN + AMT_INCOME_OTHER) AS AVG_INCOME_IN_REGION,
        COUNT(DISTINCT SKP_APPLICATION) CC
         FROM W$CLIENT
        WHERE DATE_DECISION BETWEEN ADD_MONTHS(DATE_CALC, -6) AND DATE_CALC
        group by NAME_REGION
       ),
       W$CLIENT_LANG as
       (
          select /*+ MATERIALIZE*/
                 cl.skp_client
                ,nvl(lg.NAME_LANGUAGE,'XNA') as NAME_PREFERRED_LANGUAGE
                ,FS.CODE_FAMILY_STATUS
                  
          from owner_dwh.dc_client cl
          join owner_dwh.cl_family_status fs                
            on fs.SKP_FAMILY_STATUS = CL.SKP_FAMILY_STATUS
          left join owner_dwh.cl_language lg on cl.code_preferred_language = lg.code_language
                                             and lg.flag_deleted != 'Y'
                                             and lower(lg.code_status) = 'a'
          where cl.flag_deleted != 'Y'
            and cl.code_status = 'a'
            AND CL.dtime_inserted < DATE_CALC
        ),
        W$UNION AS
        (
            ----- Client info from last application, rn=1 for unique row --------------
        select /*+ USE_HASH(CL P1 RG) MATERIALIZE*/
               cl.skp_client               
              ,P1.AGE_years
              ,P1.AGE_month
              ,P1.NAME_HOUSING_TYPE
              ,P1.NAME_EDUCATION_TYPE
              ,P1.CODE_GENDER
              ,P1.CNT_CHILDREN
              ,P1.AMT_INCOME_MAIN
              ,P1.AMT_INCOME_OTHER
              ,P1.CODE_INCOME_TYPE
              ,P1.NAME_INCOME_TYPE
              ,P1.NAME_REGION
              ,P1.NAME_TOWN

              ,cL.NAME_PREFERRED_LANGUAGE
              ,CL.CODE_FAMILY_STATUS
              ,(P1.AMT_INCOME_MAIN + P1.AMT_INCOME_OTHER)/
               rg.avg_income_in_region as SOC_SHARE_INCOME_by_region
                
          from W$CLIENT_LANG cl
          join W$CLIENT      p1                       on P1.SKP_CLIENT = cl.skp_client
          LEFT join W$REGION_INCOME rg                on rg.NAME_REGION = P1.NAME_REGION
         )
          select /*+ PARALLEL(4)*/
                 s1.Skp_Client
                ,MAX(S1.Age_Years)               as SD_AGE                      -- AGE
                ,MAX(CASE WHEN S1.CODE_GENDER = 'M' AND S1.AGE_years >= 63 THEN 'Y' 
                          WHEN S1.CODE_GENDER = 'F' AND s1.AGE_years >= least(63,(58+(MONTHS_BETWEEN(trunc(DATE_CALC,'YY'), date'2017-01-01')/24)))
                          THEN 'Y'
                          ELSE 'N' END
                    )                            as SD_FLAG_PENSIONER           -- FLAG PENSIONER
                ,MAX(S1.NAME_TOWN)               as SD_NAME_CITY                -- NAME_CITY
                ,MAX(S1.AMT_INCOME_MAIN)         as SD_AMT_INCOME_MAIN          -- AMT_INCOME_MAIN
                ,MAX(S1.AMT_INCOME_OTHER)        as SD_AMT_INCOME_OTHER         -- AMT_INCOME_OTHER
                ,MAX(S1.NAME_PREFERRED_LANGUAGE) as SD_PREFERRED_LANGUAGE       -- NAME_PREFERRED_LANGUAGE
                -------------------------------------------------------------------------------------------------------
                ,MAX(S1.Age_Month)               as SD_AGE_month                -- AGE_month
                ,MAX(S1.CODE_GENDER)             as SD_CODE_GENDER              -- CODE_GENDER
                ,MAX(S1.CODE_INCOME_TYPE)        as SD_INCOME_TYPE_CODE         -- CODE_INCOME_TYPE
                ,MAX(S1.NAME_INCOME_TYPE)        as SD_INCOME_TYPE              -- NAME_INCOME_TYPE
                ,MAX(S1.CNT_CHILDREN)            as SD_CNT_CHILDREN             -- CNT_CHILDREN
                ,MAX(S1.NAME_EDUCATION_TYPE)     as SD_EDUCATION_TYPE           -- NAME_EDUCATION_TYPE

                ,MAX(S1.CODE_FAMILY_STATUS)      as SD_CODE_FAMILY_STATUS       -- CODE_FAMILY_STATUS
                ,MAX(S1.NAME_HOUSING_TYPE)       as SD_NAME_HOUSING_TYPE        -- NAME_HOUSING_TYPE
                ,MAX(S1.NAME_REGION)             as SD_NAME_REGION              -- NAME_REGION
                ,MAX(S1.Soc_Share_Income_By_Region) as SD_SHARE_INCOME_6_M_reg  -- SOC_SHARE_INCOME_by_region last 6 month

          from W$UNION s1
         group by s1.SKP_CLIENT; 

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART1_SOC_DEM');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;




    --- T_ABT_PART0_APPLICATION     T_ABT_PART2_ACTIVE_OFFER
    PROCEDURE P_ABT_PART_2_Offer (date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_2_OFFER';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
    
    
    PKG_MZ_HINTS.pAlterSession(8);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);




    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART2_ACTIVE_OFFER');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART2_ACTIVE_OFFER');

     insert /*+ append*/
       into T_ABT_PART2_ACTIVE_OFFER

    select /*+ MATERIALIZE USE_HASH(T SO) FULL(T) FULL(SO) PARALLEL(4) */
           T.SKP_CLIENT
          ,T.SKP_SAS_CAMPAIGN
          ,T.SKF_SAS_PARTICIPANT
          ,T.AMT_CREDIT_MAX
          ,T.CODE_PRODUCT_GROUP
          ,SO.NAME_OFFER
          ,TO_NUMBER(SO.CODE_OFFER_PRIORITY) AS PRIORITY
          
          ,SO.DTIME_CREATION     AS CLIENT_OFFER_VALID_FROM
          ,SO.DTIME_DEACTIVATION AS CLIENT_OFFER_VALID_TO
          ,T.DTIME_CREATION      AS CLIENT_CMP_VALID_FROM
          ,T.DTIME_DEACTIVATION  AS CLIENT_CMP_VALID_TO
          
          ,DENSE_RANK() OVER(PARTITION BY T.SKP_CLIENT ORDER BY TRUNC(SO.DTIME_CREATION, 'MM') DESC, SO.CODE_OFFER_PRIORITY, 
                        T.DTIME_DEACTIVATION DESC, T.SKF_SAS_PARTICIPANT DESC, SO.SKP_SAS_OFFER DESC) AS RN

      FROM OWNER_DWH.F_SAS_PARTICIPANT_AT T
      JOIN OWNER_DWH.DC_SAS_OFFER         SO
        ON SO.SKF_SAS_PARTICIPANT = T.SKF_SAS_PARTICIPANT
       AND SO.CODE_COMM_CHANNEL = 'BSL'
       AND DATE_CALC BETWEEN SO.DTIME_CREATION AND ADD_MONTHS(TRUNC(SO.DTIME_DEACTIVATION, 'MM'), 1)

    WHERE T.CODE_PRODUCT NOT IN ('CALXP1000','CALXP1500','CALXP750')
      AND LOWER(SO.NAME_OFFER) NOT LIKE '%test%'
      and T.CODE_PRODUCT_GROUP NOT IN ('Виртуальный XP', 'XNA')
      AND LOWER(T.CODE_PRODUCT_GROUP) NOT LIKE '%test%'
      and TRUNC(SO.DTIME_DEACTIVATION) - TRUNC(SO.DTIME_CREATION) > 3
      AND DATE_CALC BETWEEN T.DTIME_CREATION AND ADD_MONTHS(TRUNC(T.DTIME_DEACTIVATION, 'MM'), 1);


      PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                            acTable      => 'T_ABT_PART2_ACTIVE_OFFER');





    ---------- STEP 2 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART2_OFFER');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                                   acAction  => 'T_ABT_PART2_OFFER');

      insert /*+ append*/into T_ABT_PART2_OFFER
      
      with  W$CONTRACTS as ( -- has ever cash
      select /*+ MATERIALIZE */
             cc.skp_client
            ,max(case when cc.skp_credit_type = 2 then 1 else 0 end) has_cash
            ,max(case when cc.skp_credit_type = 3 then 1 else 0 end) has_card
            ,max(case when cc.skp_credit_type = 2 then cc.date_decision end) as date_last_desicion_cash
            ,max(case when cc.skp_credit_type = 3 then cc.date_decision end) as date_last_desicion_card
            ,max(case when cc.skp_credit_type = 2 AND CC.DTIME_CLOSE < DATE_CALC then CC.DTIME_CLOSE end) date_last_close_cash
            ,max(case when cc.skp_credit_type = 3 AND CC.DTIME_CLOSE < DATE_CALC then CC.DTIME_CLOSE end) date_last_close_card
            ,count(CASE WHEN CC.DTIME_CLOSE > DATE_CALC THEN CC.skp_credit_case END) as cnt_contract_ACTIVE
            ,sum(CASE WHEN CC.DTIME_CLOSE > DATE_CALC THEN CC.amt_credit_total END) as sum_amt_credit_ACTIVE
            ,sum(CASE WHEN CC.DTIME_CLOSE > DATE_CALC THEN CC.AMT_CURRENT_DEBT END) as current_debt_ACTIVE
            
       from T_ABT_PART0_APPLICATION cc
      where cc.date_decision < DATE_CALC
        and CC.FLAG_IS_DEBIT != 'Y'
        and cc.Flag_Booked = 1
      group by cc.skp_client
      ),
      W$CMP AS
      (
      SELECT /*+ MATERIALIZE*/
            T.SKP_CLIENT
           ,T.AMT_CREDIT_MAX
           ,T.name_offer
           ,T.CODE_PRODUCT_GROUP
           ,T.CLIENT_OFFER_VALID_FROM
           ,T.CLIENT_CMP_VALID_TO
       ,  case when UPPER(T.CODE_PRODUCT_GROUP) like 'REFINANCE%CB%'              then 'REFINANCE_CB'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'REFINANCE%'                 then 'REFINANCE'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'RBP_RAP%'                   then 'BSL_Monitoring'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'RBP_FR%'                    then 'Price_pilot'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'RBP_CB%'                    then 'RBP_CB'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'RBP%'                       then 'RBP'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_CASHBACK' then 'CE_RBP_CB'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_1M'       then 'CE_RBP'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'ORBP_GIFT_XSELL'            then 'oRBP'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'ORBP_REFINANCE_GIFT_XSELL'  then 'oRBP_REFIN'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'ORBP_FLATPRICE%'            then 'oRBP_FR'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'DOS CE CARD'                then 'Card_CE' 
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'X-SELL CARD DOS'            then 'Card_XS' 
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'RESTRUCTURING%'             then 'Restructuring' 
               else 'other' 
           end as NAME_PRODUCT_GROUP
       ,  case WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_CASHBACK' then 'CASH EXISTING'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'CASH_EXISTING_RBP_1M'       then 'CASH EXISTING'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like 'DOS CE CARD'                then 'CASH EXISTING'
               ELSE 'X-SELL' END AS CMP_TYPE_GROUP 
       ,  case WHEN UPPER(T.CODE_PRODUCT_GROUP) like '%REFINANCE%'                then 'REFINANCE'
               WHEN UPPER(T.CODE_PRODUCT_GROUP) like '%CARD%'                     then 'CARD'
               ELSE 'CASH' END AS CMP_TYPE_PRODUCT
       , CASE WHEN T.NAME_OFFER LIKE '[RD][CEL]%' THEN 1 ELSE 0 END               AS CMP_FLAG_RD_POOL
       FROM T_ABT_PART2_ACTIVE_OFFER t
      WHERE RN = 1 -- LAST ACTIVE CAMPAIGN
      ),
      W$ELIG AS
       (SELECT /*+ MATERIALIZE FULL(H) USE_HASH(H W$CMP) PARALLEL(2)*/
         H.SKP_CLIENT,
         max(H.RISK_GRADE) keep(dense_rank last order by h.DATE_EFFECTIVE) as RISK_GRADE,
         max(H.FLAG_ELIGIBILITY) keep(dense_rank last order by h.DATE_EFFECTIVE) as FLAG_ELIGIBILITY,
         max(H.FLAG_ELIGIBILITY_XSELL) keep(dense_rank last order by h.DATE_EFFECTIVE) as FLAG_ELIGIBILITY_XSELL,
         max(H.FLAG_ELIGIBILITY_CE) keep(dense_rank last order by h.DATE_EFFECTIVE) as FLAG_ELIGIBILITY_CE,
         max(H.DATE_EFFECTIVE) AS DATE_EFFECTIVE
        
          FROM AP_RISK.OB_ELIGIBILITY_HIST H
          JOIN W$CMP
            ON H.SKP_CLIENT = W$CMP.SKP_CLIENT
           AND W$CMP.CLIENT_OFFER_VALID_FROM > h.DATE_EFFECTIVE
         WHERE DATE_EFFECTIVE BETWEEN ADD_MONTHS(DATE_CALC, -3) AND ADD_MONTHS(DATE_CALC, 1)
         group by H.SKP_CLIENT
        )

      select /*+ USE_HASH(W$CMP W$CONTRACTS W$ELIG) parallel(4)*/
             W$CMP.skp_client
            ,DECODE(W$CMP.CMP_TYPE_GROUP, 'X-SELL', 'XS', 'CE') AS cmp_type_group   -- 1. flag XS/CE
            ,W$CMP.CMP_TYPE_PRODUCT --2. flag CASH/CARD/REFINANCE cmp
            ,W$CMP.CMP_FLAG_RD_POOL --3. flag RD pool
            ,W$CMP.AMT_CREDIT_MAX                               as CMP_AMT_OFFER                                      -- 4.AMT OFFERED LIMIT
            ,W$ELIG.RISK_GRADE                                  as CMP_RISK_GRADE                -- 
            ,nvl(W$ELIG.FLAG_ELIGIBILITY, 0)                    as FLAG_ELIGIBILITY                -- 
            ,nvl(W$ELIG.FLAG_ELIGIBILITY_XSELL, 0)              as FLAG_ELIGIBILITY_XSELL                -- 
            ,nvl(W$ELIG.FLAG_ELIGIBILITY_CE, 0)                 as FLAG_ELIGIBILITY_CE
            ,nvl(W$CONTRACTS.cnt_contract_ACTIVE, 0)            AS CMP_CNT_CONTRACT_ACT  -- 6.num  active contract
            ,nvl(W$CONTRACTS.sum_amt_credit_ACTIVE, 0)          AS CMP_AMT_CREDIT_ACT-- 7. amt_credit_from_active_contract
            ,nvl(W$CONTRACTS.current_debt_ACTIVE, 0)            as CMP_AMT_DEBT_ACT-- 8. amt_credit_debt_from_active_contract
            ,nvl(W$CONTRACTS.has_cash, 0)                       as CMP_FLAG_ever_cash  -- 9.flag_has_ever_cash
            ,nvl(W$CONTRACTS.has_card, 0)                       as CMP_FLAG_ever_card  -- 12.flag_has_ever_card

            ,months_between(DATE_CALC,W$CONTRACTS.date_last_desicion_cash) as CMP_CNT_M_LAST_CASH_OPEN --10. num_month_from_last_cash_desicion
            ,months_between(DATE_CALC,W$CONTRACTS.date_last_close_cash)    as CMP_CNT_M_LAST_CASH_CLOSE --11. num_month_from_last_cash_close
            ,months_between(DATE_CALC,W$CONTRACTS.date_last_desicion_card) as CMP_CNT_M_LAST_CARD_OPEN --13. num_month_from_last_card_des
            ,months_between(DATE_CALC,W$CONTRACTS.date_last_close_card)    as CMP_CNT_M_LAST_CARD_CLOSE --14. num_month_from_last_card_close

            ,W$CMP.name_offer                                              as CMP_NAME
            ,W$CMP.client_offer_valid_from                                 as CMP_date_valid_from
            ,W$CMP.client_CMP_valid_to                                     as CMP_date_valid_to

      from W$CMP
      left join W$CONTRACTS                        on W$CONTRACTS.skp_client = W$CMP.skp_client
      left join W$ELIG                             on W$ELIG.skp_client = W$CMP.skp_client;


      PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                            acTable      => 'T_ABT_PART2_OFFER');

	    
      

      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

      EXCEPTION
          WHEN OTHERS THEN
          ROLLBACK;
          PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
          DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
          --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
      end;
      
      
      

      ---- T_ABT_PART3_APPLICATION         T_ABT_PART2_OFFER
      PROCEDURE P_ABT_PART_3_Application(date_clc DATE) IS

      AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_3_APPLICATION';
      i_step    NUMBER         := 0;

      BEGIN
      DATE_CALC            := nvl(date_clc, DATE_CALC);
      
      
      PKG_MZ_HINTS.pAlterSession(8);
        -- Start Init Log ---------------------------
      PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);

      

      I_STEP := I_STEP + 1;
      PKG_MZ_HINTS.pTruncate('T_ABT_PART3_APPLICATION');
      PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                                     acAction  => 'T_ABT_PART3_APPLICATION');

      insert /*+ append*/ into T_ABT_PART3_APPLICATION
      with
      W$APPL as (
      select /*+ MATERIALIZE PARALLEL(4)*/
             cc.skp_client
            ,max(cc.date_decision)                                                                                                 as LA_date
            ,max(case when cc.skp_credit_type = 1 then cc.date_decision end)                                                       as LA_date_POS
            ,max(case when cc.skp_credit_type = 1 and cc.Flag_Booked = 1 then cc.date_decision end)                                as LA_date_POS_booked
            ,max(case when cc.skp_credit_type = 2 then cc.date_decision end)                                                       as LA_date_CASH
            ,max(case when cc.skp_credit_type = 2 and cc.Flag_Booked = 1 then cc.date_decision end)                                as LA_date_CASH_booked  -- XS or CE
            ,max(case when cc.skp_credit_type = 3 then cc.date_decision end)                                                       as LA_date_CARD
            ,max(case when cc.skp_credit_type = 3 and cc.Flag_Booked = 1 then cc.date_decision end)                                as LA_date_CARD_booked

            ,max(case when cc.skp_credit_type = 2 and cc.code_product_purpose = 'BOUND' then cc.date_decision end)                 as LA_date_CASH_XS -- XS or CE
            ,max(case when cc.skp_credit_type = 3 and cc.code_product_purpose = 'BOUND' then cc.date_decision end)                 as LA_date_CARD_XS -- XS or CE

            ,count(cc.skp_credit_case)                                                                                                 as cnt_app
            ,count(case when cc.skp_credit_type = 2 then cc.skp_credit_case end)                                                       as cnt_app_CASH
            ,count(case when cc.skp_credit_type = 3 then cc.skp_credit_case end)                                                       as cnt_app_CARD
            ,count(case when cc.skp_credit_type = 1 then cc.skp_credit_case end)                                                       as cnt_app_POS
            ,count(case when cc.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                                            as cnt_app_XS
            ,count(case when cc.skp_credit_type = 2 and cc.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                 as cnt_app_XS_CASH
            ,count(case when cc.skp_credit_type = 3 and cc.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                 as cnt_app_XS_CARD

            ,count(case when cc.Flag_Booked = 1 then cc.skp_credit_case end)                            as cnt_app_appr
            ,count(case when cc.skp_credit_type = 2 and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_CASH_appr
            ,count(case when cc.skp_credit_type = 3 and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_CARD_appr
            ,count(case when cc.skp_credit_type = 1 and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_POS_appr
            ,count(case when cc.code_product_purpose = 'BOUND' and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_XS_appr
            ,count(case when cc.skp_credit_type = 2 and CC.code_product_purpose = 'BOUND' and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_XS_CASH_appr
            ,count(case when cc.skp_credit_type = 3 and CC.code_product_purpose = 'BOUND' and cc.Flag_Booked = 1 then cc.skp_credit_case end) as cnt_app_XS_CARD_appr

            ,count(case when CC.FLAG_APPROVE != 1 then cc.skp_credit_case end) as cnt_app_rej
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 then cc.skp_credit_case end) as cnt_app_rej_CASH
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-3)  >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_CASH_3M
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-6)  >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_CASH_6M
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_CASH_12M
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_CASH_24M
            ,count(case when CC.FLAG_APPROVE != 1  and cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_CASH_36M


            ,count(case when CC.FLAG_APPROVE != 1  and ADD_MONTHS(DATE_CALC,-3)  >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_3M
            ,count(case when CC.FLAG_APPROVE != 1  and ADD_MONTHS(DATE_CALC,-6)  >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_6M
            ,count(case when CC.FLAG_APPROVE != 1  and ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_12M
            ,count(case when CC.FLAG_APPROVE != 1  and ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_24M
            ,count(case when CC.FLAG_APPROVE != 1  and ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) as cnt_app_rej_36M


            ,count(case when ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) cnt_app_12M
            ,count(case when ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) cnt_app_24M
            ,count(case when ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) cnt_app_36M

            ,count(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-3) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_POS_3M
            ,count(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-6) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_POS_6M
            ,count(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) cnt_app_POS_12M
            ,count(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) cnt_app_POS_24M
            ,count(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) cnt_app_POS_36M

            ,count(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-3) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_CASH_3M
            ,count(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-6) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_CASH_6M
            ,count(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CASH_12M
            ,count(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CASH_24M
            ,count(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CASH_36M

            ,count(case when cc.skp_credit_type = 3 and ADD_MONTHS(DATE_CALC,-3) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_CARD_3M
            ,count(case when cc.skp_credit_type = 3 and ADD_MONTHS(DATE_CALC,-6) >= cc.date_decision then cc.skp_credit_case end)  cnt_app_CARD_6M
            ,count(case when cc.skp_credit_type = 3 and ADD_MONTHS(DATE_CALC,-12) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CARD_12M
            ,count(case when cc.skp_credit_type = 3 and ADD_MONTHS(DATE_CALC,-24) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CARD_24M
            ,count(case when cc.skp_credit_type = 3 and ADD_MONTHS(DATE_CALC,-36) >= cc.date_decision then cc.skp_credit_case end) cnt_app_CARD_36M

            ,count(case when cc.skp_credit_type = 1 and cc.product_classification = 'Zero promo' then cc.skp_credit_case end) cnt_app_POS_ZP  -- CNT_APPLICATION_POS_ZP
            ,count(case when cc.skp_credit_type = 1 and cc.product_classification != 'Zero promo' then cc.skp_credit_case end) cnt_app_POS_ST  -- CNT_APPLICATION_POS_ZP

            ,count (case when cc.code_salesroom_group = 'M_BRNC' then cc.skp_credit_case end) as cnt_app_ROOM_MBR
            ,count (case when cc.code_salesroom_group = 'BRANCH' then cc.skp_credit_case end) as cnt_app_ROOM_BR
            ,count (case when cc.code_salesroom_group = 'KP' then cc.skp_credit_case end)     as cnt_app_ROOM_KP
            ,count (case when cc.code_salesroom_group = any('STONE', 'TOP', 'FED') then cc.skp_credit_case end)     as cnt_app_ROOM_POS            
              
      from T_ABT_PART0_APPLICATION cc
      where cc.date_decision < DATE_CALC
        and cc.FLAG_IS_DEBIT != 'Y'
      group by cc.skp_client
      )

      select /*+ PARALLEL(4) USE_HASH(S1 CMP)*/
             s1.skp_client
            ,months_between(DATE_CALC,s1.LA_date)          as APP_CNT_M_LAST
            ,months_between(DATE_CALC,s1.LA_date_CASH)     as APP_CNT_M_LAST_CASH
            ,months_between(DATE_CALC,s1.LA_date_CASH_XS)  as APP_CNT_M_LAST_CASH_XS
            ,months_between(DATE_CALC,s1.LA_date_CARD)     as APP_CNT_M_LAST_CARD
            ,months_between(DATE_CALC,s1.LA_date_CARD_XS)  as APP_CNT_M_LAST_CARD_XS
            ,months_between(DATE_CALC,s1.LA_date_POS)      as APP_CNT_M_LAST_POS

            ,s1.cnt_app_appr/ nullif(s1.cnt_app, 0)            as APP_RATE_APROVAL
            ,s1.cnt_app_CASH_appr/nullif(s1.cnt_app_CASH, 0)   as APP_RATE_APROVAL_CASH
            ,s1.cnt_app_CARD_appr/nullif(s1.cnt_app_CARD, 0)   as APP_RATE_APROVAL_CARD
            ,s1.cnt_app_POS_appr/nullif(s1.cnt_app_POS, 0)     as APP_RATE_APROVAL_POS
            ,s1.cnt_app_XS_appr/nullif(s1.cnt_app_XS, 0)       as APP_RATE_APROVAL_XS
            ,s1.cnt_app_XS_CASH_appr/nullif(s1.cnt_app_XS_CASH, 0)  as APP_RATE_APROVAL_CASH_XS
            ,s1.cnt_app_XS_CASH_appr/nullif(s1.cnt_app_XS_CARD, 0)  as APP_RATE_APROVAL_CARD_XS

            ,s1.cnt_app_POS                 as APP_CNT_POS
            ,s1.cnt_app_CASH                as APP_CNT_CASH
            ,s1.cnt_app_CARD                as APP_CNT_CARD
            ,s1.cnt_app_XS                  as APP_CNT_XSELL
            ,s1.cnt_app_XS_CASH             as APP_CNT_CASH_XS
            ,s1.cnt_app_XS_CARD             as APP_CNT_CARD_XS
            ,s1.cnt_app_POS_ZP              as APP_CNT_POS_ZP
            ,s1.cnt_app_POS_ST              as APP_CNT_POS_ST

            ,s1.cnt_app_CASH_3M             as APP_CNT_CASH_3M
            ,s1.cnt_app_CASH_6M             as APP_CNT_CASH_6M
            ,s1.cnt_app_CASH_12M            as APP_CNT_CASH_12M
            ,s1.cnt_app_CASH_24M            as APP_CNT_CASH_24M

            ,S1.cnt_app_POS_3M              AS APP_CNT_POS_3M
            ,S1.cnt_app_POS_6M              AS APP_CNT_POS_6M
            ,S1.cnt_app_POS_12M             AS APP_CNT_POS_12M
            ,S1.cnt_app_POS_24M             AS APP_CNT_POS_24M

            ,s1.cnt_app_CARD_3M             as APP_CNT_CARD_3M
            ,s1.cnt_app_CARD_6M             as APP_CNT_CARD_6M
            ,s1.cnt_app_CARD_12M            as APP_CNT_CARD_12M
            ,s1.cnt_app_CARD_24M            as APP_CNT_CARD_24M

            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_CASH,'mm')        then 1 else 0 end as APP_FLAG_CASH_FULL_1M
            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_CASH_booked,'mm') then 1 else 0 end as APP_FLAG_CASH_BOOK_1M
            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_CARD,'mm')        then 1 else 0 end as APP_FLAG_CARD_FULL_1M
            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_CARD_booked,'mm') then 1 else 0 end as APP_FLAG_CARD_BOOK_1M
            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_POS,'mm')         then 1 else 0 end as APP_FLAG_POS_FULL_1M
            ,case when trunc( DATE_CALC - 1,'mm') = trunc(s1.LA_date_POS_booked,'mm')  then 1 else 0 end as APP_FLAG_POS_BOOK_1M

            ,case when s1.LA_date_CASH        >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_CASH_FULL_CT
            ,case when s1.LA_date_CASH_booked >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_CASH_BOOK_CT
            ,case when s1.LA_date_CARD        >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_CARD_FULL_CT
            ,case when s1.LA_date_CARD_booked >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_CARD_BOOK_CT
            ,case when s1.LA_date_POS         >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_POS_FULL_CT
            ,case when s1.LA_date_POS_booked  >= cmp.CMP_date_valid_from and s1.LA_date_CASH <= cmp.CMP_date_valid_to then 1 else 0 end as APP_FLAG_POS_BOOK_CT

            ,case when s1.cnt_app_12M != 0 then s1.cnt_app_CASH_12M/s1.cnt_app_12M end as APP_SHARE_CASH_12M
            ,case when s1.cnt_app_24M != 0 then s1.cnt_app_CASH_24M/s1.cnt_app_24M end as APP_SHARE_CASH_24M
            ,case when s1.cnt_app_36M != 0 then s1.cnt_app_CASH_36M/s1.cnt_app_36M end as APP_SHARE_CASH_36M

            ,s1.cnt_app_rej          as APP_CNT_REJ
            ,s1.cnt_app_rej_CASH     as APP_CNT_REJ_CASH
            ,s1.cnt_app_rej_CASH_3M  as APP_CNT_REJ_CASH_3M
            ,s1.cnt_app_rej_CASH_6M  as APP_CNT_REJ_CASH_6M
            ,s1.cnt_app_rej_CASH_12M as APP_CNT_REJ_CASH_12M
            ,s1.cnt_app_rej_CASH_24M as APP_CNT_REJ_CASH_24M
            ,s1.cnt_app_rej_CASH_36M as APP_CNT_REJ_CASH_36M

            ,case when s1.cnt_app_rej_3M != 0 then s1.cnt_app_rej_CASH_3M/s1.cnt_app_rej_3M end    as APP_SHARE_REJ_CASH_3M
            ,case when s1.cnt_app_rej_6M != 0 then s1.cnt_app_rej_CASH_6M/s1.cnt_app_rej_6M end    as APP_SHARE_REJ_CASH_6M
            ,case when s1.cnt_app_rej_12M != 0 then s1.cnt_app_rej_CASH_12M/s1.cnt_app_rej_12M end as APP_SHARE_REJ_CASH_12M
            ,case when s1.cnt_app_rej_24M != 0 then s1.cnt_app_rej_CASH_24M/s1.cnt_app_rej_24M end as APP_SHARE_REJ_CASH_24M
            ,case when s1.cnt_app_rej_36M != 0 then s1.cnt_app_rej_CASH_36M/s1.cnt_app_rej_36M end as APP_SHARE_REJ_CASH_36M

            ,s1.cnt_app_ROOM_MBR as APP_CNT_ROOM_MBR
            ,s1.cnt_app_ROOM_BR  as APP_CNT_ROOM_BR
            ,s1.cnt_app_ROOM_KP  as APP_CNT_ROOM_KP
            ,s1.cnt_app_ROOM_POS as APP_CNT_ROOM_POS

      from W$APPL s1
      left join T_ABT_PART2_OFFER cmp on cmp.skp_client = s1.skp_client;

        PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                              acTable      => 'T_ABT_part3_APPLICATION');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;








    ----- T_ABT_PART0_APPLICATION
    PROCEDURE P_ABT_PART_4_Last_Appl(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_4_LAST_APPL';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
    
    PKG_MZ_HINTS.pAlterSession(8);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);




    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART4_LAST_APPL');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART4_LAST_APPL');

     insert /*+ append*/
         into T_ABT_PART4_LAST_APPL
      (
             skp_client
            ,LA_credit_status
            ,LA_credit_status_group
            ,LA_TERM
            ,LA_FLAG_WITH_INS
            ,LA_FLAG_WITH_INS_BOX
            ,LA_FLAG_WITH_INS_ST
            ,LA_number_of_gift

            ,LA_RATE_INTEREST
            ,LA_RATE_INTEREST_first
            ,LA_RATE_INTEREST_CASH
            ,LA_RATE_INTEREST_CASH_XS
            ,LA_RATE_INTEREST_POS
            ,LA_RATE_INTEREST_CARD

            ,LA_AMT_CREDIT
            ,LA_AMT_CREDIT_TOTAL
            ,LA_AMT_CREDIT_TOTAL_CASH
            ,LA_AMT_CREDIT_TOTAL_POS
            ,LA_AMT_CREDIT_TOTAL_CARD

            ,LA_product_type
            ,LA_PRODUCT_TYPE_group
            ,LA_CHANNEL
            ,LA_CHANNEL_group
            ,LA_CHANNEL_POS
            ,LA_NAME_GOODS_CATEGORY
            ,LA_NUMBER_ACTIVE_CONTRACT
      )

      with
      W$LAST_APPL as (
      select /*+ MATERIALIZE PARALLEL(2)*/
             cc.skp_client
            ,max(cc.date_decision)      keep(DENSE_RANK last order by cc.dtime_proposal)  as LA_date
            ,max(cc.CODE_CREDIT_STATUS) keep(DENSE_RANK last order by cc.dtime_proposal)  as LA_credit_status
            ,max(cc.CNT_INSTALMENT)     keep(DENSE_RANK last order by cc.dtime_proposal)  as LA_TERM
            ,max(cc.flag_box_insurance) keep(DENSE_RANK last order by cc.dtime_proposal)  AS LA_FLAG_WITH_INS_BOX
            ,max(cc.flag_life_insurance)keep(DENSE_RANK last order by cc.dtime_proposal)  AS LA_FLAG_WITH_INS_ST
            ,max(greatest(cc.flag_box_insurance, cc.flag_life_insurance))keep(DENSE_RANK last order by cc.dtime_proposal) AS LA_FLAG_WITH_INS
            ,max(cc.num_gift)keep(DENSE_RANK last order by cc.dtime_proposal)                                             AS LA_number_of_gift

            ,max(cc.AMT_CREDIT)keep(DENSE_RANK last order by cc.dtime_proposal)          as LA_AMT_CREDIT
            ,max(cc.AMT_CREDIT_TOTAL)keep(DENSE_RANK last order by cc.dtime_proposal)    as LA_AMT_CREDIT_TOTAL
            ,max(decode(cc.skp_credit_type, 2, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 2 then cc.dtime_proposal end NULLS first) as LA_AMT_CREDIT_TOTAL_CASH
            ,max(decode(cc.skp_credit_type, 1, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LA_AMT_CREDIT_TOTAL_POS
            ,max(decode(cc.skp_credit_type, 3, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 3 then cc.dtime_proposal end NULLS first) as LA_AMT_CREDIT_TOTAL_CARD

            ,max(cc.RATE_INTEREST) keep(DENSE_RANK last order by cc.dtime_proposal)      as LA_RATE_INTEREST
            ,max(cc.RATE_INTEREST) keep(DENSE_RANK first order by cc.dtime_proposal)     as LA_RATE_INTEREST_first
            ,max(decode(cc.skp_credit_type, 2, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 2 then cc.dtime_proposal end NULLS first) as LA_RATE_INTEREST_CASH
            ,max(case when cc.skp_credit_type = 2  and cc.code_product_purpose = 'BOUND' then cc.RATE_INTEREST end)
             keep(DENSE_RANK last order by case when cc.skp_credit_type = 2  and cc.code_product_purpose = 'BOUND' then cc.dtime_proposal end NULLS first) as LA_RATE_INTEREST_CASH_XS
            ,max(decode(cc.skp_credit_type, 1, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LA_RATE_INTEREST_POS
            ,max(decode(cc.skp_credit_type, 3, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 3 then cc.dtime_proposal end NULLS first) as LA_RATE_INTEREST_CARD

            ,max(cc.CODE_SALESROOM_GROUP)keep(DENSE_RANK last order by cc.dtime_proposal)   as LA_CHANNEL 

            ,max(case cc.CODE_SALESROOM_GROUP 
                      when 'FED' then 'POS'
                      when 'STONE' then 'POS'
                      when 'TOP' then 'POS'
                      when 'DSA' then 'DSA'
                        ELSE cc.CODE_SALESROOM_GROUP
                      end
                )keep(DENSE_RANK last order by cc.dtime_proposal)   as LA_CHANNEL_group

            ,max(cc.CODE_SALESROOM_GROUP )keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LA_CHANNEL_POS

            ,max(case when cc.skp_credit_type = 2
                           and cc.code_product_purpose = 'BOUND' and (cc.SKP_ACCOUNTING_METHOD = 8 or cc.code_product = 'CALWS500')
                      then 'CASH_CASH_EXIST'
                      when cc.skp_credit_type = 2 and cc.code_product_purpose = 'BOUND'
                      then 'CASH_XSELL'
                      when cc.skp_credit_type = 2 
                      then 'CASH_WALKIN'
                      when cc.skp_credit_type = 3
                           and (cc.code_product_purpose = 'BOUND' or cc.SKP_ACCOUNTING_METHOD = 12 or cc.code_product in ('MI_REL_W_KPCB_RW','MI_X-S_LTC14_RC') or cc.CODE_PRODUCT_PROFILE = 'REL_CC_NO_W_KZP')
                      then 'CARD'
                      when cc.skp_credit_type = 3
                      then 'CARD_POS'
                      when cc.skp_credit_type = 1                           
                      then cc.product_classification
                      end
                      )keep(DENSE_RANK last order by case when cc.skp_credit_type > 0 then cc.dtime_proposal end nulls first) as LA_PRODUCT_TYPE  
            ,max(decode(cc.skp_credit_type, 1,'POS',2,'CASH',3,'CARD', 'OTHER')) keep(DENSE_RANK last order by cc.dtime_proposal) as LA_PRODUCT_TYPE_group
            ,max(case when cc.flag_booked = 1 then 'BOOKED'
                      when cc.flag_approve = 1 then 'APPROVED'
                      else 'RESPONDED'
                       end
                       ) keep(DENSE_RANK last order by cc.dtime_proposal) as LA_credit_status_group
            ,max(case when cc.NAME_GOODS_CATEGORY in ('Household appliances','Computers','Audio and video equipment','Office supplies','Photo-cine, optics') then 'Быт.техника'
                      when cc.NAME_GOODS_CATEGORY in ('Furniture', 'Home ware') then 'Мебель'
                      when cc.NAME_GOODS_CATEGORY in('Mobiles and headset','Tablets / Communicators') then 'Мобильные'
                      when cc.NAME_GOODS_CATEGORY in ('Tourism') then 'Туризм'
                      when cc.NAME_GOODS_CATEGORY = ('Moto') then 'Моторные'
                      when cc.NAME_GOODS_CATEGORY = ('Clothing and accessories')then 'Одежда и аксессуары'
                      when cc.NAME_GOODS_CATEGORY = ('Car accessories')then 'Авто аксессуары'
                      when cc.NAME_GOODS_CATEGORY in ('Building and finishing materials, tools', 'Forest Lumber',  'Metal constructions',  'Roofing materials',  'Gardening equipment')then 'Строительные и отделочные материaлы, инструменты'
                      when cc.NAME_GOODS_CATEGORY = ('Sports and recreation') then 'Товары для спорта и отдыха'
                      else 'other' end
                 ) keep(DENSE_RANK last order by cc.dtime_proposal) as LA_NAME_GOODS_CATEGORY
            ,COUNT(distinct CASE WHEN CC.DTIME_CLOSE > DATE_CALC AND CC.FLAG_BOOKED = 1 THEN cc.skp_credit_case END) AS cnt_contract_active
                 
      from T_ABT_PART0_APPLICATION CC
      where cc.date_decision < DATE_CALC
        and CC.FLAG_IS_DEBIT != 'Y'
      group by cc.skp_client
      )
      
      select /*+ parallel(4)*/
             s1.skp_client,
             s1.LA_credit_status,
             s1.LA_credit_status_group,
             s1.LA_TERM,
             s1.LA_FLAG_WITH_INS,
             s1.LA_FLAG_WITH_INS_BOX,
             s1.LA_FLAG_WITH_INS_ST,
             s1.LA_number_of_gift,
             s1.LA_RATE_INTEREST,
             s1.LA_RATE_INTEREST_first,
             s1.LA_RATE_INTEREST_CASH,
             s1.LA_RATE_INTEREST_CASH_XS,
             s1.LA_RATE_INTEREST_POS,
             s1.LA_RATE_INTEREST_CARD,
             
             s1.LA_AMT_CREDIT,
             s1.LA_AMT_CREDIT_TOTAL,
             s1.LA_AMT_CREDIT_TOTAL_CASH,
             s1.LA_AMT_CREDIT_TOTAL_POS,
             s1.LA_AMT_CREDIT_TOTAL_CARD,
             
             s1.LA_product_type,
             s1.LA_PRODUCT_TYPE_group,
             s1.LA_CHANNEL,
             s1.LA_CHANNEL_group,
             s1.LA_CHANNEL_POS,
             s1.LA_NAME_GOODS_CATEGORY,
             nvl(s1.cnt_contract_active, 0) as LA_NUMBER_ACTIVE_CONTRACT
             
        from W$LAST_APPL s1
      ;

        PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                              acTable      => 'T_ABT_PART4_LAST_APPL');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;







    ----- T_ABT_PART0_APPLICATION
    PROCEDURE P_ABT_PART_5_Last_Contr(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_5_LAST_CONTR';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
    
    
    PKG_MZ_HINTS.pAlterSession(8);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);




    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART5_LAST_CONTRACT');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART5_LAST_CONTRACT');

     insert /*+ append*/
         into T_ABT_PART5_LAST_CONTRACT
      (
             skp_client
            ,LC_FLAG_WITH_INS
            ,LC_FLAG_WITH_INS_st
            ,LC_FLAG_WITH_INS_box

            ,LC_RATE_INTEREST
            ,LC_RATE_INTEREST_first
            ,LC_RATE_INTEREST_CASH
            ,LC_RATE_INTEREST_CASH_XS
            ,LC_RATE_INTEREST_POS
            ,LC_RATE_INTEREST_CARD

            ,LC_term

            ,LC_PRODUCT_TYPE_group
            ,LC_PRODUCT_TYPE

            ,LC_AMT_CREDIT
            ,LC_AMT_CREDIT_TOTAL
            ,LC_AMT_CREDIT_CASH
            ,LC_AMT_CREDIT_POS
            ,LC_AMT_CREDIT_CARD

            ,LC_CREDIT_STATUS
            ,LC_CREDIT_STATUS_GROUP

            ,LC_NAME_GOODS_CATEGORY

            ,LC_CHANNEL
            ,LC_CHANNEL_group
            ,LC_CHANNEL_POS

            ,LC_number_of_gift
            ,LC_date_last_payment

            ,LC_NUMBER_ACTIVE_CONTRACT
      )

      with
      W$LAST_CNTR as (
      select /*+ MATERIALIZE PARALLEL(2)*/
             cc.skp_client
            ,max(cc.date_decision)      keep(DENSE_RANK last order by cc.dtime_proposal)  as LC_date
            ,max(cc.CODE_CREDIT_STATUS) keep(DENSE_RANK last order by cc.dtime_proposal)  as LC_credit_status
            ,max(cc.CNT_INSTALMENT)     keep(DENSE_RANK last order by cc.dtime_proposal)  as LC_TERM
            ,max(cc.flag_box_insurance) keep(DENSE_RANK last order by cc.dtime_proposal)  AS LC_FLAG_WITH_INS_BOX
            ,max(cc.flag_life_insurance)keep(DENSE_RANK last order by cc.dtime_proposal)  AS LC_FLAG_WITH_INS_ST
            ,max(greatest(cc.flag_box_insurance, cc.flag_life_insurance))keep(DENSE_RANK last order by cc.dtime_proposal) AS LC_FLAG_WITH_INS
            ,max(cc.num_gift)keep(DENSE_RANK last order by cc.dtime_proposal)                                             AS LC_number_of_gift

            ,max(cc.AMT_CREDIT)keep(DENSE_RANK last order by cc.dtime_proposal)          as LC_AMT_CREDIT
            ,max(cc.AMT_CREDIT_TOTAL)keep(DENSE_RANK last order by cc.dtime_proposal)    as LC_AMT_CREDIT_TOTAL
            ,max(decode(cc.skp_credit_type, 2, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 2 then cc.dtime_proposal end NULLS first) as LC_AMT_CREDIT_CASH
            ,max(decode(cc.skp_credit_type, 1, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LC_AMT_CREDIT_POS
            ,max(decode(cc.skp_credit_type, 3, cc.AMT_CREDIT_TOTAL))keep(DENSE_RANK last order by case when cc.skp_credit_type = 3 then cc.dtime_proposal end NULLS first) as LC_AMT_CREDIT_CARD

            ,max(cc.RATE_INTEREST) keep(DENSE_RANK last order by cc.dtime_proposal)      as LC_RATE_INTEREST
            ,max(cc.RATE_INTEREST) keep(DENSE_RANK first order by cc.dtime_proposal)     as LC_RATE_INTEREST_first
            ,max(decode(cc.skp_credit_type, 2, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 2 then cc.dtime_proposal end NULLS first) as LC_RATE_INTEREST_CASH
            ,max(case when cc.skp_credit_type = 2  and cc.code_product_purpose = 'BOUND' then cc.RATE_INTEREST end)
             keep(DENSE_RANK last order by case when cc.skp_credit_type = 2  and cc.code_product_purpose = 'BOUND' then cc.dtime_proposal end NULLS first) as LC_RATE_INTEREST_CASH_XS
            ,max(decode(cc.skp_credit_type, 1, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LC_RATE_INTEREST_POS
            ,max(decode(cc.skp_credit_type, 3, cc.RATE_INTEREST)) keep(DENSE_RANK last order by case when cc.skp_credit_type = 3 then cc.dtime_proposal end NULLS first) as LC_RATE_INTEREST_CARD

            ,max(cc.CODE_SALESROOM_GROUP)keep(DENSE_RANK last order by cc.dtime_proposal)   as LC_CHANNEL 

            ,max(case cc.CODE_SALESROOM_GROUP 
                      when 'FED' then 'POS'
                      when 'STONE' then 'POS'
                      when 'TOP' then 'POS'
                      when 'DSA' then 'DSA'
                        ELSE cc.CODE_SALESROOM_GROUP
                      end
                )keep(DENSE_RANK last order by cc.dtime_proposal)   as LC_CHANNEL_group

            ,max(decode(cc.skp_credit_type, 1, cc.CODE_SALESROOM_GROUP))keep(DENSE_RANK last order by case when cc.skp_credit_type = 1 then cc.dtime_proposal end NULLS first) as LC_CHANNEL_POS

            ,max(case when cc.skp_credit_type = 2
                           and cc.code_product_purpose = 'BOUND' and (cc.SKP_ACCOUNTING_METHOD = 8 or cc.code_product = 'CALWS500')
                      then 'CASH_CASH_EXIST'
                      when cc.skp_credit_type = 2 and cc.code_product_purpose = 'BOUND'
                      then 'CASH_XSELL'
                      when cc.skp_credit_type = 2 
                      then 'CASH_WALKIN'
                      when cc.skp_credit_type = 3
                           and (cc.code_product_purpose = 'BOUND' or cc.SKP_ACCOUNTING_METHOD = 12 or cc.code_product in ('MI_REL_W_KPCB_RW','MI_X-S_LTC14_RC') or cc.CODE_PRODUCT_PROFILE = 'REL_CC_NO_W_KZP')
                      then 'CARD'
                      when cc.skp_credit_type = 3
                      then 'CARD_POS'
                      when cc.skp_credit_type = 1                           
                      then cc.product_classification
                      end
                      )keep(DENSE_RANK last order by case when cc.skp_credit_type > 0 then cc.dtime_proposal end nulls first) as LC_PRODUCT_TYPE  
            ,max(decode(cc.skp_credit_type, 1,'POS',2,'CASH',3,'CARD', 'OTHER')) keep(DENSE_RANK last order by cc.dtime_proposal) as LC_PRODUCT_TYPE_group
            ,max(case when cc.flag_booked = 1 then 'BOOKED'
                      when cc.flag_approve = 1 then 'APPROVED'
                      else 'RESPONDED'
                       end
                       ) keep(DENSE_RANK last order by cc.dtime_proposal) as LC_credit_status_group
            ,max(case when cc.NAME_GOODS_CATEGORY in ('Household appliances','Computers','Audio and video equipment','Office supplies','Photo-cine, optics') then 'Быт.техника'
                      when cc.NAME_GOODS_CATEGORY in ('Furniture', 'Home ware') then 'Мебель'
                      when cc.NAME_GOODS_CATEGORY in('Mobiles and headset','Tablets / Communicators') then 'Мобильные'
                      when cc.NAME_GOODS_CATEGORY in ('Tourism') then 'Туризм'
                      when cc.NAME_GOODS_CATEGORY = ('Moto') then 'Моторные'
                      when cc.NAME_GOODS_CATEGORY = ('Clothing and accessories')then 'Одежда и аксессуары'
                      when cc.NAME_GOODS_CATEGORY = ('Car accessories')then 'Авто аксессуары'
                      when cc.NAME_GOODS_CATEGORY in ('Building and finishing materials, tools', 'Forest Lumber',  'Metal constructions',  'Roofing materials',  'Gardening equipment')then 'Строительные и отделочные материaлы, инструменты'
                      when cc.NAME_GOODS_CATEGORY = ('Sports and recreation') then 'Товары для спорта и отдыха'
                      else 'other' end
                 ) keep(DENSE_RANK last order by cc.dtime_proposal) as LC_NAME_GOODS_CATEGORY
            ,MAX(CC.DATE_LAST_PAYMENT) AS LC_date_last_payment
            ,COUNT(distinct CASE WHEN CC.DTIME_CLOSE > DATE_CALC AND CC.FLAG_BOOKED = 1 THEN cc.skp_credit_case END) AS cnt_contract_active
                 
      from T_ABT_PART0_APPLICATION CC
      where cc.date_decision < DATE_CALC
        and CC.FLAG_IS_DEBIT != 'Y'
        AND CC.FLAG_BOOKED = 1
      group by cc.skp_client
      )
      
      select /*+ PARALLEL(4)*/
             s1.skp_client
            ,s1.LC_FLAG_WITH_INS
            ,s1.LC_FLAG_WITH_INS_st
            ,s1.LC_FLAG_WITH_INS_box

            ,s1.LC_RATE_INTEREST
            ,s1.LC_RATE_INTEREST_first
            ,s1.LC_RATE_INTEREST_CASH
            ,s1.LC_RATE_INTEREST_CASH_XS
            ,s1.LC_RATE_INTEREST_POS
            ,s1.LC_RATE_INTEREST_CARD

            ,s1.LC_term

            ,s1.LC_PRODUCT_TYPE_group
            ,s1.LC_PRODUCT_TYPE

            ,s1.LC_AMT_CREDIT
            ,s1.LC_AMT_CREDIT_TOTAL
            ,s1.LC_AMT_CREDIT_CASH
            ,s1.LC_AMT_CREDIT_POS
            ,s1.LC_AMT_CREDIT_CARD

            ,s1.LC_CREDIT_STATUS
            ,s1.LC_CREDIT_STATUS_GROUP

            ,s1.LC_NAME_GOODS_CATEGORY

            ,s1.LC_CHANNEL
            ,s1.LC_CHANNEL_group
            ,s1.LC_CHANNEL_POS

            ,s1.LC_number_of_gift
            ,s1.LC_date_last_payment

            ,nvl(S1.cnt_contract_active,0) as LC_NUMBER_ACTIVE_CONTRACT
             
        from W$LAST_CNTR s1
      ;

        PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                              acTable      => 'T_ABT_PART5_LAST_CONTRACT');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;




    PROCEDURE P_ABT_PART_6_Contracts(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_6_CONTRACTS';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      
    
    PKG_MZ_HINTS.pAlterSession(4);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);




    ---------- STEP 1 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART6_CONTRACTS');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART6_CONTRACTS');
	  
    INSERT /*+ APPEND*/ INTO T_ABT_PART6_CONTRACTS
    with
    W$RANK AS
    (
    select /*+ materialize PARALLEL(2)*/
     cc.skp_credit_case,
     cc.date_decision,
     LAG(cc.date_decision) OVER(PARTITION BY CC.SKP_CLIENT ORDER BY CC.DTIME_PROPOSAL) AS PREV_DATE_DECISION,
     cc.skp_credit_type,
     RANK() OVER(PARTITION BY cc.skp_client ORDER BY cc.DTIME_PROPOSAL desc) rnk_all,
     RANK() OVER(PARTITION BY cc.skp_client, cc.skp_credit_type ORDER BY cc.DTIME_PROPOSAL desc) rnk_by_credit_type
    
      from T_ABT_PART0_APPLICATION CC
    
     where cc.date_decision < DATE_CALC
       and CC.FLAG_IS_DEBIT != 'Y'
       and CC.FLAG_BOOKED = 1
       and CC.skp_credit_type in (1, 2)
    ),
    W$DEBIT AS
    (
    select /*+ materialize PARALLEL(2)*/
     cc.skp_client,
     COUNT(CC.skp_credit_case) AS CNT_CARD_DEBIT,
     max(case
           when cc.dtime_close > DATE_CALC then
            1
           else
            0
         end) as FLAG_HAS_CARD_DEBIT -- A,N
    
      from T_ABT_PART0_APPLICATION CC
     where cc.date_decision < DATE_CALC
       and CC.FLAG_IS_DEBIT = 'Y'
       and CC.FLAG_BOOKED = 1
       and CC.skp_credit_type in (3)
     group by cc.skp_client
    ),
    W$CNTR as (
    select
           /*+ MATERIALIZE PARALLEL(2)*/
           cc.skp_client
          ,max(cc.date_decision)                                                                                                 as date_LC
          ,max(case when cc.skp_credit_type = 2 then cc.date_decision end)                                                       as date_LC_CASH
          ,max(case when cc.skp_credit_type = 2 and CC.code_product_purpose = 'BOUND' then cc.date_decision end)                 as date_LC_CASH_XS -- XS or CE
          ,max(case when cc.skp_credit_type = 3 then cc.date_decision end)                                                       as date_LC_CARD
          ,max(case when cc.skp_credit_type = 3 and CC.code_product_purpose = 'BOUND' then cc.date_decision end)                 as date_LC_CARD_XS -- XS or CE
          ,max(case when cc.skp_credit_type = 1 then cc.date_decision end)                                                       as date_LC_POS

          ,count(cc.skp_credit_case)                                                                                                 as CNT_ALL
          ,count(case when cc.skp_credit_type = 1 then cc.skp_credit_case end)                                                       as CNT_POS
          ,count(case when cc.skp_credit_type = 2 then cc.skp_credit_case end)                                                       as CNT_CASH
          ,count(case when cc.skp_credit_type = 3 then cc.skp_credit_case end)                                                       as CNT_CARD
          ,count(case when CC.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                                            as CNT_XSELL
          ,count(case when cc.skp_credit_type = 2 and CC.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                 as CNT_CASH_XS
          ,count(case when cc.skp_credit_type = 3 and CC.code_product_purpose = 'BOUND' then cc.skp_credit_case end)                 as CNT_CARD_XS
          ,count(case when cc.skp_credit_type = 1 and cc.product_classification = 'Zero promo' then cc.skp_credit_case end)          AS CNT_POS_ZP  -- CNT_APPLICATION_POS_ZP
          ,count(case when cc.skp_credit_type = 1 and cc.product_classification != 'Zero promo' then cc.skp_credit_case end)         AS CNT_POS_ST  -- CNT_APPLICATION_POS_ZP

          ,count(case when CC.DTIME_CLOSE > DATE_CALC then cc.skp_credit_case end)  as CNT_ACT
          ,count(case when cc.skp_credit_type = 2 and CC.DTIME_CLOSE > DATE_CALC then cc.skp_credit_case end) as CNT_CASH_ACT
          ,count(case when cc.skp_credit_type = 1 and CC.DTIME_CLOSE > DATE_CALC then cc.skp_credit_case end) as CNT_POS_ACT
          ,count(case when cc.skp_credit_type = 3 and CC.DTIME_CLOSE > DATE_CALC then cc.skp_credit_case end) as CNT_CARD_ACT
          ,count(case when cc.skp_credit_type = 1 and CC.DTIME_CLOSE > DATE_CALC
                       and cc.product_classification = 'Zero promo'
                       then cc.skp_credit_case end) CNT_POS_ZP_ACT  -- CNT_APPLICATION_POS_ZP
          ,count(case when cc.skp_credit_type = 1 and CC.DTIME_CLOSE > DATE_CALC
                       and cc.product_classification != 'Zero promo'
                       then cc.skp_credit_case end) CNT_POS_ST_ACT  -- CNT_APPLICATION_POS_ST

          ,max(case when cc.skp_credit_type = 3 and CC.DTIME_CLOSE > DATE_CALC then 1 else 0 end) as FLAG_HAS_CARD
          
          ,SUM(CC.IS_USE_CARD) AS CNT_CARD_USE
          ,sum(CC.IS_PIN_CARD) as CNT_CARD_PIN
          ,sum(CC.IS_USE_CARD*CC.IS_PIN_CARD) as CNT_CARD_USE_AND_PIN -- 11 = 1, 10 = 0
          ,max(CC.IS_USE_CARD) as  FLAG_HAS_CARD_USE
          ,max(CC.IS_PIN_CARD) as  FLAG_HAS_CARD_PIN
          ,max(CC.IS_USE_CARD*CC.IS_PIN_CARD) as  FLAG_HAS_CARD_USE_PIN

          ,avg(CC.RATE_INTEREST) as IR_AVG
          ,avg(CASE WHEN cc.skp_credit_type = 2 THEN CC.RATE_INTEREST END) as IR_AVG_CASH
          ,avg(CASE WHEN cc.skp_credit_type = 1 THEN CC.RATE_INTEREST END) as IR_AVG_POS
          ,max(CC.RATE_INTEREST) as IR_MAX
          ,min(CC.RATE_INTEREST) as IR_MIN
          ,max(CASE WHEN cc.skp_credit_type = 1 THEN CC.RATE_INTEREST END) as IR_MAX_POS
          ,min(CASE WHEN cc.skp_credit_type = 1 THEN CC.RATE_INTEREST END) as IR_MIN_POS
          ,max(CASE WHEN cc.skp_credit_type = 2 THEN CC.RATE_INTEREST END) as IR_MAX_CASH
          ,min(CASE WHEN cc.skp_credit_type = 2 THEN CC.RATE_INTEREST END) as IR_MIN_CASH

          ,sum(CC.amt_credit_total)                                                                  as AMT_SUM_CREDIT
          ,sum(case when cc.skp_credit_type = 2 then CC.amt_credit_total end)                        as AMT_SUM_CREDIT_CASH
          ,sum(case when cc.skp_credit_type = 1 then CC.amt_credit_total end)                        as AMT_SUM_CREDIT_POS
          ,sum(case when cc.skp_credit_type = 3 then CC.amt_credit_total end)                        as AMT_SUM_CREDIT_CARD
          ,sum(case when CC.IS_PIN_CARD = 1   then CC.amt_credit_total end)                        as AMT_SUM_CREDIT_CARD_PIN
          ,sum(case when CC.IS_USE_CARD = 1   then CC.amt_credit_total end)                        as AMT_SUM_CREDIT_CARD_USE
          ,sum(case when CC.IS_PIN_CARD = 1 and CC.IS_USE_CARD = 1 then CC.amt_credit_total end) as AMT_SUM_CREDIT_CARD_USE_PIN
          ,sum(CC.AMT_TRANSACTION)                                                                         as AMT_SUM_CREDIT_CARD_USAGE
          ,sum(case when cc.skp_credit_type = 1 and cc.product_classification = 'Zero promo'
                       then CC.amt_credit_total end)                                                 as AMT_SUM_CREDIT_POS_ZP  -- Zerom PROMO

          ,sum(case when CC.DTIME_CLOSE > DATE_CALC then CC.amt_credit_total end)                            as AMT_SUM_CREDIT_ACT
          ,sum(case when cc.skp_credit_type = 2 and CC.DTIME_CLOSE > DATE_CALC then CC.amt_credit_total end) as AMT_SUM_CREDIT_CASH_ACT
          ,sum(case when cc.skp_credit_type = 1 and CC.DTIME_CLOSE > DATE_CALC then CC.amt_credit_total end) as AMT_SUM_CREDIT_POS_ACT
          ,sum(case when cc.skp_credit_type = 3 and CC.DTIME_CLOSE > DATE_CALC then CC.amt_credit_total end) as AMT_SUM_CREDIT_CARD_ACT
          ,sum(case when CC.IS_PIN_CARD = 1 and CC.DTIME_CLOSE > DATE_CALC   then CC.amt_credit_total end) as AMT_SUM_CREDIT_CARD_PIN_ACT
          ,sum(case when CC.IS_USE_CARD = 1 and CC.DTIME_CLOSE > DATE_CALC   then CC.amt_credit_total end) as AMT_SUM_CREDIT_CARD_USE_ACT
          ,sum(case when CC.IS_PIN_CARD = 1 and CC.IS_USE_CARD = 1 and CC.DTIME_CLOSE > DATE_CALC then CC.amt_credit_total end) as AMT_SUM_CREDIT_CARD_USE_PIN_A
          ,sum(case when CC.DTIME_CLOSE > DATE_CALC then CC.AMT_TRANSACTION end )                                                            as AMT_SUM_CREDIT_CARD_USAGE_ACT
          ,sum(case when cc.skp_credit_type = 1 and CC.DTIME_CLOSE > DATE_CALC
                       and cc.product_classification != 'Zero promo'
                      then CC.amt_credit_total end) AMT_SUM_CREDIT_POS_ZP_ACT -- Zerom PROMO , active contract

          ,sum(CC.CNT_TRANSACTION) as CNT_DRAWING_ATM
          ,sum(CC.AMT_TRANSACTION) as AMT_DRAWING_ATM
          ,min(CC.DATE_FIRST_ACT) as DATE_FIRST_PIN
          ,max(CC.DATE_LAST_ACT)  as DATE_LAST_PIN
          ,min(CC.DATE_FIRST_TRX) as DATE_FIRST_USE
          ,max(CC.DATE_LAST_TRX)  as DATE_LAST_USE

          ,sum(case when cc.Dtime_Close > DATE_CALC then CC.AMT_ANNUITY end) as AMT_SUM_ANNUITY
          ,sum(case when cc.Dtime_Close > DATE_CALC and cc.skp_credit_type = 2 then CC.AMT_ANNUITY end) as AMT_SUM_ANNUITY_CASH
          ,sum(case when cc.Dtime_Close > DATE_CALC and cc.skp_credit_type = 1 then CC.AMT_ANNUITY end) as AMT_SUM_ANNUITY_POS

          ,sum(cc.amt_box_insurance + cc.amt_life_insurance)   as AMT_INS
          ,sum(cc.amt_box_insurance)                           as AMT_INS_BOX
          ,sum(cc.amt_life_insurance)                          as AMT_INS_LIFE

          ,count(case when greatest(cc.flag_life_insurance, cc.flag_box_insurance) > 0 then cc.skp_credit_case end) as CNT_INS
          ,count(case when cc.flag_box_insurance  is not null then cc.skp_credit_case end)                          as CNT_INS_BOX
          ,count(case when cc.flag_life_insurance is not null then cc.skp_credit_case end)                          as CNT_INS_LIFE

          /*
          CNT_INS_RET
          AMT_INS_RET
          SHARE_CNT_INS_RET
          SHARE_AMT_INS_RET
          AVG_AMT_INS_PER_CONTRACT
          */

          ,count(nvl2(cc.DATE_LAST_PAYMENT, cc.skp_credit_case, null)) as CON_CNT_PDP
          ,count(case when cc.skp_credit_type = 2 and cc.DATE_LAST_PAYMENT is not null then cc.skp_credit_case end) as CON_CNT_PDP_CASH
          ,count(case when cc.skp_credit_type = 1 and cc.DATE_LAST_PAYMENT is not null then cc.skp_credit_case end) as CON_CNT_PDP_POS

          ,max(case when ADD_MONTHS(DATE_CALC,-1)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_1m
          ,max(case when ADD_MONTHS(DATE_CALC,-3)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_3m
          ,max(case when ADD_MONTHS(DATE_CALC,-6)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_6m
          ,max(case when ADD_MONTHS(DATE_CALC,-12) = cc.date_last_payment then 1 else 0 end) FLAG_PDP_12m

          ,max(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-1)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_CASH_1m
          ,max(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-3)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_CASH_3m
          ,max(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-6)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_CASH_6m
          ,max(case when cc.skp_credit_type = 2 and ADD_MONTHS(DATE_CALC,-12) = cc.date_last_payment then 1 else 0 end) FLAG_PDP_CASH_12m

          ,max(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-1)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_POS_1m
          ,max(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-3)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_POS_3m
          ,max(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-6)  = cc.date_last_payment then 1 else 0 end) FLAG_PDP_POS_6m
          ,max(case when cc.skp_credit_type = 1 and ADD_MONTHS(DATE_CALC,-12) = cc.date_last_payment then 1 else 0 end) FLAG_PDP_POS_12m


          ,MEDIAN(CC.RATE_INTEREST) as IR_MED -- CC.rate_effective_interest
          ,MEDIAN(case when cc.skp_credit_type = 2 then CC.RATE_INTEREST end) as IR_MED_CASH
          ,MEDIAN(case when cc.skp_credit_type = 1 then CC.RATE_INTEREST end) as IR_MED_POS
          ,max(case when rnk.rnk_all = 1 then CC.RATE_INTEREST end) as IR_1
          ,max(case when rnk.rnk_all = 2 then CC.RATE_INTEREST end) as IR_2
          ,max(case when rnk.rnk_all = 3 then CC.RATE_INTEREST end) as IR_3
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 1 then CC.RATE_INTEREST end) as IR_1_CASH
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 2 then CC.RATE_INTEREST end) as IR_2_CASH
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 3 then CC.RATE_INTEREST end) as IR_3_CASH
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 1 then CC.RATE_INTEREST end) as IR_1_POS
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 2 then CC.RATE_INTEREST end) as IR_2_POS
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 3 then CC.RATE_INTEREST end) as IR_3_POS

          ,MEDIAN(CC.amt_credit_total) as AMT_CREDIT_MED
          ,MEDIAN(case when cc.skp_credit_type = 2 then CC.amt_credit_total end) as AMT_CREDIT_MED_CASH
          ,MEDIAN(case when cc.skp_credit_type = 1 then CC.amt_credit_total end) as AMT_CREDIT_MED_POS
          ,avg(CC.amt_credit_total) as AMT_CREDIT_AVG
          ,avg(CASE WHEN cc.skp_credit_type = 2 THEN CC.amt_credit_total END) as AMT_CREDIT_AVG_CASH
          ,avg(CASE WHEN cc.skp_credit_type = 1 THEN CC.amt_credit_total END) as AMT_CREDIT_AVG_POS
          ,max(case when rnk.rnk_all = 1 then CC.amt_credit_total end) as AMT_CREDIT_1
          ,max(case when rnk.rnk_all = 2 then CC.amt_credit_total end) as AMT_CREDIT_2
          ,max(case when rnk.rnk_all = 3 then CC.amt_credit_total end) as AMT_CREDIT_3
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 1 then CC.amt_credit_total end) as AMT_CREDIT_1_CASH
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 2 then CC.amt_credit_total end) as AMT_CREDIT_2_CASH
          ,max(case when cc.skp_credit_type = 2 and rnk.rnk_by_credit_type = 3 then CC.amt_credit_total end) as AMT_CREDIT_3_CASH
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 1 then CC.amt_credit_total end) as AMT_CREDIT_1_POS
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 2 then CC.amt_credit_total end) as AMT_CREDIT_2_POS
          ,max(case when cc.skp_credit_type = 1 and rnk.rnk_by_credit_type = 3 then CC.amt_credit_total end) as AMT_CREDIT_3_POS

          ,max(case when rnk.rnk_all = 1 then MONTHS_BETWEEN(rnk.date_decision,RNK.PREV_DATE_DECISION) end) as CNT_MB_LAST_1
          ,max(case when rnk.rnk_all = 2 then MONTHS_BETWEEN(rnk.date_decision,RNK.PREV_DATE_DECISION) end) as CNT_MB_LAST_2
          ,max(case when rnk.rnk_all = 3 then MONTHS_BETWEEN(rnk.date_decision,RNK.PREV_DATE_DECISION) end) as CNT_MB_LAST_3
          ,avg( MONTHS_BETWEEN(rnk.date_decision,RNK.PREV_DATE_DECISION) ) as AVG_MB

          ,sum(cc.Amt_Payment_Interest) as AMT_PAY_INTEREST_ALL
          ,sum(CASE WHEN cc.skp_credit_type = 1 THEN cc.Amt_Payment_Interest end) as AMT_PAY_INTEREST_POS
          ,sum(CASE WHEN cc.skp_credit_type = 2 THEN cc.Amt_Payment_Interest end) as AMT_PAY_INTEREST_CASH
          ,sum(CASE WHEN cc.skp_credit_type = 3 THEN cc.Amt_Payment_Interest end) as AMT_PAY_INTEREST_CARD
          ,min(cc.Date_First_Paym_Principal)   as date_FIRST_payment
          ,min(cc.Date_Last_Paym_Principal)    as date_last_payment

    from T_ABT_PART0_APPLICATION cc
    LEFT join W$RANK             rnk            on rnk.skp_credit_case  = cc.skp_credit_case -- ONLY BOOKED

    where cc.date_decision <= DATE_CALC
      and CC.FLAG_IS_DEBIT != 'Y'
      and CC.FLAG_BOOKED = 1
    group by cc.skp_client

    )

    select /*+ parallel(4)*/
           s1.skp_client

          ,round(months_between(DATE_CALC,s1.date_LC),2)          as CON_CNT_M_last
          ,round(months_between(DATE_CALC,s1.date_LC_CASH),2)     as CON_CNT_M_last_CASH
          ,round(months_between(DATE_CALC,s1.date_LC_CASH_XS),2)  as CON_CNT_M_last_CASH_XS
          ,round(months_between(DATE_CALC,s1.date_LC_CARD),2)     as CON_CNT_M_last_CARD
          ,round(months_between(DATE_CALC,s1.date_LC_CARD_XS),2)  as CON_CNT_M_last_CARD_XS
          ,round(months_between(DATE_CALC,s1.date_LC_POS),2)      as CON_CNT_M_last_POS

          ,s1.CNT_ALL                  as CON_CNT_ALL
          ,s1.CNT_POS                  as CON_CNT_POS
          ,s1.CNT_CASH                 as CON_CNT_CASH
          ,s1.CNT_CARD                 as CON_CNT_CARD
          ,s1.CNT_XSELL                as CON_CNT_XSELL
          ,s1.CNT_CASH_XS              as CON_CNT_CASH_XS
          ,s1.CNT_CARD_XS              as CON_CNT_CARD_XS
          ,s1.CNT_POS_ZP               as CON_CNT_POS_ZP
          ,s1.CNT_POS_ST               as CON_CNT_POS_ST
          ,W$DEBIT.CNT_CARD_DEBIT      as CON_CNT_CARD_DEBIT

          ,s1.CNT_ACT                  as CON_CNT_A
          ,s1.CNT_CASH_ACT             as CON_CNT_CASH_A
          ,s1.CNT_POS_ACT              as CON_CNT_POS_A
          ,s1.CNT_POS_ZP_ACT           as CON_CNT_POS_ZP_A
          ,s1.CNT_POS_ST_ACT           as CON_CNT_POS_ST_A

          ,case when s1.CNT_ACT != 0     then round(s1.CNT_POS_ZP_ACT/s1.CNT_ACT,2) end AS CON_SHARE_POS_ZP_A_TO_ALL_A
          ,case when s1.CNT_POS_ACT != 0 then round(s1.CNT_POS_ACT/s1.CNT_ACT,2)    end AS CON_SHARE_POS_ZP_A_TO_POS

          ,s1.FLAG_HAS_CARD            as CON_FLAG_HAS_CARD
          ,W$DEBIT.FLAG_HAS_CARD_DEBIT   as CON_FLAG_HAS_CARD_DEBIT
          ,s1.CNT_CARD_USE             as CON_CNT_CARD_USE
          ,s1.CNT_CARD_PIN             as CON_CNT_CARD_PIN
          ,s1.CNT_CARD_USE_AND_PIN     as CON_CNT_CARD_USE_AND_PIN
          ,s1.FLAG_HAS_CARD_USE        as CON_FLAG_HAS_CARD_USE
          ,s1.FLAG_HAS_CARD_PIN        as CON_FLAG_HAS_CARD_PIN
          ,s1.FLAG_HAS_CARD_USE_PIN    as CON_FLAG_HAS_CARD_USE_PIN

          ,round(s1.IR_AVG,2)      as CON_IR_AVG
          ,round(s1.IR_AVG_CASH,2) as CON_IR_AVG_CASH
          ,round(s1.IR_AVG_POS,2)  as CON_IR_AVG_POS
          ,round(s1.IR_MAX,2)      as CON_IR_MAX
          ,round(s1.IR_MIN,2)      as CON_IR_MIN
          ,round(s1.IR_MAX_POS,2)  as CON_IR_MAX_POS
          ,round(s1.IR_MIN_POS,2)  as CON_IR_MIN_POS
          ,round(s1.IR_MAX_CASH,2) as CON_IR_MAX_CASH
          ,round(s1.IR_MIN_CASH,2) as CON_IR_MIN_CASH

          ,s1.AMT_SUM_CREDIT                  as CON_AMT_SUM_CREDIT
          ,s1.AMT_SUM_CREDIT_CASH             as CON_AMT_SUM_CREDIT_CASH
          ,s1.AMT_SUM_CREDIT_POS              as CON_AMT_SUM_CREDIT_POS
          ,s1.AMT_SUM_CREDIT_POS_ZP           as CON_AMT_SUM_CREDIT_POS_ZP
          ,s1.AMT_SUM_CREDIT_CARD             as CON_AMT_SUM_CREDIT_CARD
          ,s1.AMT_SUM_CREDIT_CARD_PIN         as CON_AMT_SUM_C_CARD_PIN
          ,s1.AMT_SUM_CREDIT_CARD_USE         as CON_AMT_SUM_C_CARD_USE
          ,s1.AMT_SUM_CREDIT_CARD_USE_PIN     as CON_AMT_SUM_C_CARD_USE_PIN
          ,s1.AMT_SUM_CREDIT_CARD_USAGE       as CON_AMT_SUM_C_CARD_USAGE

          ,s1.AMT_SUM_CREDIT_ACT              as CON_AMT_SUM_CREDIT_A
          ,s1.AMT_SUM_CREDIT_CASH_ACT         as CON_AMT_SUM_CREDIT_CASH_A
          ,s1.AMT_SUM_CREDIT_POS_ACT          as CON_AMT_SUM_CREDIT_POS_A
          ,s1.AMT_SUM_CREDIT_POS_ZP_ACT       as CON_AMT_SUM_CREDIT_POS_ZP_A
          ,s1.AMT_SUM_CREDIT_CARD_ACT         as CON_AMT_SUM_CREDIT_CARD_A
          ,s1.AMT_SUM_CREDIT_CARD_PIN_ACT     as CON_AMT_SUM_C_CARD_PIN_A
          ,s1.AMT_SUM_CREDIT_CARD_USE_ACT     as CON_AMT_SUM_C_CARD_USE_A
          ,s1.AMT_SUM_CREDIT_CARD_USE_PIN_A   as CON_AMT_SUM_C_CARD_USE_PIN_A

          ,s1.CNT_DRAWING_ATM        as CON_CNT_DRAWING_ATM
          ,s1.AMT_DRAWING_ATM        as CON_AMT_DRAWING_ATM

          ,case when s1.DATE_FIRST_PIN is not null then round(months_between(DATE_CALC,s1.DATE_FIRST_PIN),2) end as CON_CNT_M_FIRST_PIN
          ,case when s1.DATE_LAST_PIN is not null  then round(months_between(DATE_CALC,s1.DATE_LAST_PIN),2)  end as CON_CNT_M_LAST_PIN
          ,case when s1.DATE_FIRST_USE is not null then round(months_between(DATE_CALC,s1.DATE_FIRST_USE),2) end as CON_CNT_M_FIRST_USE
          ,case when s1.DATE_LAST_USE is not null  then round(months_between(DATE_CALC,s1.DATE_LAST_USE),2)  end as CON_CNT_M_LAST_USE

          ,s1.AMT_SUM_ANNUITY        as CON_AMT_SUM_ANNUITY
          ,s1.AMT_SUM_ANNUITY_CASH   as CON_AMT_SUM_ANNUITY_CASH
          ,s1.AMT_SUM_ANNUITY_POS    as CON_AMT_SUM_ANNUITY_POS

          ,case when s1.CNT_INS !=0 then round(s1.CNT_INS/s1.CNT_ALL,2) end as CON_AVG_INS_PER_CONTRA
          ,s1.AMT_INS                as CON_AMT_INS
          ,s1.AMT_INS_BOX            as CON_AMT_INS_BOX
          ,s1.AMT_INS_LIFE           as CON_AMT_INS_LIFE
          ,s1.CNT_INS                as CON_CNT_INS
          ,s1.CNT_INS_BOX            as CON_CNT_INS_BOX
          ,s1.CNT_INS_LIFE           as CON_CNT_INS_LIFE

          ,s1.CON_CNT_PDP            as CON_CNT_PDP
          ,s1.CON_CNT_PDP_CASH       as CON_CNT_PDP_CASH
          ,s1.CON_CNT_PDP_POS        as CON_CNT_PDP_POS

          ,case when s1.CNT_INS !=0  then round(s1.CON_CNT_PDP/s1.CNT_ALL,2)         end as CON_SHARE_PDP_TO_ALL
          ,case when s1.CNT_INS !=0  then round(s1.CON_CNT_PDP_CASH/s1.CNT_ALL,2)    end as CON_SHARE_CASH_PDP_TO_ALL
          ,case when s1.CNT_INS !=0  then round(s1.CON_CNT_PDP_POS/s1.CNT_ALL,2)     end as CON_SHARE_POS_PDP_TO_ALL
          ,case when s1.CNT_CASH !=0 then round(s1.CON_CNT_PDP_CASH/s1.CNT_CASH,2)   end as CON_SHARE_CASH_PDP_TO_CASH
          ,case when s1.CNT_POS !=0  then round(s1.CON_CNT_PDP_POS/s1.CNT_POS,2)     end as CON_SHARE_POS_PDP_TO_POS

          ,s1.FLAG_PDP_1m            as CON_FLAG_PDP_1m
          ,s1.FLAG_PDP_3m            as CON_FLAG_PDP_3m
          ,s1.FLAG_PDP_6m            as CON_FLAG_PDP_6m
          ,s1.FLAG_PDP_12m           as CON_FLAG_PDP_12m
          ,s1.FLAG_PDP_CASH_1m       as CON_FLAG_PDP_CASH_1m
          ,s1.FLAG_PDP_CASH_3m       as CON_FLAG_PDP_CASH_3m
          ,s1.FLAG_PDP_CASH_6m       as CON_FLAG_PDP_CASH_6m
          ,s1.FLAG_PDP_CASH_12m      as CON_FLAG_PDP_CASH_12m
          ,s1.FLAG_PDP_POS_1m        as CON_FLAG_PDP_POS_1m
          ,s1.FLAG_PDP_POS_3m        as CON_FLAG_PDP_POS_3m
          ,s1.FLAG_PDP_POS_6m        as CON_FLAG_PDP_POS_6m
          ,s1.FLAG_PDP_POS_12m       as CON_FLAG_PDP_POS_12m

          ,case when s1.IR_MED !=0  then round(s1.IR_1/s1.IR_MED,2)                 end as CON_SHARE_IR_MED_LAST_1
          ,case when s1.IR_MED !=0  then round(s1.IR_2/s1.IR_MED,2)                 end as CON_SHARE_IR_MED_LAST_2
          ,case when s1.IR_MED !=0  then round(s1.IR_3/s1.IR_MED,2)                 end as CON_SHARE_IR_MED_LAST_3
          ,case when s1.IR_MED !=0  then round(s1.IR_AVG/s1.IR_MED,2)               end as CON_SHARE_IR_AVG_MED
          ,case when s1.IR_MED_POS !=0  then round(s1.IR_1_POS/s1.IR_MED_POS,2)     end as CON_SHARE_IR_MED_LAST_1_POS
          ,case when s1.IR_MED_POS !=0  then round(s1.IR_1_POS/s1.IR_MED_POS,2)     end as CON_SHARE_IR_MED_LAST_2_POS
          ,case when s1.IR_MED_POS !=0  then round(s1.IR_1_POS/s1.IR_MED_POS,2)     end as CON_SHARE_IR_MED_LAST_3_POS
          ,case when s1.IR_MED_POS !=0  then round(s1.IR_AVG_POS/s1.IR_MED_POS,2)   end as CON_SHARE_IR_AVG_MED_POS
          ,case when s1.IR_MED_CASH !=0 then round(s1.IR_1_CASH/s1.IR_MED_CASH,2)   end as CON_SHARE_IR_MED_LAST_1_CASH
          ,case when s1.IR_MED_CASH !=0 then round(s1.IR_1_CASH/s1.IR_MED_CASH,2)   end as CON_SHARE_IR_MED_LAST_2_CASH
          ,case when s1.IR_MED_CASH !=0 then round(s1.IR_1_CASH/s1.IR_MED_CASH,2)   end as CON_SHARE_IR_MED_LAST_3_CASH
          ,case when s1.IR_MED_CASH !=0 then round(s1.IR_AVG_CASH/s1.IR_MED_CASH,2) end as CON_SHARE_IR_AVG_MED_CASH

          ,case when s1.AMT_CREDIT_MED !=0  then round(s1.AMT_CREDIT_1/s1.AMT_CREDIT_MED,2)                 end as CON_SHARE_AMT_MED_LAST_1
          ,case when s1.AMT_CREDIT_MED !=0  then round(s1.AMT_CREDIT_2/s1.AMT_CREDIT_MED,2)                 end as CON_SHARE_AMT_MED_LAST_2
          ,case when s1.AMT_CREDIT_MED !=0  then round(s1.AMT_CREDIT_3/s1.AMT_CREDIT_MED,2)                 end as CON_SHARE_AMT_MED_LAST_3
          ,case when s1.AMT_CREDIT_MED !=0  then round(s1.AMT_CREDIT_AVG/s1.AMT_CREDIT_MED,2)               end as CON_SHARE_AMT_AVG_MED
          ,case when s1.AMT_CREDIT_MED_POS !=0  then round(s1.AMT_CREDIT_1_POS/s1.AMT_CREDIT_MED_POS,2)     end as CON_SHARE_AMT_MED_LAST_1_POS
          ,case when s1.AMT_CREDIT_MED_POS !=0  then round(s1.AMT_CREDIT_1_POS/s1.AMT_CREDIT_MED_POS,2)     end as CON_SHARE_AMT_MED_LAST_2_POS
          ,case when s1.AMT_CREDIT_MED_POS !=0  then round(s1.AMT_CREDIT_1_POS/s1.AMT_CREDIT_MED_POS,2)     end as CON_SHARE_AMT_MED_LAST_3_POS
          ,case when s1.AMT_CREDIT_MED_POS !=0  then round(s1.AMT_CREDIT_AVG_POS/s1.AMT_CREDIT_MED_POS,2)   end as CON_SHARE_AMT_AVG_MED_POS
          ,case when s1.AMT_CREDIT_MED_CASH !=0 then round(s1.AMT_CREDIT_1_CASH/s1.AMT_CREDIT_MED_CASH,2)   end as CON_SHARE_AMT_MED_LAST_1_CASH
          ,case when s1.AMT_CREDIT_MED_CASH !=0 then round(s1.AMT_CREDIT_2_CASH/s1.AMT_CREDIT_MED_CASH,2)   end as CON_SHARE_AMT_MED_LAST_2_CASH
          ,case when s1.AMT_CREDIT_MED_CASH !=0 then round(s1.AMT_CREDIT_3_CASH/s1.AMT_CREDIT_MED_CASH,2)   end as CON_SHARE_AMT_MED_LAST_3_CASH
          ,case when s1.AMT_CREDIT_MED_CASH !=0 then round(s1.AMT_CREDIT_AVG_CASH/s1.AMT_CREDIT_MED_CASH,2) end as CON_SHARE_AMT_AVG_MED_CASH

          ,round(s1.CNT_MB_LAST_1,2) as CON_CNT_MB_LAST_1
          ,round(s1.CNT_MB_LAST_2,2) as CON_CNT_MB_LAST_2
          ,round(s1.CNT_MB_LAST_3,2) as CON_CNT_MB_LAST_3
          ,round(s1.AVG_MB,2)        as CON_AVG_MB

          ,s1.AMT_PAY_INTEREST_ALL   as CON_AMT_PAY_INTEREST_ALL
          ,s1.AMT_PAY_INTEREST_POS   as CON_AMT_PAY_INTEREST_POS
          ,s1.AMT_PAY_INTEREST_CASH  as CON_AMT_PAY_INTEREST_CASH
          ,s1.AMT_PAY_INTEREST_CARD  as CON_AMT_PAY_INTEREST_CARD

          ,case when s1.date_FIRST_payment is not null  then round(months_between(DATE_CALC,s1.date_FIRST_payment),2) end as CON_CNT_M_FIRST_PAYMENT
          ,case when s1.date_last_payment  is not null then round(months_between(DATE_CALC,s1.date_last_payment),2)   end as CON_CNT_M_LAST_PAYMENT

          ,CONCAT(CONCAT(nvl(s1.CNT_POS,0),nvl(s1.CNT_CASH,0)),nvl(s1.CNT_CARD,0))             as CON_CNT_PCC_ALL
          ,CONCAT(CONCAT(case when s1.CNT_POS > 0 then 1 else 0 end,case when s1.CNT_CASH > 0 then 1 else 0 end),case when s1.CNT_CARD > 0 then 1 else 0 end) as CON_FLAG_PCC_ALL
          ,CONCAT(CONCAT(nvl(s1.CNT_POS_ACT,0),nvl(s1.CNT_CASH_ACT,0)),nvl(s1.CNT_CARD_ACT,0)) as CON_CNT_PCC_ALL_A
          ,CONCAT(CONCAT(case when s1.CNT_POS_ACT > 0 then 1 else 0 end,case when s1.CNT_CASH_ACT > 0 then 1 else 0 end),case when s1.CNT_CARD_ACT > 0 then 1 else 0 end) as CON_FLAG_PCC_ALL_A


    from W$CNTR S1
    left join W$DEBIT on W$DEBIT.skp_client = s1.skp_client;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART6_CONTRACTS');  
    
    
      
      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    
    PROCEDURE P_ABT_PART_7_Comm(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_7_COMM';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      
    
    PKG_MZ_HINTS.pAlterSession(4);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);

     ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART7_ALL_SMS_CLIENT');

    INSERT /*+ APPEND*/ INTO T_ABT_PART7_ALL_SMS_CLIENT
    WITH W$MAX_SMS_ID AS
    (
    SELECT MAX(ID_SOURCE) AS MAX_ID_SOURCE FROM T_ABT_PART7_ALL_SMS_CLIENT
    ),
    SMS AS
    (
    --- ONLY NEW DATA FROM -2 MONTH
    SELECT /*+ MATERIALIZE USE_HASH(SMS W$MAX_SMS_ID) PARALLEL(2)*/
     SMS.ID_SOURCE,
     SMS.DATE_OUT,
     SMS.CONTRACT,
     SMS.PHONE,
     SMS.COST_CENTRE,
     SMS.SUBCATEGORY_GROUP,
     SMS.STATUS
     
      FROM AP_CRM.T_MZ_CRM_SMS_MAIN SMS
      JOIN W$MAX_SMS_ID
        ON 1 = 1
     WHERE SMS.DATE_OUT > ADD_MONTHS(DATE_CALC, -2)
       AND SMS.DATE_OUT < DATE_CALC
       AND SMS.COST_CENTRE != 'CRM DEPARTMENT'
       AND SMS.ID_SOURCE > W$MAX_SMS_ID.MAX_ID_SOURCE
    ),
    EXT AS
    (
    SELECT /*+ MATERIALIZE FULL(T) FULL(CC) */
     T.SKP_CREDIT_CASE,
     SUBSTR(T.TEXT_CONTACT, -10) AS TEXT_CONTACT,
     CC.skp_client,
     CC.date_decision,
     CC.dtime_proposal,
     CC.text_contract_number
      FROM OWNER_DWH.F_APPLICATION_CONTACT_TT T
      JOIN OWNER_DWH.DC_CREDIT_CASE CC
        ON T.SKP_CREDIT_CASE = CC.skp_credit_case
       AND T.DATE_DECISION = CC.date_decision
       AND T.SKP_CREDIT_TYPE = CC.skp_credit_type
      JOIN OWNER_DWH.CL_CONTACT_TYPE CT
        ON T.SKP_CONTACT_TYPE = CT.SKP_CONTACT_TYPE
      JOIN OWNER_DWH.CL_CONTACT_RELATION_TYPE RT
        ON T.SKP_CONTACT_RELATION_TYPE = RT.SKP_CONTACT_RELATION_TYPE
     WHERE T.DATE_DECISION < DATE_CALC
       AND T.SKP_CREDIT_TYPE IN (1, 2, 3)
       AND CT.CODE_CONTACT_TYPE IN ('PRIMARY_MOBILE')
       AND RT.CODE_CONTACT_RELATION_TYPE = 'CL' --CLIENT
       AND CC.skp_client > 0
    ),
    SAS AS
    (
    SELECT /*+ MATERIALIZE FULL(T)*/
     C.skp_client,
     T.ID_CUID,
     T.TEXT_PHONE,
     T.NAME_CONTACT_TYPE,
     T.DATE_PHONE_INSERT
      FROM DM_CAMPAIGN_SAS.F_CLIENT_CONTACT_AD T
      JOIN OWNER_DWH.DC_CLIENT C
        ON T.ID_CUID = C.id_cuid
     WHERE T.FLAG_DELETED = 'N'
    )
    ,UN AS
    (
    SELECT /*+ LEADING(SMS) USE_HASH(SMS EXT EXT2 EXT3 SAS) MATERIALIZE PARALLEL(2)*/
           SMS.ID_SOURCE,
           SMS.DATE_OUT,
           SMS.CONTRACT,
           SMS.PHONE,
           SMS.COST_CENTRE,
           SMS.SUBCATEGORY_GROUP,
           SMS.STATUS,
           MAX(EXT.SKP_CLIENT) AS SKP_CLIENT1,
           MAX(EXT2.SKP_CLIENT) AS SKP_CLIENT2,
           MAX(EXT3.SKP_CLIENT) KEEP(DENSE_RANK LAST ORDER BY EXT3.DTIME_PROPOSAL) AS SKP_CLIENT3,
           MAX(SAS.SKP_CLIENT)  KEEP(DENSE_RANK LAST ORDER BY SAS.NAME_CONTACT_TYPE) AS SKP_CLIENT4
           
      FROM SMS
      LEFT JOIN EXT
        ON SMS.CONTRACT = EXT.TEXT_CONTRACT_NUMBER
       AND SMS.PHONE = EXT.TEXT_CONTACT
       AND SMS.DATE_OUT > EXT.DTIME_PROPOSAL
      LEFT JOIN EXT EXT2
        ON SMS.CONTRACT = EXT2.TEXT_CONTRACT_NUMBER
       AND SMS.DATE_OUT > EXT2.DTIME_PROPOSAL
      LEFT JOIN EXT EXT3
        ON SMS.PHONE = EXT3.TEXT_CONTACT
       AND SMS.DATE_OUT > EXT3.DTIME_PROPOSAL
      LEFT JOIN SAS
        ON SMS.PHONE = SAS.TEXT_PHONE
       AND SMS.DATE_OUT > SAS.DATE_PHONE_INSERT
       
     GROUP BY  SMS.ID_SOURCE,
               SMS.DATE_OUT,
               SMS.CONTRACT,
               SMS.PHONE,
               SMS.COST_CENTRE,
               SMS.SUBCATEGORY_GROUP,
               SMS.STATUS
        )        
    SELECT /*+ PARALLEL(4)*/
           ID_SOURCE,
           DATE_OUT,
           CONTRACT,
           PHONE,
           COST_CENTRE,
           SUBCATEGORY_GROUP,
           STATUS,
           SKP_CLIENT1,
           SKP_CLIENT2,
           SKP_CLIENT3,
           SKP_CLIENT4,
           COALESCE(SKP_CLIENT1, SKP_CLIENT2, SKP_CLIENT4, SKP_CLIENT3) AS SKP_CLIENT            
      FROM UN; 

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART7_ALL_SMS_CLIENT');




    ---------- STEP 1 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART7_COMM_SMS');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART7_COMM_SMS');

    insert /*+ append*/  into T_ABT_PART7_COMM_SMS
    with W$SMS AS
    (
       --- ALL CRM SMS
    SELECT /*+ MATERIALIZE*/
           id_source,
           date_out,
           phone,
           'CRM DEPARTMENT' AS cost_centre,
           subcategory_group,
           subcategory_name,
           status,
           skp_client
           
      FROM AP_CRM.T_MZ_CRM_SMS_MAIN_DATAMART t
     where t.date_out < DATE_CALC
       and t.date_out >= ADD_MONTHS(DATE_CALC, -12)
       AND T.SKP_CLIENT > 0
       
       UNION ALL
       --- ALL OTHER SMS
    SELECT id_source,
           date_out,
           phone,
           cost_centre,
           subcategory_group,
           NULL AS subcategory_name,
           status,
           skp_client

      FROM T_ABT_PART7_ALL_SMS_CLIENT t
     where t.date_out < DATE_CALC
       and t.date_out >= ADD_MONTHS(DATE_CALC, -12)
       AND T.SKP_CLIENT > 0   
    ),
    W$AGGR as (
      select
              t.skp_client   
             ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= t.date_out then t.id_source end) COM_CNT_sms_1m
             ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= t.date_out then t.id_source end) COM_CNT_sms_3m
             ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= t.date_out then t.id_source end) COM_CNT_sms_6m
             ,count(case when ADD_MONTHS(DATE_CALC,-12) >= t.date_out then t.id_source end) COM_CNT_sms_12m

             ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= t.date_out and t.subcategory_group = 'X-SELL' then t.id_source end) COM_CNT_sms_XS_1m
             ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= t.date_out and t.subcategory_group = 'X-SELL' then t.id_source end) COM_CNT_sms_XS_3m
             ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= t.date_out and t.subcategory_group = 'X-SELL' then t.id_source end) COM_CNT_sms_XS_6m
             ,count(case when ADD_MONTHS(DATE_CALC,-12) >= t.date_out and t.subcategory_group = 'X-SELL' then t.id_source end) COM_CNT_sms_XS_12m

             ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= t.date_out and t.subcategory_name = 'Onboarding card' then t.id_source end) COM_CNT_sms_card_1m
             ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= t.date_out and t.subcategory_name = 'Onboarding card' then t.id_source end) COM_CNT_sms_card_3m
             ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= t.date_out and t.subcategory_name = 'Onboarding card' then t.id_source end) COM_CNT_sms_card_6m
             ,count(case when ADD_MONTHS(DATE_CALC,-12) >= t.date_out and t.subcategory_name = 'Onboarding card' then t.id_source end) COM_CNT_sms_card_12m

             ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= t.date_out and t.subcategory_name IN ('FCB_Trigger', 'KAZPOST_TRIGGER', 'POS+CASH', 'Mail_ru_trigger_SMS') then t.id_source end) COM_CNT_sms_XS_trigger_1m
             ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= t.date_out and t.subcategory_name IN ('FCB_Trigger', 'KAZPOST_TRIGGER', 'POS+CASH', 'Mail_ru_trigger_SMS') then t.id_source end) COM_CNT_sms_XS_trigger_3m
             ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= t.date_out and t.subcategory_name IN ('FCB_Trigger', 'KAZPOST_TRIGGER', 'POS+CASH', 'Mail_ru_trigger_SMS') then t.id_source end) COM_CNT_sms_XS_trigger_6m
             ,count(case when ADD_MONTHS(DATE_CALC,-12) >= t.date_out and t.subcategory_name IN ('FCB_Trigger', 'KAZPOST_TRIGGER', 'POS+CASH', 'Mail_ru_trigger_SMS') then t.id_source end) COM_CNT_sms_XS_trigger_12m

             ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= t.date_out and t.status IN ('DELIVERED'/*,'ENROUTE'*/) then t.id_source end) COM_CNT_sms_delivered_1m
             ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= t.date_out and t.status IN ('DELIVERED'/*,'ENROUTE'*/) then t.id_source end) COM_CNT_sms_delivered_3m
             ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= t.date_out and t.status IN ('DELIVERED'/*,'ENROUTE'*/) then t.id_source end) COM_CNT_sms_delivered_6m
             ,count(case when ADD_MONTHS(DATE_CALC,-12) >= t.date_out and t.status IN ('DELIVERED'/*,'ENROUTE'*/) then t.id_source end) COM_CNT_sms_delivered_12m

      from W$SMS t
     group by t.skp_client
     )
      select  /*+ PARALLEL(2)*/
              s1.skp_client

             ,s1.COM_CNT_sms_1m
             ,s1.COM_CNT_sms_3m
             ,s1.COM_CNT_sms_6m
             ,s1.COM_CNT_sms_12m

             ,s1.COM_CNT_sms_XS_1m
             ,s1.COM_CNT_sms_XS_3m
             ,s1.COM_CNT_sms_XS_6m
             ,s1.COM_CNT_sms_XS_12m

             ,s1.COM_CNT_sms_card_1m
             ,s1.COM_CNT_sms_card_3m
             ,s1.COM_CNT_sms_card_6m
             ,s1.COM_CNT_sms_card_12m

             ,s1.COM_CNT_sms_XS_trigger_1m
             ,s1.COM_CNT_sms_XS_trigger_3m
             ,s1.COM_CNT_sms_XS_trigger_6m
             ,s1.COM_CNT_sms_XS_trigger_12m

             ,case when s1.COM_CNT_sms_1m > 0  then round(s1.COM_CNT_sms_delivered_1m/s1.COM_CNT_sms_1m,2) end   as COM_SHARE_sms_deliver_1m
             ,case when s1.COM_CNT_sms_3m > 0  then round(s1.COM_CNT_sms_delivered_3m/s1.COM_CNT_sms_3m,2) end   as COM_SHARE_sms_deliver_3m
             ,case when s1.COM_CNT_sms_6m > 0  then round(s1.COM_CNT_sms_delivered_6m/s1.COM_CNT_sms_6m,2) end   as COM_SHARE_sms_deliver_6m
             ,case when s1.COM_CNT_sms_12m > 0 then round(s1.COM_CNT_sms_delivered_12m/s1.COM_CNT_sms_12m,2) end as COM_SHARE_sms_deliver_12m
      from W$AGGR S1;


    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART7_COMM_SMS');

    
    
    
    
    
    ---------- STEP 3 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART7_COMM_LCS');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART7_COMM_LCS');

    insert /*+ append*/
       into T_ABT_PART7_COMM_LCS
    (
            skp_client
           ,COM_CNT_calls_collection_1m
           ,COM_CNT_calls_collection_3m
           ,COM_CNT_calls_collection_6m
           ,COM_CNT_calls_collection_12m
    )

    SELECT  /*+ USE_HASH(T D C) PARALLEL(2)*/
            c.skp_client
           ,count(case when ADD_MONTHS(date_calc,-1)  >= t.begin_action then t.tc_id end) COM_CNT_calls_collection_1m
           ,count(case when ADD_MONTHS(date_calc,-3)  >= t.begin_action then t.tc_id end) COM_CNT_calls_collection_3m
           ,count(case when ADD_MONTHS(date_calc,-6)  >= t.begin_action then t.tc_id end) COM_CNT_calls_collection_6m
           ,count(case when ADD_MONTHS(date_calc,-12) >= t.begin_action then t.tc_id end) COM_CNT_calls_collection_12m

    FROM AP_COLL.T_LOXON_EC_RESULTS T
    JOIN OWNER_INT.VH_LCS_T_DEAL D  ON D.T_DEAL_ID = T.ID_DEAL
    JOIN OWNER_DWH.DC_CREDIT_CASE C ON C.text_contract_number = D.CONTRACTNR

    where t.executiondate <  date_calc
      and t.executiondate >= ADD_MONTHS(date_calc,-12)
    group by c.skp_client;


    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART7_COMM_LCS');

    
    
    
    
    
    ---------- STEP 2 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART7_COMM_CALL');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART7_COMM_CALL');

    insert /*+ append*/
       into T_ABT_PART7_COMM_CALL
    
    with s1 as
    (
    select /*+ MATERIALIZE PARALLEL(4)*/
            dt.skp_client
           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart then dt.idchain end) COM_CNT_calls_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart then dt.idchain end) COM_CNT_calls_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart then dt.idchain end) COM_CNT_calls_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart then dt.idchain end) COM_CNT_calls_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.isoutput = 1 then dt.idchain end) COM_CNT_calls_out_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.isoutput = 1 then dt.idchain end) COM_CNT_calls_out_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.isoutput = 1 then dt.idchain end) COM_CNT_calls_out_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.isoutput = 1 then dt.idchain end) COM_CNT_calls_out_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.isoutput = 0 then dt.idchain end) COM_CNT_calls_in_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.isoutput = 0 then dt.idchain end) COM_CNT_calls_in_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.isoutput = 0 then dt.idchain end) COM_CNT_calls_in_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.isoutput = 0 then dt.idchain end) COM_CNT_calls_in_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.dept in ('X-sell','X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.dept in ('X-sell','X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.dept in ('X-sell','X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.dept in ('X-sell','X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.dept in ('X-sell') then dt.idchain end) COM_CNT_calls_XS_out_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.dept in ('X-sell') then dt.idchain end) COM_CNT_calls_XS_out_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.dept in ('X-sell') then dt.idchain end) COM_CNT_calls_XS_out_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.dept in ('X-sell') then dt.idchain end) COM_CNT_calls_XS_out_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.dept in ('X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_in_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.dept in ('X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_in_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.dept in ('X-Sell вх') then dt.idchain end) COM_CNT_calls_XS_in_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.dept in ('X-Sell вх')then dt.idchain end)  COM_CNT_calls_XS_in_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_w_res_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_w_res_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_w_res_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_w_res_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.isoutput = 1 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_out_w_res_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.isoutput = 1 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_out_w_res_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.isoutput = 1 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_out_w_res_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.isoutput = 1 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_out_w_res_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.isoutput = 0 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_in_w_res_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.isoutput = 0 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_in_w_res_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.isoutput = 0 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_in_w_res_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.isoutput = 0 and dt.is_responded = 1 then dt.idchain end) COM_CNT_calls_in_w_res_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.dept in ('!7373','7373','7979','Infoline','Infoline2') then dt.idchain end) COM_CNT_calls_service_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.dept in ('!7373','7373','7979','Infoline','Infoline2') then dt.idchain end) COM_CNT_calls_service_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.dept in ('!7373','7373','7979','Infoline','Infoline2') then dt.idchain end) COM_CNT_calls_service_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.dept in ('!7373','7373','7979','Infoline','Infoline2') then dt.idchain end) COM_CNT_calls_service_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and (nvl(dt.is_promise_bsl,0) + nvl(dt.is_promise_okt,0) + nvl(dt.is_promise_tlm,0)) > 0 then dt.idchain end) COM_CNT_calls_promise_in_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and (nvl(dt.is_promise_bsl,0) + nvl(dt.is_promise_okt,0) + nvl(dt.is_promise_tlm,0)) > 0 then dt.idchain end) COM_CNT_calls_promise_in_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and (nvl(dt.is_promise_bsl,0) + nvl(dt.is_promise_okt,0) + nvl(dt.is_promise_tlm,0)) > 0 then dt.idchain end) COM_CNT_calls_promise_in_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and (nvl(dt.is_promise_bsl,0) + nvl(dt.is_promise_okt,0) + nvl(dt.is_promise_tlm,0)) > 0 then dt.idchain end) COM_CNT_calls_promise_in_12m

           ,max(dt.lentime) COM_max_call_lenght_tlm
           ,min(dt.lentime) COM_min_call_lenght_tlm
           ,avg(dt.lentime) COM_avg_call_lenght_tlm
           ,max(case when dt.dept in ('X-sell','X-Sell вх') then dt.lentime end) COM_max_call_XS_lenght_tlm
           ,min(case when dt.dept in ('X-sell','X-Sell вх') then dt.lentime end) COM_min_call_XS_lenght_tlm
           ,avg(case when dt.dept in ('X-sell','X-Sell вх') then dt.lentime end) COM_avg_call_XS_lenght_tlm

           ,max(dt.lenqueue) COM_max_call_queue_tlm
           ,min(dt.lenqueue) COM_min_call_queue_tlm
           ,avg(dt.lenqueue) COM_avg_call_queue_tlm
           ,max(case when dt.dept in ('X-sell','X-Sell вх') then dt.lenqueue end) COM_max_call_XS_queue_tlm
           ,min(case when dt.dept in ('X-sell','X-Sell вх') then dt.lenqueue end) COM_min_call_XS_queue_tlm
           ,avg(case when dt.dept in ('X-sell','X-Sell вх') then dt.lenqueue end) COM_avg_call_XS_queue_tlm

    ---- -----
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart then dt.abonentnumber end) as COM_CNT_UNIQ_PHONES_6M
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart then dt.abonentnumber end) as COM_CNT_UNIQ_PHONES_12M
    /*       ,count(distinct case when ADD_MONTHS(date_calc,-24) >= dt.datetimestart then dt.abonentnumber end) as COM_CNT_UNIQ_PHONES_24M
           ,count(distinct case when ADD_MONTHS(date_calc,-36) >= dt.datetimestart then dt.abonentnumber end) as COM_CNT_UNIQ_PHONES_36M
    */
           ,max(dt.datetimestart) as date_last_call
           ,max(case when dt.callresult = 5 then dt.datetimestart end) as date_last_call_suc
           ,max(dt.callresultname)keep(dense_rank last order by dt.datetimestart) as COM_LAST_CALL_STATUS
           ,max(dt.dept)keep(dense_rank last order by dt.datetimestart) as COM_LAST_CALL_DEPT


           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.callresult = 5 then dt.idchain end) COM_CNT_calls_succes_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.callresult = 5 then dt.idchain end) COM_CNT_calls_succes_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.callresult = 5 then dt.idchain end) COM_CNT_calls_succes_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.callresult = 5 then dt.idchain end) COM_CNT_calls_succes_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.callresult = 8 then dt.idchain end) COM_CNT_calls_busy_in_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.callresult = 8 then dt.idchain end) COM_CNT_calls_busy_in_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.callresult = 8 then dt.idchain end) COM_CNT_calls_busy_in_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.callresult = 8 then dt.idchain end) COM_CNT_calls_busy_in_12m

           ,count(distinct case when ADD_MONTHS(date_calc,-1)  >= dt.datetimestart and dt.callresult != 5 then dt.idchain end) COM_CNT_calls_Neg_in_1m
           ,count(distinct case when ADD_MONTHS(date_calc,-3)  >= dt.datetimestart and dt.callresult != 5 then dt.idchain end) COM_CNT_calls_Neg_in_3m
           ,count(distinct case when ADD_MONTHS(date_calc,-6)  >= dt.datetimestart and dt.callresult != 5 then dt.idchain end) COM_CNT_calls_Neg_in_6m
           ,count(distinct case when ADD_MONTHS(date_calc,-12) >= dt.datetimestart and dt.callresult != 5 then dt.idchain end) COM_CNT_calls_Neg_in_12m

    from AP_CRM.T_MZ_CALL_DATAMART_2_SALES dt

    where dt.datetimestart < date_calc
      and dt.datetimestart >= ADD_MONTHS(date_calc,-12)
      AND DT.SKP_CLIENT IS NOT NULL
    group by dt.skp_client
    )

    select  s1.skp_client

           ,s1.COM_CNT_calls_1m
           ,s1.COM_CNT_calls_3m
           ,s1.COM_CNT_calls_6m
           ,s1.COM_CNT_calls_12m

           ,s1.COM_CNT_calls_out_1m
           ,s1.COM_CNT_calls_out_3m
           ,s1.COM_CNT_calls_out_6m
           ,s1.COM_CNT_calls_out_12m

           ,s1.COM_CNT_calls_in_1m
           ,s1.COM_CNT_calls_in_3m
           ,s1.COM_CNT_calls_in_6m
           ,s1.COM_CNT_calls_in_12m

           ,s1.COM_CNT_calls_XS_1m
           ,s1.COM_CNT_calls_XS_3m
           ,s1.COM_CNT_calls_XS_6m
           ,s1.COM_CNT_calls_XS_12m

           ,s1.COM_CNT_calls_XS_out_1m
           ,s1.COM_CNT_calls_XS_out_3m
           ,s1.COM_CNT_calls_XS_out_6m
           ,s1.COM_CNT_calls_XS_out_12m

           ,s1.COM_CNT_calls_XS_in_1m
           ,s1.COM_CNT_calls_XS_in_3m
           ,s1.COM_CNT_calls_XS_in_6m
           ,s1.COM_CNT_calls_XS_in_12m

           ,s1.COM_CNT_calls_w_res_1m
           ,s1.COM_CNT_calls_w_res_3m
           ,s1.COM_CNT_calls_w_res_6m
           ,s1.COM_CNT_calls_w_res_12m

           ,s1.COM_CNT_calls_out_w_res_1m
           ,s1.COM_CNT_calls_out_w_res_3m
           ,s1.COM_CNT_calls_out_w_res_6m
           ,s1.COM_CNT_calls_out_w_res_12m

           ,s1.COM_CNT_calls_in_w_res_1m
           ,s1.COM_CNT_calls_in_w_res_3m
           ,s1.COM_CNT_calls_in_w_res_6m
           ,s1.COM_CNT_calls_in_w_res_12m

           ,s1.COM_CNT_calls_service_1m
           ,s1.COM_CNT_calls_service_3m
           ,s1.COM_CNT_calls_service_6m
           ,s1.COM_CNT_calls_service_12m

           ,s1.COM_CNT_calls_Neg_in_1m
           ,s1.COM_CNT_calls_Neg_in_3m
           ,s1.COM_CNT_calls_Neg_in_6m
           ,s1.COM_CNT_calls_Neg_in_12m

           ,s1.COM_CNT_calls_busy_in_1m
           ,s1.COM_CNT_calls_busy_in_3m
           ,s1.COM_CNT_calls_busy_in_6m
           ,s1.COM_CNT_calls_busy_in_12m

           ,s1.COM_CNT_calls_promise_in_1m
           ,s1.COM_CNT_calls_promise_in_3m
           ,s1.COM_CNT_calls_promise_in_6m
           ,s1.COM_CNT_calls_promise_in_12m

           ,s1.COM_max_call_lenght_tlm
           ,s1.COM_min_call_lenght_tlm
           ,s1.COM_avg_call_lenght_tlm
           ,s1.COM_max_call_XS_lenght_tlm
           ,s1.COM_min_call_XS_lenght_tlm
           ,s1.COM_avg_call_XS_lenght_tlm
           ,s1.COM_max_call_queue_tlm
           ,s1.COM_min_call_queue_tlm
           ,s1.COM_avg_call_queue_tlm
           ,s1.COM_max_call_XS_queue_tlm
           ,s1.COM_min_call_XS_queue_tlm
           ,s1.COM_avg_call_XS_queue_tlm

           ,s1.COM_LAST_CALL_STATUS
           ,s1.COM_LAST_CALL_DEPT

           ,s1.COM_CNT_UNIQ_PHONES_6M
           ,s1.COM_CNT_UNIQ_PHONES_12M

           ,case when s1.COM_CNT_calls_1m  != 0 then s1.COM_CNT_calls_succes_1m/s1.COM_CNT_calls_1m end   as COM_SHARE_SUCCESS_1M
           ,case when s1.COM_CNT_calls_3m  != 0 then s1.COM_CNT_calls_succes_3m/s1.COM_CNT_calls_3m end   as COM_SHARE_SUCCESS_3M
           ,case when s1.COM_CNT_calls_6m  != 0 then s1.COM_CNT_calls_succes_6m/s1.COM_CNT_calls_6m end   as COM_SHARE_SUCCESS_6M
           ,case when s1.COM_CNT_calls_12m != 0 then s1.COM_CNT_calls_succes_12m/s1.COM_CNT_calls_12m end as COM_SHARE_SUCCESS_12M

           ,months_between(date_calc,s1.date_last_call) as COM_CNT_M_LAST_CALL
           ,months_between(date_calc,s1.date_last_call_suc) as COM_CNT_M_LAST_CALL_SUCCESS

    from s1;


    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART7_COMM_CALL');
    
    
    
    
    
    
    ---------- STEP 4 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART7_COMM');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART7_COMM');

    insert /*+ append*/
      into T_ABT_PART7_COMM
    (
           SKP_CLIENT
          ,Com_Cnt_Sms_1m
          ,Com_Cnt_Sms_3m
          ,Com_Cnt_Sms_6m
          ,Com_Cnt_Sms_12m
          ,Com_Cnt_Sms_Xs_1m
          ,Com_Cnt_Sms_Xs_3m
          ,Com_Cnt_Sms_Xs_6m
          ,Com_Cnt_Sms_Xs_12m
          ,Com_Cnt_Sms_Card_1m
          ,Com_Cnt_Sms_Card_3m
          ,Com_Cnt_Sms_Card_6m
          ,Com_Cnt_Sms_Card_12m
          ,Com_Cnt_Sms_Xs_Trigger_1m
          ,Com_Cnt_Sms_Xs_Trigger_3m
          ,Com_Cnt_Sms_Xs_Trigger_6m
          ,Com_Cnt_Sms_Xs_Trigger_12m
          ,Com_Share_Sms_Deliver_1m
          ,Com_Share_Sms_Deliver_3m
          ,Com_Share_Sms_Deliver_6m
          ,Com_Share_Sms_Deliver_12m

          ,Com_Cnt_Calls_1m
          ,Com_Cnt_Calls_3m
          ,Com_Cnt_Calls_6m
          ,Com_Cnt_Calls_12m
          ,Com_Cnt_Calls_Out_1m
          ,Com_Cnt_Calls_Out_3m
          ,Com_Cnt_Calls_Out_6m
          ,Com_Cnt_Calls_Out_12m
          ,Com_Cnt_Calls_In_1m
          ,Com_Cnt_Calls_In_3m
          ,Com_Cnt_Calls_In_6m
          ,Com_Cnt_Calls_In_12m
          ,Com_Cnt_Calls_Xs_1m
          ,Com_Cnt_Calls_Xs_3m
          ,Com_Cnt_Calls_Xs_6m
          ,Com_Cnt_Calls_Xs_12m
          ,Com_Cnt_Calls_Xs_Out_1m
          ,Com_Cnt_Calls_Xs_Out_3m
          ,Com_Cnt_Calls_Xs_Out_6m
          ,Com_Cnt_Calls_Xs_Out_12m
          ,Com_Cnt_Calls_Xs_In_1m
          ,Com_Cnt_Calls_Xs_In_3m
          ,Com_Cnt_Calls_Xs_In_6m
          ,Com_Cnt_Calls_Xs_In_12m

          ,Com_Cnt_Calls_w_Res_1m
          ,Com_Cnt_Calls_w_Res_3m
          ,Com_Cnt_Calls_w_Res_6m
          ,Com_Cnt_Calls_w_Res_12m
          ,Com_Cnt_Calls_Out_w_Res_1m
          ,Com_Cnt_Calls_Out_w_Res_3m
          ,Com_Cnt_Calls_Out_w_Res_6m
          ,Com_Cnt_Calls_Out_w_Res_12m
          ,Com_Cnt_Calls_In_w_Res_1m
          ,Com_Cnt_Calls_In_w_Res_3m
          ,Com_Cnt_Calls_In_w_Res_6m
          ,Com_Cnt_Calls_In_w_Res_12m
          ,Com_Cnt_Calls_Service_1m
          ,Com_Cnt_Calls_Service_3m
          ,Com_Cnt_Calls_Service_6m
          ,Com_Cnt_Calls_Service_12m
          ,Com_Cnt_Calls_Neg_In_1m
          ,Com_Cnt_Calls_Neg_In_3m
          ,Com_Cnt_Calls_Neg_In_6m
          ,Com_Cnt_Calls_Neg_In_12m
          ,Com_Cnt_Calls_Busy_In_1m
          ,Com_Cnt_Calls_Busy_In_3m
          ,Com_Cnt_Calls_Busy_In_6m
          ,Com_Cnt_Calls_Busy_In_12m
          ,Com_Cnt_Calls_Promise_In_1m
          ,Com_Cnt_Calls_Promise_In_3m
          ,Com_Cnt_Calls_Promise_In_6m
          ,Com_Cnt_Calls_Promise_In_12m

    --     ,COM_CNT_calls_PDP_in_1
    --     ,COM_CNT_calls_PDP_in_3m
    --     ,COM_CNT_calls_PDP_in_6m
    --     ,COM_CNT_calls_PDP_in_12m

          ,Com_Max_Call_Lenght_Tlm
          ,Com_Min_Call_Lenght_Tlm
          ,Com_Avg_Call_Lenght_Tlm
          ,Com_Max_Call_Xs_Lenght_Tlm
          ,Com_Min_Call_Xs_Lenght_Tlm
          ,Com_Avg_Call_Xs_Lenght_Tlm
          ,Com_Max_Call_Queue_Tlm
          ,Com_Min_Call_Queue_Tlm
          ,Com_Avg_Call_Queue_Tlm
          ,Com_Max_Call_Xs_Queue_Tlm
          ,Com_Min_Call_Xs_Queue_Tlm
          ,Com_Avg_Call_Xs_Queue_Tlm
          ,Com_Last_Call_Status
          ,Com_Last_Call_Dept
    --     ,COM_flag_had_CATI_call_6m
          ,Com_Cnt_Uniq_Phones_6m
          ,Com_Cnt_Uniq_Phones_12m
    --     ,COM_CNT_UNIQ_PHONES_24M
    --     ,COM_CNT_UNIQ_PHONES_36M

          ,Com_Share_Success_1m
          ,Com_Share_Success_3m
          ,Com_Share_Success_6m
          ,Com_Share_Success_12m
          ,Com_Cnt_m_Last_Call
          ,Com_Cnt_m_Last_Call_Success
          ,Com_Cnt_Calls_Collection_1m
          ,Com_Cnt_Calls_Collection_3m
          ,Com_Cnt_Calls_Collection_6m
          ,Com_Cnt_Calls_Collection_12m
    )

    with s1 as (
    select p1.skp_client
    from T_ABT_PART7_COMM_SMS p1
    union 
    select p2.skp_client
    from T_ABT_PART7_COMM_LCS p2
    union 
    select p3.skp_client
    from T_ABT_PART7_COMM_CALL p3
    )

    select  /*+ PARALLEL(4)*/
            s1.skp_client

           ,nvl(p1.COM_CNT_sms_1m,0) AS COM_CNT_sms_1m
           ,nvl(p1.COM_CNT_sms_3m,0) AS COM_CNT_sms_3m
           ,nvl(p1.COM_CNT_sms_6m,0) AS COM_CNT_sms_6m
           ,nvl(p1.COM_CNT_sms_12m,0) AS COM_CNT_sms_12m

           ,nvl(p1.COM_CNT_sms_XS_1m,0) AS COM_CNT_sms_XS_1m
           ,nvl(p1.COM_CNT_sms_XS_3m,0) AS COM_CNT_sms_XS_3m
           ,nvl(p1.COM_CNT_sms_XS_6m,0) AS COM_CNT_sms_XS_6m
           ,nvl(p1.COM_CNT_sms_XS_12m,0) as COM_CNT_sms_XS_12m

           ,nvl(p1.COM_CNT_sms_card_1m,0) as COM_CNT_sms_card_1m
           ,nvl(p1.COM_CNT_sms_card_3m,0) as COM_CNT_sms_card_3m
           ,nvl(p1.COM_CNT_sms_card_3m,0) as COM_CNT_sms_card_6m
           ,nvl(p1.COM_CNT_sms_card_12m,0) as COM_CNT_sms_card_12m

           ,nvl(p1.COM_CNT_sms_XS_trigger_1m,0) as COM_CNT_sms_XS_trigger_1m
           ,nvl(p1.COM_CNT_sms_XS_trigger_3m,0) as COM_CNT_sms_XS_trigger_3m
           ,nvl(p1.COM_CNT_sms_XS_trigger_6m,0) as COM_CNT_sms_XS_trigger_6m
           ,nvl(p1.COM_CNT_sms_XS_trigger_12m,0) as COM_CNT_sms_XS_trigger_12m

           ,p1.COM_SHARE_sms_deliver_1m
           ,p1.COM_SHARE_sms_deliver_3m
           ,p1.COM_SHARE_sms_deliver_6m
           ,p1.COM_SHARE_sms_deliver_12m

    -----
           ,nvl(p3.COM_CNT_calls_1m,0)  as COM_CNT_calls_1m
           ,nvl(p3.COM_CNT_calls_3m,0)  as COM_CNT_calls_3m
           ,nvl(p3.COM_CNT_calls_6m,0)  as COM_CNT_calls_6m
           ,nvl(p3.COM_CNT_calls_12m,0) as COM_CNT_calls_12m

           ,nvl(p3.COM_CNT_calls_out_1m,0)  as COM_CNT_calls_out_1m
           ,nvl(p3.COM_CNT_calls_out_3m,0)  as COM_CNT_calls_out_3m
           ,nvl(p3.COM_CNT_calls_out_6m,0)  as COM_CNT_calls_out_6m
           ,nvl(p3.COM_CNT_calls_out_12m,0) as COM_CNT_calls_out_12m

           ,nvl(p3.COM_CNT_calls_in_1m,0)   as COM_CNT_calls_in_1m
           ,nvl(p3.COM_CNT_calls_in_3m,0)   as COM_CNT_calls_in_3m
           ,nvl(p3.COM_CNT_calls_in_6m,0)   as COM_CNT_calls_in_6m
           ,nvl(p3.COM_CNT_calls_in_12m,0)  as COM_CNT_calls_in_12m

           ,nvl(p3.COM_CNT_calls_XS_1m,0)   as COM_CNT_calls_XS_1m
           ,nvl(p3.COM_CNT_calls_XS_3m,0)   as COM_CNT_calls_XS_3m
           ,nvl(p3.COM_CNT_calls_XS_6m,0)   as COM_CNT_calls_XS_6m
           ,nvl(p3.COM_CNT_calls_XS_12m,0)  as COM_CNT_calls_XS_12m

           ,nvl(p3.COM_CNT_calls_XS_out_1m,0)  as COM_CNT_calls_XS_out_1m
           ,nvl(p3.COM_CNT_calls_XS_out_3m,0)  as COM_CNT_calls_XS_out_3m
           ,nvl(p3.COM_CNT_calls_XS_out_6m,0)  as COM_CNT_calls_XS_out_6m
           ,nvl(p3.COM_CNT_calls_XS_out_12m,0) as COM_CNT_calls_XS_out_12m

           ,nvl(p3.COM_CNT_calls_XS_in_1m,0)   as COM_CNT_calls_XS_in_1m
           ,nvl(p3.COM_CNT_calls_XS_in_3m,0)   as COM_CNT_calls_XS_in_3m
           ,nvl(p3.COM_CNT_calls_XS_in_6m,0)   as COM_CNT_calls_XS_in_6m
           ,nvl(p3.COM_CNT_calls_XS_in_12m,0)  as COM_CNT_calls_XS_in_12m

           ,nvl(p3.COM_CNT_calls_w_res_1m,0)   as COM_CNT_calls_w_res_1m
           ,nvl(p3.COM_CNT_calls_w_res_3m,0)   as COM_CNT_calls_w_res_3m
           ,nvl(p3.COM_CNT_calls_w_res_6m,0)   as COM_CNT_calls_w_res_6m
           ,nvl(p3.COM_CNT_calls_w_res_12m,0)  as COM_CNT_calls_w_res_12m

           ,nvl(p3.COM_CNT_calls_out_w_res_1m,0)  as COM_CNT_calls_out_w_res_1m
           ,nvl(p3.COM_CNT_calls_out_w_res_3m,0)  as COM_CNT_calls_out_w_res_3m
           ,nvl(p3.COM_CNT_calls_out_w_res_6m,0)  as COM_CNT_calls_out_w_res_6m
           ,nvl(p3.COM_CNT_calls_out_w_res_12m,0) as COM_CNT_calls_out_w_res_12m

           ,nvl(p3.COM_CNT_calls_in_w_res_1m,0)   as COM_CNT_calls_in_w_res_1m
           ,nvl(p3.COM_CNT_calls_in_w_res_3m,0)   as COM_CNT_calls_in_w_res_3m
           ,nvl(p3.COM_CNT_calls_in_w_res_6m,0)   as COM_CNT_calls_in_w_res_6m
           ,nvl(p3.COM_CNT_calls_in_w_res_12m,0)  as COM_CNT_calls_in_w_res_12m

           ,nvl(p3.COM_CNT_calls_service_1m,0)    as COM_CNT_calls_service_1m
           ,nvl(p3.COM_CNT_calls_service_3m,0)    as COM_CNT_calls_service_3m
           ,nvl(p3.COM_CNT_calls_service_6m,0)    as COM_CNT_calls_service_6m
           ,nvl(p3.COM_CNT_calls_service_12m,0)   as COM_CNT_calls_service_12m

           ,nvl(p3.COM_CNT_calls_Neg_in_1m,0)     as COM_CNT_calls_Neg_in_1m
           ,nvl(p3.COM_CNT_calls_Neg_in_3m,0)     as COM_CNT_calls_Neg_in_3m
           ,nvl(p3.COM_CNT_calls_Neg_in_6m,0)     as COM_CNT_calls_Neg_in_6m
           ,nvl(p3.COM_CNT_calls_Neg_in_12m,0)    as COM_CNT_calls_Neg_in_12m

           ,nvl(p3.COM_CNT_calls_busy_in_1m,0)    as COM_CNT_calls_busy_in_1m
           ,nvl(p3.COM_CNT_calls_busy_in_3m,0)    as COM_CNT_calls_busy_in_3m
           ,nvl(p3.COM_CNT_calls_busy_in_6m,0)    as COM_CNT_calls_busy_in_6m
           ,nvl(p3.COM_CNT_calls_busy_in_12m,0)   as COM_CNT_calls_busy_in_12m

           ,nvl(p3.COM_CNT_calls_promise_in_1m,0) as COM_CNT_calls_promise_in_1m
           ,nvl(p3.COM_CNT_calls_promise_in_3m,0) as COM_CNT_calls_promise_in_3m
           ,nvl(p3.COM_CNT_calls_promise_in_6m,0) as COM_CNT_calls_promise_in_6m
           ,nvl(p3.COM_CNT_calls_promise_in_12m,0) as COM_CNT_calls_promise_in_12m

           ,nvl(p3.COM_max_call_lenght_tlm,0)    as COM_max_call_lenght_tlm
           ,nvl(p3.COM_min_call_lenght_tlm,0)    as COM_min_call_lenght_tlm
           ,nvl(p3.COM_avg_call_lenght_tlm,0)    as COM_avg_call_lenght_tlm
           ,nvl(p3.COM_max_call_XS_lenght_tlm,0) as COM_max_call_XS_lenght_tlm
           ,nvl(p3.COM_min_call_XS_lenght_tlm,0) as COM_min_call_XS_lenght_tlm
           ,nvl(p3.COM_avg_call_XS_lenght_tlm,0) as COM_avg_call_XS_lenght_tlm

           ,nvl(p3.COM_max_call_queue_tlm,0)    as COM_max_call_queue_tlm
           ,nvl(p3.COM_min_call_queue_tlm,0)    as COM_min_call_queue_tlm
           ,nvl(p3.COM_avg_call_queue_tlm,0)    as COM_avg_call_queue_tlm
           ,nvl(p3.COM_max_call_XS_queue_tlm,0) as COM_max_call_XS_queue_tlm
           ,nvl(p3.COM_min_call_XS_queue_tlm,0) as COM_min_call_XS_queue_tlm
           ,nvl(p3.COM_avg_call_XS_queue_tlm,0) as COM_avg_call_XS_queue_tlm

           ,nvl(p3.COM_LAST_CALL_STATUS,0)     as COM_LAST_CALL_STATUS
           ,nvl(p3.COM_LAST_CALL_DEPT,0)       as COM_LAST_CALL_DEPT

           ,nvl(p3.COM_CNT_UNIQ_PHONES_6M,0)   as COM_CNT_UNIQ_PHONES_6M
           ,nvl(p3.COM_CNT_UNIQ_PHONES_12M ,0) as COM_CNT_UNIQ_PHONES_12M

           ,p3.COM_SHARE_SUCCESS_1M
           ,p3.COM_SHARE_SUCCESS_3M
           ,p3.COM_SHARE_SUCCESS_6M
           ,p3.COM_SHARE_SUCCESS_12M

           ,nvl(round(p3.COM_CNT_M_LAST_CALL,2),0)         as COM_CNT_M_LAST_CALL
           ,nvl(round(p3.COM_CNT_M_LAST_CALL_SUCCESS,2),0) as COM_CNT_M_LAST_CALL_SUCCESS

    ------
           ,p2.COM_CNT_calls_collection_1m
           ,p2.COM_CNT_calls_collection_3m
           ,p2.COM_CNT_calls_collection_6m
           ,p2.COM_CNT_calls_collection_12m


    from s1

    left join T_ABT_PART7_COMM_SMS p1 on p1.skp_client = s1.skp_client -- SMS
    left join T_ABT_PART7_COMM_LCS  p2 on p2.skp_client = s1.skp_client -- Collcection calls
    left join T_ABT_PART7_COMM_CALL p3 on p3.skp_client = s1.skp_client -- CRM and Oper dep calls
    ;


    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART7_COMM');
                     
                                                                     

      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    
    PROCEDURE P_ABT_PART_8_Appeal(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_8_APPEAL';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      
    
    PKG_MZ_HINTS.pAlterSession(4);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);


    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART8_APPEAL');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART8_APPEAL');

     insert /*+ append*/
         into T_ABT_PART8_APPEAL
         
      WITH W$JIRA as (
      Select /*+ MATERIALIZE USE_HASH(T1 T2 T3 T7 ST) PARALLEL(2)*/
       t1.PROJECT As PROJECT_
      ,t7.PNAME
      ,t1.SUMMARY
      ,st.PNAME As status_issue
      ,t1.CREATED As Date_created
      ,Max(Case
             When t3.ID = '20913' Then
              (Select t6.CUSTOMVALUE
                 From owner_int.vh_jira_customfieldoption_001 t6
                Where t6.ID = Cast(t2.STRINGVALUE As Number)
                  And t6.FLAG_DELETED = 'N'
                  And t6.DISABLED = 'N')
           End) As topic_
      ,Max(Case
             When lower(t3.CFNAME) Like '%подтема%' And
                  lower(t3.CFNAME) Not Like '%подтема%благодар%' Then
              (Select t6.CUSTOMVALUE
                 From owner_int.vh_jira_customfieldoption_001 t6
                Where t6.ID = Cast(t2.STRINGVALUE As Number)
                  And t6.FLAG_DELETED = 'N'
                  And t6.DISABLED = 'N')
             When lower(t3.CFNAME) Like '%подтема%благодар%' Then
              t2.STRINGVALUE
           End) As under_topic
      ,Max(Case
             When t3.ID = '13700' Then
              t2.STRINGVALUE
           End) FIO_client
      ,Max(Case
             When t3.ID = '10200' Then
              t2.STRINGVALUE
           End) IIN
      ,Max(Case
             When t3.ID = '10207' Then
              t2.STRINGVALUE
           End) TEXT_CONTRACT_NUMBER
      ,Max(Case
             When t3.ID = '10311' Then
              t2.STRINGVALUE
           End) PHONE_NUMBER
      
        From owner_int.vh_jira_jiraissue_001 t1
        Join owner_int.vh_jira_customfieldvalue_001 t2
          On t2.issue = t1.id
        Join owner_int.vh_jira_customfield_001 t3
          On t3.id = t2.customfield
        Join owner_int.vh_jira_issuetype_001 t7
          On t7.ID = t1.ISSUETYPE
        Join owner_int.vh_jira_issuestatus_001 st
          On st.ID = t1.issuestatus
      
       Where t1.project = 11400
         And t1.CREATED /*Between Date '2017-12-12' And*/< DATE_CALC
         And t1.FLAG_DELETED = 'N'
         And t2.FLAG_DELETED = 'N'
         And t3.FLAG_DELETED = 'N'
         And t7.PNAME != 'Response - duplicate'
         And t1.ISSUENUM Not In ('144184', '151393')
         And (t3.ID = '20913' Or lower(t3.CFNAME) Like '%подтема%' Or
             t3.ID = '15000' Or t3.ID = '13700' Or t3.ID = '10200' Or
             t3.ID = '10207' Or t3.ID = '10311' Or t3.ID = '13701' Or
             t3.ID = '22600' Or t3.ID = '17901' Or t3.ID = '20931' Or
             lower(t3.CFNAME) Like '%фио%виновного%работника%' Or
             t3.ID = '20933' Or t3.ID = '20932' Or t3.ID = '23100' Or
             t3.ID = '22604' Or t3.ID = '20852' Or t3.ID = '20930' Or
             t3.ID = '20929' Or t3.ID = '21008')
       Group By t1.PROJECT
               ,t7.PNAME
               ,t1.SUMMARY
               ,st.PNAME
               ,t1.CREATED
      ),
    W$AGGR as (
    select  /*+ MATERIALIZE */
            nvl(cl.skp_client, cc.skp_client) as skp_client

           ,count(1) as APPEAL_CNT
           ,count(case when ADD_MONTHS(DATE_CALC,-3) >= p1.date_created then 1 end) as APPEAL_CNT_3M
           ,count(case when ADD_MONTHS(DATE_CALC,-6) >= p1.date_created then 1 end) as APPEAL_CNT_6M
           ,count(case when ADD_MONTHS(DATE_CALC,-12)>= p1.date_created then 1 end) as APPEAL_CNT_12M

           ,count(case when lower(p1.topic_) like '%жалоба%' then 1 end) as APPEAL_CNT_ZHAL
           ,count(case when lower(p1.topic_) like '%жалоба%' and ADD_MONTHS(DATE_CALC,-3) >= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_3M
           ,count(case when lower(p1.topic_) like '%жалоба%' and ADD_MONTHS(DATE_CALC,-6) >= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_6M
           ,count(case when lower(p1.topic_) like '%жалоба%' and ADD_MONTHS(DATE_CALC,-12)>= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_12M

           ,count(case when p1.topic_ = 'ЖАЛОБА: на корреспонденцию банка' and p1.under_topic in ('%Xsell') and p1.under_topic in ('Звонки/смс по текущим кредитам и депозитам','СМС с ошибочными данными') then 1 end)  as APPEAL_CNT_ZHAL_CRM
           ,count(case when p1.topic_ = 'ЖАЛОБА: на корреспонденцию банка' and p1.under_topic in ('%Xsell') and p1.under_topic in ('Звонки/смс по текущим кредитам и депозитам','СМС с ошибочными данными') and ADD_MONTHS(DATE_CALC,-3) >= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_CRM_3M
           ,count(case when p1.topic_ = 'ЖАЛОБА: на корреспонденцию банка' and p1.under_topic in ('%Xsell') and p1.under_topic in ('Звонки/смс по текущим кредитам и депозитам','СМС с ошибочными данными') and ADD_MONTHS(DATE_CALC,-6) >= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_CRM_6M
           ,count(case when p1.topic_ = 'ЖАЛОБА: на корреспонденцию банка' and p1.under_topic in ('%Xsell') and p1.under_topic in ('Звонки/смс по текущим кредитам и депозитам','СМС с ошибочными данными') and ADD_MONTHS(DATE_CALC,-12)>= p1.date_created then 1 end) as APPEAL_CNT_ZHAL_CRM_12M

    from W$JIRA p1
    left join owner_dwh.dc_client      cl on cl.text_identification_number = P1.iin
    left join owner_dwh.dc_credit_case cc on cc.text_contract_numbeR = P1.TEXT_CONTRACT_NUMBER
   group by nvl(cl.skp_client, cc.skp_client) 

    )
    select /*+ parallel(4)*/
            s1.skp_client
           ,s1.APPEAL_CNT
           ,s1.APPEAL_CNT_3M
           ,s1.APPEAL_CNT_6M
           ,s1.APPEAL_CNT_12M

           ,s1.APPEAL_CNT_ZHAL
           ,s1.APPEAL_CNT_ZHAL_3M
           ,s1.APPEAL_CNT_ZHAL_6M
           ,s1.APPEAL_CNT_ZHAL_12M

           ,s1.APPEAL_CNT_ZHAL_CRM
           ,s1.APPEAL_CNT_ZHAL_CRM_3M
           ,s1.APPEAL_CNT_ZHAL_CRM_6M
           ,s1.APPEAL_CNT_ZHAL_CRM_12M

           ,case when s1.APPEAL_CNT     != 0 then s1.APPEAL_CNT_ZHAL/s1.APPEAL_CNT         end as APPEAL_SHARE_ZHAL_TO_ALL
           ,case when s1.APPEAL_CNT_3M  != 0 then s1.APPEAL_CNT_ZHAL_3M/s1.APPEAL_CNT_3M   end as APPEAL_SHARE_ZHAL_TO_ALL_3M
           ,case when s1.APPEAL_CNT_6M  != 0 then s1.APPEAL_CNT_ZHAL_6M/s1.APPEAL_CNT_6M   end as APPEAL_SHARE_ZHAL_TO_ALL_6M
           ,case when s1.APPEAL_CNT_12M != 0 then s1.APPEAL_CNT_ZHAL_12M/s1.APPEAL_CNT_12M end as APPEAL_SHARE_ZHAL_TO_ALL_12M

           ,case when s1.APPEAL_CNT_ZHAL     != 0 then s1.APPEAL_CNT_ZHAL_CRM/s1.APPEAL_CNT_ZHAL         end as APPEAL_SHARE_CRM_TO_ZHAL
           ,case when s1.APPEAL_CNT_ZHAL_3M  != 0 then s1.APPEAL_CNT_ZHAL_CRM_3M/s1.APPEAL_CNT_ZHAL_3M   end as APPEAL_SHARE_CRM_TO_ZHAL_3M
           ,case when s1.APPEAL_CNT_ZHAL_6M  != 0 then s1.APPEAL_CNT_ZHAL_CRM_6M/s1.APPEAL_CNT_ZHAL_6M   end as APPEAL_SHARE_CRM_TO_ZHAL_6M
           ,case when s1.APPEAL_CNT_ZHAL_12M != 0 then s1.APPEAL_CNT_ZHAL_CRM_12M/s1.APPEAL_CNT_ZHAL_12M end as APPEAL_SHARE_CRM_TO_ZHAL_12M

    from W$AGGR S1;


    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART8_APPEAL');
                                                                     

      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    
    PROCEDURE P_ABT_PART_9_Deposit(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_9_DEPOSIT';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      
    
    PKG_MZ_HINTS.pAlterSession(4);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);


    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART9_DEP');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART9_DEP');
    
     
    insert /*+ append*/ into T_ABT_PART9_DEP 
    
    with W$TRN AS
     (
     select /*+ MATERIALIZE*/
            DATE_CALC as month_
           ,tr.skp_account_bank
           ,nvl(sum(case when tr.direction = 'cr' then tr.amt_transaction end),0) - nvl(sum(case when tr.direction = 'db' then tr.amt_transaction end),0) as AMT_Balance
           ,sum(case when tr.direction = 'cr' then tr.amt_transaction end) as amt_top_up_all_hist
           ,sum(case when tr.direction = 'db' then tr.amt_transaction end) as amt_withdraw_hist

      from hckz_korniyenko.T_DEP_DA_SUB2_TRN tr  -- on tr.skp_account_bank = sk.skp_account_bank
     where tr.date_realization < DATE_CALC
       AND TR.SKP_ACCOUNT_BANK = 119446247
     group by DATE_CALC
              ,tr.skp_account_bank
     ),
      t1 as (            
      Select /*+ MATERIALIZE*/
             A.SKP_CLIENT
            ,CL.id_cuid
        From T_ABT_PART0_APPLICATION A
        Join owner_dwh.dc_client cl
          On cl.skp_client = A.skp_client
       Where A.FLAG_BOOKED = 1
       Group By A.SKP_CLIENT
               ,CL.id_cuid
      )
      , s1 as (
      select /*+ MATERIALIZE USE_HASH(DT T1 SK TR)*/
             t1.skp_client
            ,dt.id_source
            ,case when (dt.code_currency = 'KZT' and tr.amt_balance >=1000) 
                    or (dt.code_currency in ('USD','EUR') and tr.amt_balance >=1) then 1 else 0 end is_Active
            ,dt.code_currency
            ,dt.date_created
            ,dt.dtime_valid_to
            ,dt.cnt_term
            ,ADD_MONTHS(dt.date_created,dt.cnt_term) as date_prolong
            ,dt.flag_prolongation
            ,dt.rate_interest
            ,fc.rate_exchange_middle
            ,dt.amt_capitalization
            ,dt.amt_start_balance
            ,tr.amt_balance
            ,case when dt.code_currency = 'KZT' then tr.amt_balance else tr.amt_balance*fc.rate_exchange_middle end AMT_BALANCE_KZT


       from DM_CAMPAIGN_SAS.F_DEP_CONTRACT_AD dt
       join t1 on to_char(t1.id_cuid) = dt.id_cuid -- Only clients with flag had credit = 1
       join HCKZ_KORNIYENKO.M_DEP_DA_DTM sk on sk.idbankaccount = dt.id_account_bank
       left join W$TRN tr on tr.skp_account_bank = sk.skp_account_bank

       left join owner_dwh.cl_currency_obs ob on ob.CODE_CURRENCY = dt.code_currency
       left join owner_dwh.f_currency_rate_td fc on ob.SKP_CURRENCY_OBS = fc.SKP_CURRENCY_OBS_SOURCE
                                                 and fc.SKP_CURRENCY_RATE_STATUS = 4
                                                 and ob.CODE_CURRENCY in ('EUR','USD') --RUB,GBP,CHF,TRY,CNY
                                                 AND DATE_CALC BETWEEN fc.DTIME_RATE_VALID_FROM AND fc.DTIME_RATE_VALID_TO
      WHERE DT.dtime_valid_from < DATE_CALC
      )
      ,s2 as (
      select /*+ MATERIALIZE*/
             s1.skp_client

            ,count(s1.id_source) as DEP_CNT
            ,count(case when s1.is_Active = 1 then s1.id_source end) as  DEP_CNT_OPEN -- dt.name_term_deposit_status  = 'Active'
            ,count(case when s1.is_Active = 0 then s1.id_source end) as  DEP_CNT_CLOSE
            ,count(case when s1.code_currency = 'KZT' then s1.id_source end)  as  DEP_CNT_OPEN_TG
            ,count(case when s1.code_currency != 'KZT' then s1.id_source end) as  DEP_CNT_FOREIGN_CURR
            ,count(case when s1.code_currency = 'USD' then s1.id_source end)  as  DEP_CNT_OPEN_USD
            ,count(case when s1.code_currency = 'EUR' then s1.id_source end)  as  DEP_CNT_OPEN_EUR
            ,count(case when s1.code_currency not in ('KZT','USD','EUR') then s1.id_source end)  as DEP_CNT_OPEN_OTHER_CUR

            ,min(s1.date_created) as date_first_open
            ,max(s1.date_created) as date_last_open
            ,max(case when s1.dtime_valid_to != date '3000-01-01' then s1.dtime_valid_to end) as date_last_close -- date maturity ???
            ,max(case when s1.is_Active = 1 and s1.date_prolong > DATE_CALC then s1.date_prolong end) as date_next_close
            ,max(case when s1.flag_prolongation = 'Y' then ADD_MONTHS(s1.date_prolong,round((MONTHS_BETWEEN(DATE_CALC,s1.date_prolong)/s1.cnt_term) -0.5,0)*s1.cnt_term) end) as date_last_prolon

            ,avg(s1.rate_interest) as DEP_AVG_IR

            ,sum(s1.AMT_BALANCE_KZT) as DEP_AMT
            ,sum(case when s1.is_Active = 1 then s1.AMT_BALANCE_KZT end) as DEP_AMT_OPEN
            ,sum(case when s1.is_Active = 0 then s1.AMT_BALANCE_KZT end) as DEP_AMT_CLOSE

            ,max(s1.AMT_BALANCE_KZT) as DEP_AMT_MAX
            ,min(s1.AMT_BALANCE_KZT) as DEP_AMT_MIN
            ,max(case when s1.is_Active = 1 then s1.AMT_BALANCE_KZT end) as DEP_AMT_OPEN_MAX
            ,min(case when s1.is_Active = 1 then s1.AMT_BALANCE_KZT end) as DEP_AMT_OPEN_MIN
            ,max(case when s1.is_Active = 0 then s1.AMT_BALANCE_KZT end) as DEP_AMT_CLOSE_MAX
            ,min(case when s1.is_Active = 0 then s1.AMT_BALANCE_KZT end) as DEP_AMT_CLOSE_MIN

            ,avg(case when ADD_MONTHS(DATE_CALC,-3) >= s1.date_created then s1.amt_start_balance end) as DEP_AVG_SUM_3M
            ,avg(case when ADD_MONTHS(DATE_CALC,-6) >= s1.date_created then s1.amt_start_balance end) as DEP_AVG_SUM_6M
            ,avg(case when ADD_MONTHS(DATE_CALC,-12)>= s1.date_created then s1.amt_start_balance end) as DEP_AVG_SUM_12M
            ,avg(case when ADD_MONTHS(DATE_CALC,-24)>= s1.date_created then s1.amt_start_balance end) as DEP_AVG_SUM_24M
            ,avg(case when ADD_MONTHS(DATE_CALC,-36)>= s1.date_created then s1.amt_start_balance end) as DEP_AVG_SUM_36M

            ,count(case when s1.amt_capitalization > 0 then s1.id_source end) as DEP_CNT_WITH_CAP
            ,count(case when s1.amt_capitalization > 0 and s1.is_Active = 1 then s1.id_source end) as DEP_CNT_OPEN_WITH_CAP
            ,count(case when s1.amt_capitalization > 0 and s1.is_Active = 0 then s1.id_source end) as DEP_CNT_CLOSE_WITH_CAP

       from s1

       group by s1.skp_client

      )

      select /*+ PARALLEL(4)*/
             s2.skp_client
            ,round(case when s2.date_first_open is not null then months_between(DATE_CALC,s2.date_first_open) end,2) as DEP_CNT_M_FIRST_OPEN
            ,round(case when s2.date_last_open is not null then months_between(DATE_CALC,s2.date_last_open)   end,2) as DEP_CNT_M_LAST_OPEN
            ,round(case when s2.date_last_close is not null then months_between(DATE_CALC,s2.date_last_close) end,2) as DEP_CNT_M_LAST_CLOSE
            ,round(case when s2.date_next_close is not null then months_between(DATE_CALC,s2.date_next_close) end,2) as DEP_CNT_M_NEXT_CLOSE
            ,round(case when s2.date_last_prolon is not null then months_between(DATE_CALC,s2.date_last_prolon) end,2) as DEP_CNT_M_LAST_PROLONG

            ,s2.DEP_CNT
            ,s2.DEP_CNT_OPEN -- dt.name_term_deposit_status  = 'Active'
            ,s2.DEP_CNT_CLOSE
            ,s2.DEP_CNT_OPEN_TG
            ,s2.DEP_CNT_FOREIGN_CURR
            ,s2.DEP_CNT_OPEN_USD
            ,s2.DEP_CNT_OPEN_EUR
            ,s2.DEP_CNT_OPEN_OTHER_CUR

            ,s2.DEP_CNT_WITH_CAP
            ,s2.DEP_CNT_OPEN_WITH_CAP
            ,s2.DEP_CNT_CLOSE_WITH_CAP

            ,s2.DEP_AMT
            ,s2.DEP_AMT_OPEN
            ,s2.DEP_AMT_CLOSE

            ,s2.DEP_AMT_MAX
            ,s2.DEP_AMT_MIN
            ,s2.DEP_AMT_OPEN_MAX
            ,s2.DEP_AMT_OPEN_MIN
            ,s2.DEP_AMT_CLOSE_MAX
            ,s2.DEP_AMT_CLOSE_MIN

            ,round(s2.DEP_AVG_IR,2)      as DEP_AVG_IR
            ,round(s2.DEP_AVG_SUM_3M,2)  as DEP_AVG_SUM_3M
            ,round(s2.DEP_AVG_SUM_6M,2)  as DEP_AVG_SUM_6M
            ,round(s2.DEP_AVG_SUM_12M,2) as DEP_AVG_SUM_12M
            ,round(s2.DEP_AVG_SUM_24M,2) as DEP_AVG_SUM_24M
            ,round(s2.DEP_AVG_SUM_36M,2) as DEP_AVG_SUM_36M

      from s2;
    

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART9_DEP');
        

      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    
    PROCEDURE P_ABT_PART_10_Mapp(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_10_MAPP';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
      
    
    PKG_MZ_HINTS.pAlterSession(4);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);

    
    ---------- STEP 4 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART10_WEB');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART10_WEB');

    insert /*+ append*/
       into T_ABT_PART10_WEB
    
    with s1 as (
    Select /*+ MATERIALIZE*/
     c.skp_client
    ,cl.CHANNEL
    ,trunc(cl.EVENT_DATE, 'mm') As month_
    ,Count(1) As cnt_open
    
      From AP_IT.V_IB_CLIENT_AUTH_HISTORY CL
      Join OWNER_DWH.DC_CLIENT C
        On C.ID_CUID = CL.CUID
     Where cl.EVENT_DATE < DATE_CALC
     Group By c.skp_client
             ,cl.CHANNEL
             ,trunc(cl.EVENT_DATE, 'mm')
    )
    , s2 as (
    select /*+ MATERIALIZE*/
           s1.skp_client
          ,max(case when s1.CHANNEL = 'MB' then month_ end) as date_last_month_MB
          ,max(case when s1.CHANNEL = 'LH' then month_ end) as date_last_month_LH
          ,max(case when s1.CHANNEL = 'IB' then month_ end) as date_last_month_IB

          ,avg(case when s1.CHANNEL = 'MB' then cnt_open end) as avg_open_MB
          ,avg(case when s1.CHANNEL = 'LH' then cnt_open end) as avg_open_LH
          ,avg(case when s1.CHANNEL = 'IB' then cnt_open end) as avg_open_IB
    from s1
    group by s1.skp_client
    )
    ,t1 as (
    select /*+ MATERIALIZE USE_HASH(T O) full(t) FULL(O)*/
            o.SKP_CLIENT
           ,case when t.CODE_RESPONSE_STATUS like '%MB%' or t.CODE_RESPONSE_STATUS like '%МВ' then 'MB' else 'LH' end as channel
           ,trunc(t.DTIME_RESPONSE,'mm') as month_response
           ,count(1) as cnt_response
    from owner_dwh.f_sas_response_tt t
    join owner_Dwh.dc_sas_offer      o      on o.SKP_SAS_OFFER = t.SKP_SAS_OFFER
    where t.DTIME_RESPONSE < DATE_CALC
      and o.DTIME_CREATION < DATE_CALC
      and o.CODE_COMM_CHANNEL = 'MAPP'
      and T.CODE_CHANNEL = 'MAPP'
      AND t.CODE_RESPONSE_STATUS not in ('?????? ?????? X-Sell ?????? ?? ??????? ???????? MB')
      and t.CODE_RESPONSE_STATUS not in ('Предложение не отобразилось')
      --  or (t.CODE_CHANNEL ='PUSH' and t.CODE_RESPONSE_STATUS in ('ANSWERED','DELIVERED','PARTIALLY_DELIVERED'))
           
    group by o.SKP_CLIENT
             ,trunc(t.DTIME_RESPONSE,'mm')
             ,case when t.CODE_RESPONSE_STATUS like '%MB%' or t.CODE_RESPONSE_STATUS like '%МВ' then 'MB' else 'LH' end
    )
    ,t2 as (
    select
           /*+ MATERIALIZE*/
           t1.SKP_CLIENT
          ,max(case when t1.CHANNEL = 'MB' then month_response end) as date_last_month_res_MB
          ,max(case when t1.CHANNEL = 'LH' then month_response end) as date_last_month_res_LH
          ,avg(case when t1.CHANNEL = 'MB' then cnt_response end) as avg_open_res_MB
          ,avg(case when t1.CHANNEL = 'LH' then cnt_response end) as avg_open_res_LH
    from t1
    group by t1.SKP_CLIENT
    )

    select /*+ PARALLEL(4)*/
           s2.skp_client
          ,round(s2.avg_open_MB,2) as web_avg_open_MB
          ,round(s2.avg_open_LH,2) as web_avg_open_LH
          ,round(s2.avg_open_IB,2) as web_avg_open_IB

          ,round(t2.avg_open_res_MB,2) as web_avg_open_res_MB
          ,round(t2.avg_open_res_LH,2) as web_avg_open_res_LH

          ,MONTHS_BETWEEN(DATE_CALC, s2.date_last_month_MB) as web_CNT_M_last_MB
          ,MONTHS_BETWEEN(DATE_CALC, s2.date_last_month_LH) as web_CNT_M_last_LH
          ,MONTHS_BETWEEN(DATE_CALC, s2.date_last_month_IB) as web_CNT_M_last_IB

          ,MONTHS_BETWEEN(DATE_CALC, t2.date_last_month_res_MB) as web_CNT_M_last_res_MB
          ,MONTHS_BETWEEN(DATE_CALC, t2.date_last_month_res_MB) as web_CNT_M_last_res_LH

    from s2
    join t2 on t2.SKP_CLIENT = s2.skp_client;
    
    

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART10_WEB');
                     
                                                                     

      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    PROCEDURE P_ABT_PART_11_Payments(date_clc DATE) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_11_PAYMENTS';
    i_step    NUMBER         := 0;
    cnt_rows  number         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
    
    
    PKG_MZ_HINTS.pAlterSession(8);
    
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);



    
    ---------- STEP 1 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART11_PAYMENTS_CHANNEL');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART11_PAYMENTS_CHANNEL');

    insert /*+ append*/ into T_ABT_PART11_PAYMENTS_CHANNEL
    WITH P1 AS
    (
    select /*+ MATERIALIZE FULL(TT) FULL(FP) USE_HASH(TT FP CL) PARALLEL(4)*/
           case
             when tt.TEXT_PAYMENT_CHANNEL in ('BANK', 'Bank') then          'BANK'
             when tt.TEXT_PAYMENT_CHANNEL in ('KAZPOST', 'Kazpost') then    'KAZPOST'
             when tt.TEXT_PAYMENT_CHANNEL in ('Kiosk') then                 'TERMINAL'
             when tt.TEXT_PAYMENT_CHANNEL in ('Cash') then                  'CASH_BOX'
             when tt.TEXT_PAYMENT_CHANNEL in ('UFO', 'OTHER') then          'OTHER'
             else          tt.TEXT_PAYMENT_CHANNEL --  / IB  / Halyk
           end as PAYMENT_CHANNEL,
           tt.SKP_CLIENT,
           cl.CODE_INSTALMENT_LINE_GROUP as INSTALMENT_GROUP,
           fp.DTIME_PAYMENT              AS date_payment,
           fp.SKP_CREDIT_CASE,
           SUM(fp.AMT_PAYMENT)           AS AMT_PAYMENT

      from OWNER_DWH.F_INCOMING_PAYMENT_TT tt --.TEXT_PAYMENT_CHANNEL
      join OWNER_DWH.F_INSTALMENT_PAYMENT_AD fp
        on fp.SKF_INCOMING_PAYMENT = tt.SKF_INCOMING_PAYMENT
       and fp.SKP_CREDIT_CASE = tt.SKP_CREDIT_CASE
       AND FP.DATE_DECISION = TT.DATE_DECISION
       AND FP.SKP_CREDIT_TYPE = TT.SKP_CREDIT_TYPE

      JOIN OWNER_DWH.CL_INSTALMENT_LINE_TYPE CL
        ON CL.SKP_INSTALMENT_LINE_TYPE = FP.SKP_INSTALMENT_LINE_TYPE

     where fp.DTIME_PAYMENT < DATE_CALC
       AND TT.DATE_DECISION > ADD_MONTHS(TRUNC(DATE_CALC, 'YY'), -60) -- -5 YEAR
       AND TT.FLAG_DELETED = 'N'
       AND FP.SKP_INSTALMENT_REGULARITY in (1, 2, 5)
       AND fp.CODE_INSTALMENT_PAYMENT_STATUS = 'a'
     GROUP BY case
                 when tt.TEXT_PAYMENT_CHANNEL in ('BANK', 'Bank') then          'BANK'
                 when tt.TEXT_PAYMENT_CHANNEL in ('KAZPOST', 'Kazpost') then    'KAZPOST'
                 when tt.TEXT_PAYMENT_CHANNEL in ('Kiosk') then                 'TERMINAL'
                 when tt.TEXT_PAYMENT_CHANNEL in ('Cash') then                  'CASH_BOX'
                 when tt.TEXT_PAYMENT_CHANNEL in ('UFO', 'OTHER') then          'OTHER'
                 else tt.TEXT_PAYMENT_CHANNEL --  / IB  / Halyk
               end,
              tt.SKP_CLIENT,
              cl.CODE_INSTALMENT_LINE_GROUP,
              fp.DTIME_PAYMENT,
              fp.SKP_CREDIT_CASE
     )
                
    SELECT /*+ PARALLEL(4)*/
           P1.SKP_CLIENT
          ,P1.INSTALMENT_GROUP
          ,p1.payment_channel
          ,p1.date_payment
          ,P1.amt_payment
          ,RANK() OVER(PARTITION BY P1.SKP_CLIENT ORDER BY p1.date_payment desc) AS RANK_PAYMENT
          ,lag(p1.DATE_PAYMENT) OVER(PARTITION BY p1.SKP_CLIENT, p1.INSTALMENT_GROUP ORDER BY p1.DATE_PAYMENT) PREV_DATE_PAYMENT
              
      FROM P1;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART11_PAYMENTS_CHANNEL');

          
         

    ---------- STEP 2 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART11_PAYMENTS');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART11_PAYMENTS');

    for i in
    (
    Select p.partition_name, p.partition_position, o.DATA_OBJECT_ID
      From user_tab_partitions p
      join user_objects o
        on o.SUBOBJECT_NAME = p.partition_name
       and o.OBJECT_NAME    = p.table_name
     Where p.table_name = 'T_ABT_PART11_PAYMENTS_CHANNEL'
       --And p.partition_name = 'PRT_0'
     Order By p.partition_position desc
    )
    loop

    pkg_mz_hints.pAppInfo(acAction => 'Part #' || i.partition_position || '; rows ' || cnt_rows);


    insert /*+ append*/
       into T_ABT_PART11_PAYMENTS

    WITH W$PMT AS
    (
    SELECT /*+ MATERIALIZE*/
     * 
      FROM T_ABT_PART11_PAYMENTS_CHANNEL partition (DATAOBJ_TO_PARTITION(T_ABT_PART11_PAYMENTS_CHANNEL, i.DATA_OBJECT_ID))
    ),
    W$CHANNEL AS
    (
    select /*+ MATERIALIZE */
         p2.skp_client
        ,max(p2.date_payment) as date_pmt_max
        ,max(case when p2.payment_channel = 'BANK'     then p2.date_payment end) as date_pmt_max_BANK
        ,max(case when p2.payment_channel = 'TERMINAL' then p2.date_payment end) as date_pmt_max_TERMINAL
        ,max(case when p2.payment_channel = 'CASH_BOX' then p2.date_payment end) as date_pmt_max_CASH_BOX
        ,max(case when p2.payment_channel = 'IB'       then p2.date_payment end) as date_pmt_max_IB
        ,max(case when p2.payment_channel = 'KAZPOST'  then p2.date_payment end) as date_pmt_max_KAZPOST
        ,max(case when p2.payment_channel = 'Halyk'    then p2.date_payment end) as date_pmt_max_Halyk
        ,max(case when p2.payment_channel = 'OTHER'    then p2.date_payment end) as date_pmt_max_OTHER

        from W$PMT p2
        group by p2.skp_client
    )

    select /*+ parallel(8) USE_HASH(W$PMT W$CHANNEL)*/
          p2.SKP_CLIENT
          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_CNT_TOTAL_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_CNT_TOTAL_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_CNT_TOTAL_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_CNT_TOTAL_12M

          ,max(case when p2.rank_payment = 1 then p2.payment_channel end) as PMT_NAME_CHANNEL_LAST_1
          ,max(case when p2.rank_payment = 2 then p2.payment_channel end) as PMT_NAME_CHANNEL_LAST_2
          ,max(case when p2.rank_payment = 3 then p2.payment_channel end) as PMT_NAME_CHANNEL_LAST_3

          ,avg(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_AVG_FEE_3M
          ,avg(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_AVG_FEE_6M
          ,avg(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_AVG_FEE_12M

          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_MODE_FEE_3M
          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_MODE_FEE_6M
          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_MODE_FEE_12M

          ,avg(case when ADD_MONTHS(DATE_CALC,-3) >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_AVG_DAYS_3M
          ,avg(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_AVG_DAYS_6M
          ,avg(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_AVG_DAYS_12M

          ,MEDIAN(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MED_DAYS_3M
          ,MEDIAN(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MED_DAYS_6M
          ,MEDIAN(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MED_DAYS_12M

          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MODE_DAYS_3M
          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MODE_DAYS_6M
          ,STATS_MODE(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_MODE_DAYS_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_CNT_BANK_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_CNT_BANK_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_CNT_BANK_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_CNT_BANK_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_CNT_TER_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_CNT_TER_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_CNT_TER_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_CNT_TER_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_CNT_CASH_BOX_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_CNT_CASH_BOX_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_CNT_CASH_BOX_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_CNT_CASH_BOX_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_CNT_IB_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_CNT_IB_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_CNT_IB_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_CNT_IB_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_CNT_Kazpost_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_CNT_Kazpost_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_CNT_Kazpost_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_CNT_Kazpost_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_CNT_Halyk_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_CNT_Halyk_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_CNT_Halyk_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_CNT_Halyk_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_CNT_OTHER_1M
          ,count(case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_CNT_OTHER_3M
          ,count(case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_CNT_OTHER_6M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_CNT_OTHER_12M

          ,max(case when ADD_MONTHS(DATE_CALC,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_MAX_BANK_12M
          ,min(case when ADD_MONTHS(DATE_CALC,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_MIN_BANK_12M
          ,avg(case when ADD_MONTHS(DATE_CALC,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_AVG_BANK_12M
          ,MEDIAN(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.amt_payment end) as PMT_MED_BANK_12M

          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end)/    NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_BANK_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_TER_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_CASH_BOX_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end)/      NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_IB_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end)/ NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_Kazpost_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end)/   NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_Halyk_12M
          ,count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end)/   NULLIF(count(case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0)as PMT_SHARE_OTHER_12M

          ,count(distinct case when ADD_MONTHS(DATE_CALC,-1)  >= p2.Date_Payment then p2.payment_channel end) as PMT_CNT_DIFF_CHANNELS_1M
          ,count(distinct case when ADD_MONTHS(DATE_CALC,-3)  >= p2.Date_Payment then p2.payment_channel end) as PMT_CNT_DIFF_CHANNELS_3M
          ,count(distinct case when ADD_MONTHS(DATE_CALC,-6)  >= p2.Date_Payment then p2.payment_channel end) as PMT_CNT_DIFF_CHANNELS_6M
          ,count(distinct case when ADD_MONTHS(DATE_CALC,-12) >= p2.Date_Payment then p2.payment_channel end) as PMT_CNT_DIFF_CHANNELS_12M
          -------

          ,count(case when ADD_MONTHS(s1.date_pmt_max,-1)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_LP_CNT_TOTAL_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_LP_CNT_TOTAL_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-6)  >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_LP_CNT_TOTAL_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end) as PMT_LP_CNT_TOTAL_12M

          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_AVG_FEE_3M
          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-6)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_AVG_FEE_6M
          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_AVG_FEE_12M

          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_MODE_FEE_3M
          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-6)  >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_MODE_FEE_6M
          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.instalment_group = 'FEE' then p2.amt_payment end) as PMT_LP_MODE_FEE_12M

          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL'  then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_AVG_DAYS_3M
          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_AVG_DAYS_6M
          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_AVG_DAYS_12M

          ,MEDIAN(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MED_DAYS_3M
          ,MEDIAN(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MED_DAYS_6M
          ,MEDIAN(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MED_DAYS_12M

          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MODE_DAYS_3M
          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MODE_DAYS_6M
          ,STATS_MODE(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment AND P2.instalment_group='PRINCIPAL' then (p2.Date_Payment-p2.PREV_date_payment) end) as PMT_LP_MODE_DAYS_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_BANK,-1)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_LP_CNT_BANK_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_BANK,-3)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_LP_CNT_BANK_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_BANK,-6)  >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_LP_CNT_BANK_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_BANK,-12) >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end) as PMT_LP_CNT_BANK_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_TERMINAL,-1)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_LP_CNT_TER_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_TERMINAL,-3)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_LP_CNT_TER_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_TERMINAL,-6)  >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_LP_CNT_TER_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_TERMINAL,-12) >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end) as PMT_LP_CNT_TER_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_CASH_BOX,-1)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_LP_CNT_CASH_BOX_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_CASH_BOX,-3)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_LP_CNT_CASH_BOX_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_CASH_BOX,-6)  >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_LP_CNT_CASH_BOX_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_CASH_BOX,-12) >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end) as PMT_LP_CNT_CASH_BOX_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_IB,-1)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_LP_CNT_IB_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_IB,-3)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_LP_CNT_IB_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_IB,-6)  >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_LP_CNT_IB_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_IB,-12) >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end) as PMT_LP_CNT_IB_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_KAZPOST,-1)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_LP_CNT_Kazpost_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_KAZPOST,-3)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_LP_CNT_Kazpost_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_KAZPOST,-6)  >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_LP_CNT_Kazpost_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_KAZPOST,-12) >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end) as PMT_LP_CNT_Kazpost_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max_Halyk,-1)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_LP_CNT_Halyk_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_Halyk,-3)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_LP_CNT_Halyk_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_Halyk,-6)  >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_LP_CNT_Halyk_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max_Halyk,-12) >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end) as PMT_LP_CNT_Halyk_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max,-1)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_LP_CNT_OTHER_1M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_LP_CNT_OTHER_3M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-6)  >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_LP_CNT_OTHER_6M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end) as PMT_LP_CNT_OTHER_12M

          -------
          ,max(case when ADD_MONTHS(s1.date_pmt_max,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_LP_MAX_BANK_12M
          ,min(case when ADD_MONTHS(s1.date_pmt_max,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_LP_MIN_BANK_12M
          ,avg(case when ADD_MONTHS(s1.date_pmt_max,-12)  >= p2.Date_Payment   then p2.amt_payment end) as PMT_LP_AVG_BANK_12M
          ,MEDIAN(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.amt_payment end) as PMT_LP_MED_BANK_12M

          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'BANK' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_BANK_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'TERMINAL' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_TER_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'CASH_BOX' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_CASH_BOX_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'IB' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_IB_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'KAZPOST' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_Kazpost_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'Halyk' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_Halyk_12M
          ,count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment and p2.payment_channel = 'OTHER' then p2.SKP_CLIENT end)/NULLIF(count(case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.SKP_CLIENT end), 0) as PMT_LP_SHARE_OTHER_12M

          ,count(distinct case when ADD_MONTHS(s1.date_pmt_max,-1)  >= p2.Date_Payment then p2.payment_channel end) as PMT_LP_CNT_DIFF_CHANNELS_1M
          ,count(distinct case when ADD_MONTHS(s1.date_pmt_max,-3)  >= p2.Date_Payment then p2.payment_channel end) as PMT_LP_CNT_DIFF_CHANNELS_3M
          ,count(distinct case when ADD_MONTHS(s1.date_pmt_max,-6)  >= p2.Date_Payment then p2.payment_channel end) as PMT_LP_CNT_DIFF_CHANNELS_6M
          ,count(distinct case when ADD_MONTHS(s1.date_pmt_max,-12) >= p2.Date_Payment then p2.payment_channel end) as PMT_LP_CNT_DIFF_CHANNELS_12M

    from W$PMT      p2
    join W$CHANNEL  s1    on s1.skp_client = p2.skp_client
    group by p2.SKP_CLIENT;
    
    cnt_rows := cnt_rows + sql%rowcount;
    commit;
    
    end loop;

    PKG_MZ_HINTS.pStepEnd(anRowsResult => cnt_rows,
                          acTable      => 'T_ABT_PART11_PAYMENTS');



      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    PROCEDURE P_ABT_PART_12_FCB(date_clc DATE ) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_PART_12_FCB';
    i_step    NUMBER         := 0;

    BEGIN
    DATE_CALC            := nvl(date_clc, DATE_CALC);
    
    
    PKG_MZ_HINTS.pAlterSession(8);
      -- Start Init Log ---------------------------
    PKG_MZ_HINTS.pStepStart(acModule => AC_MODULE);



    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pTruncate('T_ABT_PART12_FCB');
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_PART12_FCB');

    INSERT /*+ APPEND*/ INTO T_ABT_PART12_FCB
    with w$hcb as (
    select /*+ MATERIALIZE*/
            cc.skp_client
           ,TRUNC(cc.Dtime_Proposal) AS DATE_PROPOSAL
           ,count(cc.skp_credit_case) as cnt_app_HCB
           
    from T_ABT_PART0_APPLICATION CC
    where cc.Dtime_Proposal < DATE_CALC
      and cc.Dtime_Proposal >=  ADD_MONTHS(DATE_CALC,-12)
      and CC.FLAG_IS_DEBIT != 'Y'
    group by cc.skp_client
            ,TRUNC(cc.Dtime_Proposal)
    )
    ,w$FCB AS
    (
    select /*+ materialize*/
     iin,
     new_value,
     event_date,
     lag(event_date) over(partition by iin, TRUNC(event_date) order by event_date, new_value) prev_value_dt
     
      from ap_it.fcb_monitoring fm
     where fm.event_date BETWEEN ADD_MONTHS(DATE_CALC, -12) AND DATE_CALC
       AND TO_CHAR(FM.event_date, 'HH24') BETWEEN '08' AND '22' -- WORK HOURS
       and FM.rule_key = 'MonitoringRule.QRY'
    )
    ,w$hcb_app as (
    select
            /*+ MATERIALIZE*/
            cl.skp_client
           ,trunc(t.event_date) as date_
            ,count(*) cnt_app_FCB
            
    from w$FCB t
    join owner_dwh.dc_client cl on cl.text_identification_number = to_char(t.iin)    
    where event_date-NVL(prev_value_dt, event_date - 1) > INTERVAL '1' MINUTE
    
    group by cl.skp_client
            ,trunc(t.event_date)
    ),
    w$fcb_aggr as
    (
    select /*+ MATERIALIZE*/
           s2.skp_client
          ,sum(case when months_between(DATE_CALC, s2.date_) <= 1  then ( s2.cnt_app_FCB - nvl(cnt_app_HCB,0) ) end ) as cnt_app_1m
          ,sum(case when months_between(DATE_CALC, s2.date_) <= 3  then ( s2.cnt_app_FCB - nvl(cnt_app_HCB,0) ) end ) as cnt_app_3m
          ,sum(case when months_between(DATE_CALC, s2.date_) <= 6  then ( s2.cnt_app_FCB - nvl(cnt_app_HCB,0) ) end ) as cnt_app_6m
          ,sum(case when months_between(DATE_CALC, s2.date_) <= 12 then ( s2.cnt_app_FCB - nvl(cnt_app_HCB,0) ) end ) as cnt_app_12m

    from w$hcb_app s2
    left join w$hcb s1 on s1.skp_client = s2.skp_client
                 and s1.date_proposal = s2.date_
                 and s1.cnt_app_HCB > s2.cnt_app_FCB

    where s1.skp_client is null
    group by s2.skp_client
    ),
    W$FCB_CNTR as 
    (
    select  /*+ materialize*/
            FCB.skp_client
           ,min(fcb.date_credit_start) as date_min_credit
           ,max(fcb.date_credit_start) as date_max_credit
           ,max(fcb.date_credit_end)   as date_max_close
           ,avg( MONTHS_BETWEEN(fcb.date_credit_end,fcb.date_credit_start) ) as avg_term_plan
           ,avg( MONTHS_BETWEEN(fcb.date_close,fcb.date_credit_start) )      as avg_term_fact
           ,min(case when fcb.code_founding_type in ('Займ','Кредитная карта') then nvl(fcb.amt_total, fcb.amt_credit_limit) end) as min_amt_credit
           ,max(case when fcb.code_founding_type in ('Займ','Кредитная карта') then nvl(fcb.amt_total, fcb.amt_credit_limit) end) as max_amt_credit
           ,avg(case when fcb.code_founding_type in ('Займ','Кредитная карта') then nvl(fcb.amt_total, fcb.amt_credit_limit) end) as avg_amt_credit
           ,sum( nvl(fcb.amt_total, fcb.amt_credit_limit) ) as sum_amt_credit_w_m
           ,sum(case when fcb.code_founding_type in ('Займ','Кредитная карта') then nvl(fcb.amt_total, fcb.amt_credit_limit) end) as sum_amt_credit_wo_m

           ,sum(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  = 'Кредитная карта' then (fcb.amt_credit_limit)/12
                     when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type != 'Кредитная карта'
                            and fcb.amt_total > 0 and fcb.cnt_instalments >0  then fcb.amt_total/ fcb.cnt_instalments
                     end) as sum_annuity_w_m
           ,sum(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  = 'Кредитная карта' then (fcb.amt_credit_limit)/12
                     when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  = 'Займ'
                           and fcb.amt_total > 0 and fcb.cnt_instalments >0  then fcb.amt_total/ fcb.cnt_instalments
                     end) as sum_annuity_wo_m

           ,sum(fcb.cnt_max_dpd) as cnt_dpd
           ,avg(fcb.cnt_max_dpd) as avg_dpd

           ,count(case when fcb.date_close < fcb.date_credit_end - 30 then 1 end ) cnt_early_close
           ,count(1) cnt_all
           ,count(case when fcb.code_founding_type  = 'Займ' then 1 end ) cnt_loan
           ,count(case when fcb.code_founding_type  = 'Кредитная карта' then 1 end ) cnt_card
           ,count(case when fcb.code_founding_type  like 'АО «Банк Развития Казахстана» %' then 1 end ) cnt_car
           ,count(case when fcb.code_founding_type  like 'Программа%ипотечн%' then 1 end ) cnt_mogtrage
           ,count(case when fcb.code_founding_type  like '%АО%ФРП%Даму%' then 1 end ) cnt_biz

           ,count(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC then 1 end) cnt_all_act
           ,count(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  = 'Займ' then 1 end) cnt_loan_act
           ,count(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  = 'Кредитная карта' then 1 end ) cnt_card_act
           ,count(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  like 'АО «Банк Развития Казахстана» %' then 1 end ) cnt_car_act
           ,count(case when least(fcb.date_credit_end,fcb.date_close) > DATE_CALC  and fcb.code_founding_type  like 'Программа%ипотечн%' then 1 end ) cnt_mogtrage_act
           ,count(case when fcb.code_founding_type like '%АО%ФРП%Даму%' then 1 end ) cnt_biz_act

    from AP_RISK.VW_FCB_CONTRACTS fcb

    where fcb.date_credit_start <= DATE_CALC
      and fcb.date_credit_start != date '1900-01-01'

     group by FCB.skp_client
    ),    
    W$UNN as ( -- clients
    select /*+ materialize*/
           t1.skp_client
    from  W$FCB_AGGR t1
    union
    select t2.skp_client
    from  W$FCB_CNTR t2
    )

    select /*+ PARALLEL(8)*/
           s1.skp_client
          ,nvl(p1.cnt_app_1m,0)   as FCB_CNT_APP_1M
          ,nvl(p1.cnt_app_3m,0)   as FCB_CNT_APP_3M
          ,nvl(p1.cnt_app_6m,0)   as FCB_CNT_APP_6M
          ,nvl(p1.cnt_app_12m,0)  as FCB_CNT_APP_12M

          ,case when p2.date_min_credit is not null then round(months_between(DATE_CALC,p2.date_min_credit),2) end as FCB_CNT_M_FIRST_OPEN
          ,case when p2.date_max_credit is not null then round(months_between(DATE_CALC,p2.date_max_credit),2) end as FCB_CNT_M_LAST_OPEN
          ,case when p2.date_max_close  is not null then round(months_between(DATE_CALC,p2.date_max_close),2)  end as FCB_CNT_M_LAST_CLOSE

          ,round(p2.avg_term_plan,2)   as FCB_AVG_TERM_PLAN
          ,round(p2.avg_term_fact,2)   as FCB_AVG_TERM_FACT

          ,p2.min_amt_credit           as FCB_MIN_AMT_CREDIT
          ,p2.max_amt_credit           as FCB_MAX_AMT_CREDIT
          ,round(p2.avg_amt_credit,2)  as FCB_AVG_AMT_CREDIT

          ,p2.sum_amt_credit_w_m       as FCB_SUM_AMT_CREDIT_W_MOR
          ,p2.sum_amt_credit_wo_m      as FCB_SUM_AMT_CREDIT_W_O_MOR
          ,p2.sum_annuity_w_m          as FCB_SUM_ANNUITY_W_MOR
          ,p2.sum_annuity_wo_m         as FCB_SUM_ANNUITY_W_O_MOR
          ,p2.cnt_dpd                  as FCB_CNT_OVERDUE
          ,round(p2.avg_dpd,2)         as FCB_MAX_OVERDUE

          ,p2.cnt_early_close          as FCB_CNT_EARLY_CLOSE
          ,nvl(p2.cnt_all,0)           as FCB_CNT_CONTRACT
          ,p2.cnt_loan                 as FCB_CNT_CONTRACT_LOAN
          ,p2.cnt_card                 as FCB_CNT_CONTRACT_CARD
          ,p2.cnt_car                  as FCB_CNT_CONTRACT_CAR
          ,p2.cnt_mogtrage             as FCB_CNT_CONTRACT_MOR
          ,p2.cnt_biz                  as FCB_CNT_CONTRACT_biz

          ,p2.cnt_all_act              as FCB_CNT_CONTRACT_ACT
          ,p2.cnt_loan_act             as FCB_CNT_CONTRACT_LOAN_ACT
          ,p2.cnt_card_act             as FCB_CNT_CONTRACT_CARD_ACT
          ,p2.cnt_car_act              as FCB_CNT_CONTRACT_CAR_ACT
          ,p2.cnt_mogtrage_act         as FCB_CNT_CONTRACT_MOR_ACT
          ,p2.cnt_biz_act              as FCB_CNT_CONTRACT_biz_ACT

    from W$UNN S1
    left join W$FCB_AGGR p1 on p1.skp_client = s1.skP_client
    left join W$FCB_CNTR p2 on p2.skp_client = s1.skP_client; 
    
    
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_PART12_FCB');                          
                              
      -- Finish Log  ------------------------------
      PKG_MZ_HINTS.pStepEnd(isFinish => 1);

    EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PKG_MZ_HINTS.pStepErr(fnEmailSend => 0);
        DBMS_OUTPUT.put_line(SQLCODE || SQLERRM);
        --raise_application_error(-20123, 'Error in STEP (' || I_step || ') ' ||SQLERRM, True);
    end;
    
    
    
    
    
    
    PROCEDURE P_ABT_Cash_DataMart(date_clc DATE ) IS

    AC_MODULE VARCHAR2(30)   := 'P_ABT_CASH_DATAMART';
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
      From T_ABT_CASH_DATAMART T
     Where T.MONTH_ = DATE_CALC;
    
    IF IS_CUR_MONTH = 0 THEN
    
    ---------- STEP 0 ---------------------
    I_STEP := I_STEP + 1;
    PKG_MZ_HINTS.pStepStart(anStepNo  => i_step,
                            acAction  => 'T_ABT_CASH_DATAMART');
    
    insert /*+ append*/ into T_ABT_CASH_DATAMART
    select /*+ USE_HASH(S0 S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 DM) PARALLEL(4)*/
          s0.skp_client
         ,s0.month_ 
         ,s1.Sd_Age
         ,s1.Sd_Flag_Pensioner
         ,s1.Sd_Name_City
         ,s1.Sd_Amt_Income_Main
         ,s1.Sd_Amt_Income_Other
         ,s1.Sd_Preferred_Language
         ,s1.Sd_Age_Month
         ,s1.SD_CODE_GENDER
         ,s1.SD_INCOME_TYPE_CODE
         ,s1.SD_INCOME_TYPE
         ,s1.SD_CNT_CHILDREN
         ,s1.SD_EDUCATION_TYPE
         ,s1.SD_CODE_FAMILY_STATUS
         ,s1.SD_NAME_HOUSING_TYPE
         ,s1.SD_NAME_REGION 
         ,s1.SD_SHARE_INCOME_6_M_reg
         
    --------------------------------------------     
         
         ,s2.cmp_type_group
         ,s2.cmp_type_product
         ,s2.Cmp_Flag_Rd_Pool
         ,s2.Cmp_Amt_Offer
         ,s2.CMP_RISK_GRADE
         ,s2.Cmp_Cnt_Contract_Act
         ,s2.Cmp_Amt_Credit_Act
         ,s2.Cmp_Amt_Debt_Act
         ,s2.Cmp_Flag_Ever_Cash
         ,s2.Cmp_Flag_Ever_Card
         ,s2.Cmp_Cnt_m_Last_Cash_Open
         ,s2.Cmp_Cnt_m_Last_Cash_Close
         ,s2.Cmp_Cnt_m_Last_Card_Open
         ,s2.Cmp_Cnt_m_Last_Card_Close
         ,s2.Cmp_Name
         ,s2.Cmp_Date_Valid_From
         ,s2.Cmp_Date_Valid_To
         
    --------------------------------------------   
         ,s3.App_Cnt_m_Last
         ,s3.App_Cnt_m_Last_Cash
         ,s3.App_Cnt_m_Last_Cash_Xs
         ,s3.App_Cnt_m_Last_Card
         ,s3.App_Cnt_m_Last_Card_Xs
         ,s3.App_Cnt_m_Last_Pos
         ,s3.App_Rate_Aproval
         ,s3.App_Rate_Aproval_Cash
         ,s3.App_Rate_Aproval_Card
         ,s3.App_Rate_Aproval_Pos
         ,s3.App_Rate_Aproval_Xs
         ,s3.App_Rate_Aproval_Cash_Xs
         ,s3.App_Rate_Aproval_Card_Xs
         ,s3.App_Cnt_Pos
         ,s3.App_Cnt_Cash
         ,s3.App_Cnt_Card
         ,s3.App_Cnt_Xsell
         ,s3.App_Cnt_Cash_Xs
         ,s3.App_Cnt_Card_Xs
         ,s3.App_Cnt_Pos_Zp
         ,s3.App_Cnt_Pos_St
         ,s3.App_Cnt_Cash_3m
         ,s3.App_Cnt_Cash_6m
         ,s3.App_Cnt_Cash_12m
         ,s3.App_Cnt_Cash_24m
         ,s3.App_Cnt_Pos_3m
         ,s3.App_Cnt_Pos_6m
         ,s3.App_Cnt_Pos_12m
         ,s3.App_Cnt_Pos_24m
         ,s3.App_Cnt_Card_3m
         ,s3.App_Cnt_Card_6m
         ,s3.App_Cnt_Card_12m
         ,s3.App_Cnt_Card_24m
         ,s3.App_Flag_Cash_Full_1m
         ,s3.App_Flag_Cash_Book_1m
         ,s3.App_Flag_Card_Full_1m
         ,s3.App_Flag_Card_Book_1m
         ,s3.App_Flag_Pos_Full_1m
         ,s3.App_Flag_Pos_Book_1m
         ,s3.App_Flag_Cash_Full_Ct
         ,s3.App_Flag_Cash_Book_Ct
         ,s3.App_Flag_Card_Full_Ct
         ,s3.App_Flag_Card_Book_Ct
         ,s3.App_Flag_Pos_Full_Ct
         ,s3.App_Flag_Pos_Book_Ct
         ,s3.App_Share_Cash_12m
         ,s3.App_Share_Cash_24m
         ,s3.App_Share_Cash_36m
         ,s3.App_Cnt_Rej
         ,s3.App_Cnt_Rej_Cash
         ,s3.App_Cnt_Rej_Cash_3m
         ,s3.App_Cnt_Rej_Cash_6m
         ,s3.App_Cnt_Rej_Cash_12m
         ,s3.App_Cnt_Rej_Cash_24m
         ,s3.App_Cnt_Rej_Cash_36m
         ,s3.App_Share_Rej_Cash_3m
         ,s3.App_Share_Rej_Cash_6m
         ,s3.App_Share_Rej_Cash_12m
         ,s3.App_Share_Rej_Cash_24m
         ,s3.App_Share_Rej_Cash_36m
         ,s3.App_Cnt_Room_Mbr
         ,s3.App_Cnt_Room_Br
         ,s3.App_Cnt_Room_Kp
         ,s3.App_Cnt_Room_Pos
        
    ----------------------------------------------
         
         ,s4.La_Credit_Status
         ,s4.La_Credit_Status_Group
         ,s4.La_Term
         ,s4.La_Flag_With_Ins
         ,s4.La_Flag_With_Ins_Box
         ,s4.La_Flag_With_Ins_St
         ,s4.La_Number_Of_Gift
         ,s4.La_Rate_Interest
         ,s4.La_Rate_Interest_First
         ,s4.La_Rate_Interest_Cash
         ,s4.La_Rate_Interest_Cash_Xs
         ,s4.La_Rate_Interest_Pos
         ,s4.La_Rate_Interest_Card
         ,s4.La_Amt_Credit
         ,s4.La_Amt_Credit_Total
         ,s4.La_Amt_Credit_Total_Cash
         ,s4.La_Amt_Credit_Total_Pos
         ,s4.La_Amt_Credit_Total_Card
         ,s4.La_Product_Type
         ,s4.La_Product_Type_Group
         ,s4.La_Channel
         ,s4.La_Channel_Group
         ,s4.La_Channel_Pos
         ,s4.La_Name_Goods_Category
         ,s4.La_Number_Active_Contract

    ----------------------------------------------
       
         ,s5.Lc_Flag_With_Ins
         ,s5.Lc_Flag_With_Ins_St
         ,s5.Lc_Flag_With_Ins_Box
         ,s5.Lc_Rate_Interest
         ,s5.Lc_Rate_Interest_First
         ,s5.Lc_Rate_Interest_Cash
         ,s5.Lc_Rate_Interest_Cash_Xs
         ,s5.Lc_Rate_Interest_Pos
         ,s5.Lc_Rate_Interest_Card
         ,s5.Lc_Term
         ,s5.Lc_Product_Type_Group
         ,s5.Lc_Product_Type
         ,s5.Lc_Amt_Credit
         ,s5.Lc_Amt_Credit_Total
         ,s5.Lc_Amt_Credit_Cash
         ,s5.Lc_Amt_Credit_Pos
         ,s5.Lc_Amt_Credit_Card
         ,s5.Lc_Credit_Status
         ,s5.Lc_Credit_Status_Group
         ,s5.Lc_Name_Goods_Category
         ,s5.Lc_Channel
         ,s5.Lc_Channel_Group
         ,s5.Lc_Channel_Pos
         ,s5.Lc_Number_Of_Gift
         ,s5.Lc_Date_Last_Payment
         ,s5.Lc_Number_Active_Contract
     
    ----------------------------------------------
        
         ,s6.Con_Cnt_m_Last
         ,s6.Con_Cnt_m_Last_Cash
         ,s6.Con_Cnt_m_Last_Cash_Xs
         ,s6.Con_Cnt_m_Last_Card
         ,s6.Con_Cnt_m_Last_Card_Xs
         ,s6.Con_Cnt_m_Last_Pos
         ,s6.Con_Cnt_All
         ,s6.Con_Cnt_Pos
         ,s6.Con_Cnt_Cash
         ,s6.Con_Cnt_Card
         ,s6.Con_Cnt_Xsell
         ,s6.Con_Cnt_Cash_Xs
         ,s6.Con_Cnt_Card_Xs
         ,s6.Con_Cnt_Pos_Zp
         ,s6.Con_Cnt_Pos_St
         ,s6.Con_Cnt_Card_Debit
         ,s6.Con_Cnt_a
         ,s6.Con_Cnt_Cash_a
         ,s6.Con_Cnt_Pos_a
         ,s6.Con_Cnt_Pos_Zp_a
         ,s6.Con_Cnt_Pos_St_a
         ,s6.Con_Share_Pos_Zp_a_To_All_a
         ,s6.Con_Share_Pos_Zp_a_To_Pos
         ,s6.Con_Flag_Has_Card
         ,s6.Con_Flag_Has_Card_Debit
         ,s6.Con_Cnt_Card_Use
         ,s6.Con_Cnt_Card_Pin
         ,s6.Con_Cnt_Card_Use_And_Pin
         ,s6.Con_Flag_Has_Card_Use
         ,s6.Con_Flag_Has_Card_Pin
         ,s6.Con_Flag_Has_Card_Use_Pin
         ,s6.Con_Ir_Avg
         ,s6.Con_Ir_Avg_Cash
         ,s6.Con_Ir_Avg_Pos
         ,s6.Con_Ir_Max
         ,s6.Con_Ir_Min
         ,s6.Con_Ir_Max_Pos
         ,s6.Con_Ir_Min_Pos
         ,s6.Con_Ir_Max_Cash
         ,s6.Con_Ir_Min_Cash
         ,s6.Con_Amt_Sum_Credit
         ,s6.Con_Amt_Sum_Credit_Cash
         ,s6.Con_Amt_Sum_Credit_Pos
         ,s6.Con_Amt_Sum_Credit_Pos_Zp
         ,s6.Con_Amt_Sum_Credit_Card
         ,s6.Con_Amt_Sum_c_Card_Pin
         ,s6.Con_Amt_Sum_c_Card_Use
         ,s6.Con_Amt_Sum_c_Card_Use_Pin
         ,s6.Con_Amt_Sum_c_Card_Usage
         ,s6.Con_Amt_Sum_Credit_a
         ,s6.Con_Amt_Sum_Credit_Cash_a
         ,s6.Con_Amt_Sum_Credit_Pos_a
         ,s6.Con_Amt_Sum_Credit_Pos_Zp_a
         ,s6.Con_Amt_Sum_Credit_Card_a
         ,s6.Con_Amt_Sum_c_Card_Pin_a
         ,s6.Con_Amt_Sum_c_Card_Use_a
         ,s6.Con_Amt_Sum_c_Card_Use_Pin_a
         ,s6.Con_Cnt_Drawing_Atm
         ,s6.Con_Amt_Drawing_Atm
         ,s6.Con_Cnt_m_First_Pin
         ,s6.Con_Cnt_m_Last_Pin
         ,s6.Con_Cnt_m_First_Use
         ,s6.Con_Cnt_m_Last_Use
         ,s6.Con_Amt_Sum_Annuity
         ,s6.Con_Amt_Sum_Annuity_Cash
         ,s6.Con_Amt_Sum_Annuity_Pos
         ,s6.Con_Avg_Ins_Per_Contra
         ,s6.Con_Amt_Ins
         ,s6.Con_Amt_Ins_Box
         ,s6.Con_Amt_Ins_Life
         ,s6.Con_Cnt_Ins
         ,s6.Con_Cnt_Ins_Box
         ,s6.Con_Cnt_Ins_Life
    --     ,s6.CNT_INS_RET
    --     ,s6.AMT_INS_RE
    --     ,s6.SHARE_CNT_INS_RET
    --     ,s6.SHARE_AMT_INS_RET
    --     ,s6.AVG_AMT_INS_PER_CONTRA
         ,s6.Con_Cnt_Pdp
         ,s6.Con_Cnt_Pdp_Cash
         ,s6.Con_Cnt_Pdp_Pos
         ,s6.Con_Share_Pdp_To_All
         ,s6.Con_Share_Cash_Pdp_To_All
         ,s6.Con_Share_Pos_Pdp_To_All
         ,s6.Con_Share_Cash_Pdp_To_Cash
         ,s6.Con_Share_Pos_Pdp_To_Pos
         ,s6.Con_Flag_Pdp_1m
         ,s6.Con_Flag_Pdp_3m
         ,s6.Con_Flag_Pdp_6m
         ,s6.Con_Flag_Pdp_12m
         ,s6.Con_Flag_Pdp_Cash_1m
         ,s6.Con_Flag_Pdp_Cash_3m
         ,s6.Con_Flag_Pdp_Cash_6m
         ,s6.Con_Flag_Pdp_Cash_12m
         ,s6.Con_Flag_Pdp_Pos_1m
         ,s6.Con_Flag_Pdp_Pos_3m
         ,s6.Con_Flag_Pdp_Pos_6m
         ,s6.Con_Flag_Pdp_Pos_12m
         ,s6.Con_Share_Ir_Med_Last_1
         ,s6.Con_Share_Ir_Med_Last_2
         ,s6.Con_Share_Ir_Med_Last_3
         ,s6.Con_Share_Ir_Avg_Med
         ,s6.Con_Share_Ir_Med_Last_1_Pos
         ,s6.Con_Share_Ir_Med_Last_2_Pos
         ,s6.Con_Share_Ir_Med_Last_3_Pos
         ,s6.Con_Share_Ir_Avg_Med_Pos
         ,s6.Con_Share_Ir_Med_Last_1_Cash
         ,s6.Con_Share_Ir_Med_Last_2_Cash
         ,s6.Con_Share_Ir_Med_Last_3_Cash
         ,s6.Con_Share_Ir_Avg_Med_Cash
         ,s6.Con_Share_Amt_Med_Last_1
         ,s6.Con_Share_Amt_Med_Last_2
         ,s6.Con_Share_Amt_Med_Last_3
         ,s6.Con_Share_Amt_Avg_Med
         ,s6.Con_Share_Amt_Med_Last_1_Pos
         ,s6.Con_Share_Amt_Med_Last_2_Pos
         ,s6.Con_Share_Amt_Med_Last_3_Pos
         ,s6.Con_Share_Amt_Avg_Med_Pos
         ,s6.Con_Share_Amt_Med_Last_1_Cash
         ,s6.Con_Share_Amt_Med_Last_2_Cash
         ,s6.Con_Share_Amt_Med_Last_3_Cash
         ,s6.Con_Share_Amt_Avg_Med_Cash
         ,s6.Con_Cnt_Mb_Last_1
         ,s6.Con_Cnt_Mb_Last_2
         ,s6.Con_Cnt_Mb_Last_3
         ,s6.Con_Avg_Mb
         ,s6.Con_Amt_Pay_Interest_All
         ,s6.Con_Amt_Pay_Interest_Pos
         ,s6.Con_Amt_Pay_Interest_Cash
         ,s6.Con_Amt_Pay_Interest_Card
         ,s6.Con_Cnt_m_First_Payment
         ,s6.Con_Cnt_m_Last_Payment
         ,s6.Con_Cnt_Pcc_All
         ,s6.Con_Flag_Pcc_All
         ,s6.Con_Cnt_Pcc_All_a
         ,s6.Con_Flag_Pcc_All_a

    ----------------------------------------------
        
         ,s7.Com_Cnt_Sms_1m
         ,s7.Com_Cnt_Sms_3m
         ,s7.Com_Cnt_Sms_6m
         ,s7.Com_Cnt_Sms_12m
         ,s7.Com_Cnt_Sms_Xs_1m
         ,s7.Com_Cnt_Sms_Xs_3m
         ,s7.Com_Cnt_Sms_Xs_6m
         ,s7.Com_Cnt_Sms_Xs_12m
         ,s7.Com_Cnt_Sms_Xs_Trigger_1m
         ,s7.Com_Cnt_Sms_Xs_Trigger_3m
         ,s7.Com_Cnt_Sms_Xs_Trigger_6m
         ,s7.Com_Cnt_Sms_Xs_Trigger_12m
         ,s7.Com_Cnt_Sms_Card_1m
         ,s7.Com_Cnt_Sms_Card_3m
         ,s7.Com_Cnt_Sms_Card_6m
         ,s7.Com_Cnt_Sms_Card_12m
         ,s7.Com_Share_Sms_Deliver_1m
         ,s7.Com_Share_Sms_Deliver_3m
         ,s7.Com_Share_Sms_Deliver_6m
         ,s7.Com_Share_Sms_Deliver_12m
         ,s7.Com_Cnt_Calls_1m
         ,s7.Com_Cnt_Calls_3m
         ,s7.Com_Cnt_Calls_6m
         ,s7.Com_Cnt_Calls_12m
         ,s7.Com_Cnt_Calls_Out_1m
         ,s7.Com_Cnt_Calls_Out_3m
         ,s7.Com_Cnt_Calls_Out_6m
         ,s7.Com_Cnt_Calls_Out_12m
         ,s7.Com_Cnt_Calls_In_1m
         ,s7.Com_Cnt_Calls_In_3m
         ,s7.Com_Cnt_Calls_In_6m
         ,s7.Com_Cnt_Calls_In_12m
         ,s7.Com_Cnt_Calls_Xs_1m
         ,s7.Com_Cnt_Calls_Xs_3m
         ,s7.Com_Cnt_Calls_Xs_6m
         ,s7.Com_Cnt_Calls_Xs_12m
         ,s7.Com_Cnt_Calls_Xs_Out_1m
         ,s7.Com_Cnt_Calls_Xs_Out_3m
         ,s7.Com_Cnt_Calls_Xs_Out_6m
         ,s7.Com_Cnt_Calls_Xs_Out_12m
         ,s7.Com_Cnt_Calls_Xs_In_1m
         ,s7.Com_Cnt_Calls_Xs_In_3m
         ,s7.Com_Cnt_Calls_Xs_In_6m
         ,s7.Com_Cnt_Calls_Xs_In_12m
         ,s7.Com_Cnt_Calls_w_Res_1m
         ,s7.Com_Cnt_Calls_w_Res_3m
         ,s7.Com_Cnt_Calls_w_Res_6m
         ,s7.Com_Cnt_Calls_w_Res_12m
         ,s7.Com_Cnt_Calls_Out_w_Res_1m
         ,s7.Com_Cnt_Calls_Out_w_Res_3m
         ,s7.Com_Cnt_Calls_Out_w_Res_6m
         ,s7.Com_Cnt_Calls_Out_w_Res_12m
         ,s7.Com_Cnt_Calls_In_w_Res_1m
         ,s7.Com_Cnt_Calls_In_w_Res_3m
         ,s7.Com_Cnt_Calls_In_w_Res_6m
         ,s7.Com_Cnt_Calls_In_w_Res_12m
         ,s7.Com_Cnt_Calls_Service_1m
         ,s7.Com_Cnt_Calls_Service_3m
         ,s7.Com_Cnt_Calls_Service_6m
         ,s7.Com_Cnt_Calls_Service_12m
         ,s7.Com_Cnt_Calls_Collection_1m
         ,s7.Com_Cnt_Calls_Collection_3m
         ,s7.Com_Cnt_Calls_Collection_6m
         ,s7.Com_Cnt_Calls_Collection_12m

    --     ,s7.COM_CNT_calls_PDP_in_1
    --     ,s7.COM_CNT_calls_PDP_in_3m
    --     ,s7.COM_CNT_calls_PDP_in_6m
    --     ,s7.COM_CNT_calls_PDP_in_12m
         ,s7.Com_Cnt_Calls_Neg_In_1m
         ,s7.Com_Cnt_Calls_Neg_In_3m
         ,s7.Com_Cnt_Calls_Neg_In_6m
         ,s7.Com_Cnt_Calls_Neg_In_12m
         ,s7.Com_Cnt_Calls_Busy_In_1m
         ,s7.Com_Cnt_Calls_Busy_In_3m
         ,s7.Com_Cnt_Calls_Busy_In_6m
         ,s7.Com_Cnt_Calls_Busy_In_12m
         ,s7.Com_Cnt_Calls_Promise_In_1m
         ,s7.Com_Cnt_Calls_Promise_In_3m
         ,s7.Com_Cnt_Calls_Promise_In_6m
         ,s7.Com_Cnt_Calls_Promise_In_12m

         ,s7.Com_Max_Call_Lenght_Tlm
         ,s7.Com_Min_Call_Lenght_Tlm
         ,s7.Com_Avg_Call_Lenght_Tlm
         ,s7.Com_Max_Call_Xs_Lenght_Tlm
         ,s7.Com_Min_Call_Xs_Lenght_Tlm
         ,s7.Com_Avg_Call_Xs_Lenght_Tlm
         ,s7.Com_Max_Call_Queue_Tlm
         ,s7.Com_Min_Call_Queue_Tlm
         ,s7.Com_Avg_Call_Queue_Tlm
         ,s7.Com_Max_Call_Xs_Queue_Tlm
         ,s7.Com_Min_Call_Xs_Queue_Tlm
         ,s7.Com_Avg_Call_Xs_Queue_Tlm
         ,s7.Com_Last_Call_Dept
    --     ,s7.COM_flag_had_CATI_call_6m
         ,s7.Com_Last_Call_Status
         ,s7.Com_Cnt_m_Last_Call
         ,s7.Com_Cnt_m_Last_Call_Success

         ,s7.Com_Share_Success_1m
         ,s7.Com_Share_Success_3m
         ,s7.Com_Share_Success_6m
         ,s7.Com_Share_Success_12m
         ,s7.Com_Cnt_Uniq_Phones_6m
         ,s7.Com_Cnt_Uniq_Phones_12m
    --     ,s7.COM_CNT_UNIQ_PHONES_24M
    --     ,s7.COM_CNT_UNIQ_PHONES_36M

    ----------------------------------------------

         ,s8.appeal_cnt
         ,s8.appeal_cnt_3m 
         ,s8.appeal_cnt_6m
         ,s8.appeal_cnt_12m
         ,s8.appeal_cnt_zhal
         ,s8.appeal_cnt_zhal_3m
         ,s8.appeal_cnt_zhal_6m
         ,s8.appeal_cnt_zhal_12m
         ,s8.appeal_cnt_zhal_crm
         ,s8.appeal_cnt_zhal_crm_3m
         ,s8.appeal_cnt_zhal_crm_6m
         ,s8.appeal_cnt_zhal_crm_12m
         ,s8.appeal_share_zhal_to_all
         ,s8.appeal_share_zhal_to_all_3m
         ,s8.appeal_share_zhal_to_all_6m
         ,s8.appeal_share_zhal_to_all_12m
         ,s8.appeal_share_crm_to_zhal
         ,s8.appeal_share_crm_to_zhal_3m
         ,s8.appeal_share_crm_to_zhal_6m
         ,s8.appeal_share_crm_to_zhal_12m 


    ----------------------------------------------

         ,s9.DEP_CNT_M_FIRST_OPEN
         ,s9.DEP_CNT_M_LAST_OPEN
         ,s9.DEP_CNT_M_LAST_CLOSE
         ,s9.DEP_CNT_M_NEXT_CLOSE
         ,s9.DEP_CNT_M_LAST_PROLONG
         ,s9.DEP_CNT
         ,s9.DEP_CNT_OPEN
         ,s9.DEP_CNT_CLOSE
         ,s9.DEP_CNT_OPEN_TG
         ,s9.DEP_CNT_FOREIGN_CURR
         ,s9.DEP_CNT_OPEN_USD
         ,s9.DEP_CNT_OPEN_EUR
         ,s9.DEP_CNT_OPEN_OTHER_CUR
         ,s9.DEP_CNT_WITH_CAP
         ,s9.DEP_CNT_OPEN_WITH_CAP
         ,s9.DEP_CNT_CLOSE_WITH_CAP
         ,s9.DEP_AMT
         ,s9.DEP_AMT_OPEN
         ,s9.DEP_AMT_CLOSE
         ,s9.DEP_AMT_MAX
         ,s9.DEP_AMT_MIN
         ,s9.DEP_AMT_OPEN_MAX
         ,s9.DEP_AMT_OPEN_MIN
         ,s9.DEP_AMT_CLOSE_MAX
         ,s9.DEP_AMT_CLOSE_MIN
         ,s9.DEP_AVG_IR
         ,s9.DEP_AVG_SUM_3M
         ,s9.DEP_AVG_SUM_6M
         ,s9.DEP_AVG_SUM_12M
         ,s9.DEP_AVG_SUM_24M
         ,s9.DEP_AVG_SUM_36M


    ----------------------------------------------

         ,s10.Web_Cnt_m_Last_Mb
         ,s10.Web_Cnt_m_Last_Lh
         ,s10.Web_Cnt_m_Last_Ib
         ,s10.web_avg_open_mb
         ,s10.web_avg_open_lh
         ,s10.web_avg_open_ib
         ,s10.Web_Cnt_m_Last_Res_Mb
         ,s10.Web_Cnt_m_Last_Res_Lh
         ,s10.Web_Avg_Open_Res_Mb
         ,s10.Web_Avg_Open_Res_Lh


    ----------------------------------------------
     
         ,s11.PMT_CNT_TOTAL_1M
         ,s11.PMT_CNT_TOTAL_3M
         ,s11.PMT_CNT_TOTAL_6M
         ,s11.PMT_CNT_TOTAL_12M
         ,s11.PMT_NAME_CHANNEL_LAST_1
         ,s11.PMT_NAME_CHANNEL_LAST_2
         ,s11.PMT_NAME_CHANNEL_LAST_3
         ,s11.PMT_AVG_FEE_3M
         ,s11.PMT_AVG_FEE_6M
         ,s11.PMT_AVG_FEE_12M
         ,s11.PMT_MODE_FEE_3M
         ,s11.PMT_MODE_FEE_6M
         ,s11.PMT_MODE_FEE_12M
         ,s11.PMT_AVG_DAYS_3M
         ,s11.PMT_AVG_DAYS_6M
         ,s11.PMT_AVG_DAYS_12M
         ,s11.PMT_MED_DAYS_3M
         ,s11.PMT_MED_DAYS_6M
         ,s11.PMT_MED_DAYS_12M
         ,s11.PMT_MODE_DAYS_3M
         ,s11.PMT_MODE_DAYS_6M
         ,s11.PMT_MODE_DAYS_12M
         ,s11.PMT_CNT_BANK_1M
         ,s11.PMT_CNT_BANK_3M
         ,s11.PMT_CNT_BANK_6M
         ,s11.PMT_CNT_BANK_12M
         ,s11.PMT_CNT_TER_1M
         ,s11.PMT_CNT_TER_3M
         ,s11.PMT_CNT_TER_6M
         ,s11.PMT_CNT_TER_12M
         ,s11.PMT_CNT_CASH_BOX_1M
         ,s11.PMT_CNT_CASH_BOX_3M
         ,s11.PMT_CNT_CASH_BOX_6M
         ,s11.PMT_CNT_CASH_BOX_12M
         ,s11.PMT_CNT_IB_1M
         ,s11.PMT_CNT_IB_3M
         ,s11.PMT_CNT_IB_6M
         ,s11.PMT_CNT_IB_12M
         ,s11.PMT_CNT_Kazpost_1M
         ,s11.PMT_CNT_Kazpost_3M
         ,s11.PMT_CNT_Kazpost_6M
         ,s11.PMT_CNT_Kazpost_12M
         ,s11.PMT_CNT_Halyk_1M
         ,s11.PMT_CNT_Halyk_3M
         ,s11.PMT_CNT_Halyk_6M
         ,s11.PMT_CNT_Halyk_12M
         ,s11.PMT_CNT_OTHER_1M
         ,s11.PMT_CNT_OTHER_3M
         ,s11.PMT_CNT_OTHER_6M
         ,s11.PMT_CNT_OTHER_12M
         ,s11.PMT_MAX_BANK_12M
         ,s11.PMT_MIN_BANK_12M
         ,s11.PMT_AVG_BANK_12M
         ,s11.PMT_MED_BANK_12M
         ,s11.PMT_SHARE_BANK_12M
         ,s11.PMT_SHARE_TER_12M
         ,s11.PMT_SHARE_CASH_BOX_12M
         ,s11.PMT_SHARE_IB_12M
         ,s11.PMT_SHARE_Kazpost_12M
         ,s11.PMT_SHARE_Halyk_12M
         ,s11.PMT_SHARE_OTHER_12M
         ,s11.PMT_CNT_DIFF_CHANNELS_1M
         ,s11.PMT_CNT_DIFF_CHANNELS_3M
         ,s11.PMT_CNT_DIFF_CHANNELS_6M
         ,s11.PMT_CNT_DIFF_CHANNELS_12M

         ,s11.PMT_LP_CNT_TOTAL_1M
         ,s11.PMT_LP_CNT_TOTAL_3M
         ,s11.PMT_LP_CNT_TOTAL_6M
         ,s11.PMT_LP_CNT_TOTAL_12M
         ,s11.PMT_LP_AVG_FEE_3M
         ,s11.PMT_LP_AVG_FEE_6M
         ,s11.PMT_LP_AVG_FEE_12M
         ,s11.PMT_LP_MODE_FEE_3M
         ,s11.PMT_LP_MODE_FEE_6M
         ,s11.PMT_LP_MODE_FEE_12M
         ,s11.PMT_LP_AVG_DAYS_3M
         ,s11.PMT_LP_AVG_DAYS_6M
         ,s11.PMT_LP_AVG_DAYS_12M
         ,s11.PMT_LP_MED_DAYS_3M
         ,s11.PMT_LP_MED_DAYS_6M
         ,s11.PMT_LP_MED_DAYS_12M
         ,s11.PMT_LP_MODE_DAYS_3M
         ,s11.PMT_LP_MODE_DAYS_6M
         ,s11.PMT_LP_MODE_DAYS_12M
         ,s11.PMT_LP_CNT_BANK_1M
         ,s11.PMT_LP_CNT_BANK_3M
         ,s11.PMT_LP_CNT_BANK_6M
         ,s11.PMT_LP_CNT_BANK_12M
         ,s11.PMT_LP_CNT_TER_1M
         ,s11.PMT_LP_CNT_TER_3M
         ,s11.PMT_LP_CNT_TER_6M
         ,s11.PMT_LP_CNT_TER_12M
         ,s11.PMT_LP_CNT_CASH_BOX_1M
         ,s11.PMT_LP_CNT_CASH_BOX_3M
         ,s11.PMT_LP_CNT_CASH_BOX_6M
         ,s11.PMT_LP_CNT_CASH_BOX_12M
         ,s11.PMT_LP_CNT_IB_1M
         ,s11.PMT_LP_CNT_IB_3M
         ,s11.PMT_LP_CNT_IB_6M
         ,s11.PMT_LP_CNT_IB_12M
         ,s11.PMT_LP_CNT_Kazpost_1M
         ,s11.PMT_LP_CNT_Kazpost_3M
         ,s11.PMT_LP_CNT_Kazpost_6M
         ,s11.PMT_LP_CNT_Kazpost_12M
         ,s11.PMT_LP_CNT_Halyk_1M
         ,s11.PMT_LP_CNT_Halyk_3M
         ,s11.PMT_LP_CNT_Halyk_6M
         ,s11.PMT_LP_CNT_Halyk_12M
         ,s11.PMT_LP_CNT_OTHER_1M
         ,s11.PMT_LP_CNT_OTHER_3M
         ,s11.PMT_LP_CNT_OTHER_6M
         ,s11.PMT_LP_CNT_OTHER_12M
         ,s11.PMT_LP_MAX_BANK_12M
         ,s11.PMT_LP_MIN_BANK_12M
         ,s11.PMT_LP_AVG_BANK_12M
         ,s11.PMT_LP_MED_BANK_12M
         ,s11.PMT_LP_SHARE_BANK_12M
         ,s11.PMT_LP_SHARE_TER_12M
         ,s11.PMT_LP_SHARE_CASH_BOX_12M
         ,s11.PMT_LP_SHARE_IB_12M
         ,s11.PMT_LP_SHARE_Kazpost_12M
         ,s11.PMT_LP_SHARE_Halyk_12M
         ,s11.PMT_LP_SHARE_OTHER_12M
         ,s11.PMT_LP_CNT_DIFF_CHANNELS_1M
         ,s11.PMT_LP_CNT_DIFF_CHANNELS_3M
         ,s11.PMT_LP_CNT_DIFF_CHANNELS_6M
         ,s11.PMT_LP_CNT_DIFF_CHANNELS_12M


    ----------------------------------------------
     
         ,s12.Fcb_Cnt_App_1m
         ,s12.Fcb_Cnt_App_3m
         ,s12.Fcb_Cnt_App_6m
         ,s12.Fcb_Cnt_App_12m

         ,s12.Fcb_Cnt_m_First_Open
         ,s12.Fcb_Cnt_m_Last_Open
         ,s12.Fcb_Cnt_m_Last_Close

         ,s12.Fcb_Avg_Term_Plan
         ,s12.Fcb_Avg_Term_Fact
         ,s12.Fcb_Min_Amt_Credit
         ,s12.Fcb_Max_Amt_Credit
         ,s12.Fcb_Avg_Amt_Credit

         ,s12.Fcb_Sum_Amt_Credit_w_Mor
         ,s12.Fcb_Sum_Amt_Credit_w_o_Mor
         ,s12.Fcb_Sum_Annuity_w_Mor
         ,s12.Fcb_Sum_Annuity_w_o_Mor
         ,s12.Fcb_Cnt_Overdue
         ,s12.Fcb_Max_Overdue

         ,s12.Fcb_Cnt_Early_Close
         ,s12.Fcb_Cnt_Contract
         ,s12.Fcb_Cnt_Contract_Loan
         ,s12.Fcb_Cnt_Contract_Card
         ,s12.Fcb_Cnt_Contract_Car
         ,s12.Fcb_Cnt_Contract_Mor
         ,s12.Fcb_Cnt_Contract_Biz
         ,s12.Fcb_Cnt_Contract_Act
         ,s12.Fcb_Cnt_Contract_Loan_Act
         ,s12.Fcb_Cnt_Contract_Card_Act
         ,s12.Fcb_Cnt_Contract_Car_Act
         ,s12.Fcb_Cnt_Contract_Mor_Act
         ,s12.Fcb_Cnt_Contract_Biz_Act
         
    from      T_ABT_PART0_CLIENT         s0
    left join T_ABT_PART1_SOC_DEM        s1  on s0.skp_client = s1.skp_client  -- 12
    left join T_ABT_PART2_OFFER          s2  on s0.skp_client = s2.skp_client  -- 17
    left join T_ABT_PART3_APPLICATION    s3  on s0.skp_client = s3.skp_client  -- 64
    left join T_ABT_PART4_LAST_APPL      s4  on s0.skp_client = s4.skp_client  -- 25
    left join T_ABT_PART5_LAST_CONTRACT  s5  on s0.skp_client = s5.skp_client  -- 26
    left join T_ABT_PART6_CONTRACTS      s6  on s0.skp_client = s6.skp_client  -- 131 (136)
    left join T_ABT_PART7_COMM           s7  on s0.skp_client = s7.skp_client  -- 100 (105)
    left join T_ABT_PART8_APPEAL         s8  on s0.skp_client = s8.skp_client  -- 20
    left join T_ABT_PART9_DEP            s9  on s0.skp_client = s9.skp_client  -- 31
    left join T_ABT_PART10_WEB           s10 on s0.skp_client = s10.skp_client -- 10
    left join T_ABT_PART11_PAYMENTS      s11 on s0.skp_client = s11.skp_client -- 131 
    left join T_ABT_PART12_FCB           s12 on s0.skp_client = s12.skp_client -- 31
    
    /*LEFT JOIN T_ABT_CASH_DATAMART        DM  ON S0.SKP_CLIENT = DM.SKP_CLIENT
                                            AND S0.MONTH_     = DM.MONTH_
   WHERE DM.SKP_CLIENT IS NULL*/
     ;
  
    PKG_MZ_HINTS.pStepEnd(anRowsResult => SQL%ROWCOUNT,
                          acTable      => 'T_ABT_CASH_DATAMART',
                          calcStats    => 0);
    PKG_MZ_HINTS.pStatsPartTab(acOwner => USER, acTable => 'T_ABT_CASH_DATAMART', anCntPartLast => 1);     
    
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
    DBMS_OUTPUT.DISABLE;
    PKG_MZ_HINTS.pAlterSession(8);
       
     ---- Call Procedures ----
     P_ABT_Data_Preparation      (DATE_CALC);
     P_ABT_PART_1_Soc_Dem        (DATE_CALC);
     P_ABT_PART_2_Offer          (DATE_CALC);
     P_ABT_PART_3_Application    (DATE_CALC);
     P_ABT_PART_4_Last_Appl      (DATE_CALC);
     P_ABT_PART_5_Last_Contr     (DATE_CALC);
     P_ABT_PART_6_Contracts      (DATE_CALC);
     P_ABT_PART_7_Comm           (DATE_CALC);
     P_ABT_PART_8_Appeal         (DATE_CALC);
     P_ABT_PART_9_Deposit        (DATE_CALC);
     P_ABT_PART_10_Mapp          (DATE_CALC);
     P_ABT_PART_11_Payments      (DATE_CALC);
     P_ABT_PART_12_FCB           (DATE_CALC);
     P_ABT_CASH_DATAMART         (DATE_CALC);
     --------------------------
        
     ---- For Report to Email -----------------
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_DATA_PREPARATION';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_1_SOC_DEM';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_2_OFFER';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_3_APPLICATION';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_4_LAST_APPL';        
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_5_LAST_CONTR';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_6_CONTRACTS';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_7_COMM';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_8_APPEAL';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_9_DEPOSIT';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_10_MAPP';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_11_PAYMENTS';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_PART_12_FCB';
      I := I + 1;
      P_SUBJECTS(I) := 'P_ABT_CASH_DATAMART';
     --------------------------------------------
     
     
     
     PKG_MZ_HINTS.pMail(P_SUBJECTS, 
                       'PKG_ABT_DATAMART', 
                       1);        

   END;

END;
/
