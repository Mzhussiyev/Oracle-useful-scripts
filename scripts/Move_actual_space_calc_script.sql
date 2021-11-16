With tab As
 (Select dt.OWNER
        ,DT.TABLE_NAME
        ,round(((blocks * 16 / 1024)), 2) "TOT_SZ_MB"
         ,round((num_rows * avg_row_len / 1024 / 1024), 2) "ACT_SZ_MB"
         ,round(((blocks * 16 / 1024) -
               (num_rows * avg_row_len / 1024 / 1024)),
               2) "FRAG_SPACE_MB"
         ,(round(((blocks * 16 / 1024) -
                (num_rows * avg_row_len / 1024 / 1024)),
                2) / round(((blocks * 16 / 1024)), 2)) * 100 "FRAG_PERC"/*,
                dt.**/
    From dba_tables dt
   Where blocks <> 0
     --and table_name LIKE 'T_MZ%'
     AND TABLE_NAME = 'T_MZ_INCALL_SMS3'
     And dt.owner Like '&SCM'),
segm As
 (Select owner
        ,segment_name table_name
        ,round(Sum(bytes) / 1024 / 1024, 2) "SEGM_SIZE_MB"
    From dba_segments ds
   Where owner Like '&SCM'
   Group By owner
           ,segment_name)
Select *
  From tab
  Join segm
 Using (OWNER, table_name)
