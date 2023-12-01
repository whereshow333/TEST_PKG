SELECT acct_nbr_ori, acct_nbr, act_date, tx_date, tx_time, drcr, cust_id, own_trans_acct, tx_amt, pb_bal, tx_brh, cur, channel_desc, chal_1, memo, remk, jrnl_no, tx_mode
FROM SAV_TXN_VIEW.SAV_TXN                                     --請修改活存交易檔[VIEW].[TABLE NAME] (VIEW.活存交易檔名ex.WMG_SAV_TXN)
WHERE acct_nbr_ori not in (
        /*排除所有警示戶、薪轉戶、證券戶*/
        SELECT distinct acct_nbr_ori AS acct_nbr_ori
        FROM (
            /*已為警示戶*/
            SELECT acct_no_sav AS acct_nbr_ori
            FROM HIVE_VIEW.BANCS_CUST_ACCT_EXP       --請改修改核心客戶檔存放位置名稱[usr_XXX_XXX].[TABLE NAME] (VIEW.核心客戶檔名ex.BANCS_CUST_ACCT_EXP)
            WHERE alert_flg = 'Y'
            UNION ALL
            /*排除薪轉*/
            SELECT distinct acct_no_14 AS acct_nbr_ori
            FROM HIVE_VIEW.POLICE_DASB_job_salary_acct
            UNION ALL
            /*排除證券*/
            SELECT distinct dep_acct_no AS acct_nbr_ori
            FROM HIVE_VIEW.POLICE_DASB_ashf_list
        ) T
)
AND cust_id not in (
    /*排除會計師醫師律師*/
    SELECT DISTINCT CUST_ID AS cust_id
    FROM HIVE_VIEW.WMG_CUST
    WHERE (OCCUPATION IN ('20', '25', '26') OR AGE <= 10)  --20:專業服務業(醫師、會計師、律師)、25:記帳士、26:記帳及報稅代理人
    AND LENGTH(CUST_ID) = 10
    AND LEFT(CUST_ID, 1) rlike '[a-zA-Z]'
)
AND length(cust_id) = 10
AND LEFT(CUST_ID, 1) rlike '[a-zA-Z]'
AND TX_MODE = '1'
AND (substr(emp_no, 4, 4) in ('4001','4002','4005','4011','4012','4013','4014','4021','4022','4111','4112','4115','4215')
OR substr(emp_no, 4, 1) = '3')
AND tx_date BETWEEN ADD_MONTHS('SNAP_DATE', -1) AND ('SNAP_DATE')
ORDER BY acct_nbr_ori ASC, tx_date ASC, tx_time ASC