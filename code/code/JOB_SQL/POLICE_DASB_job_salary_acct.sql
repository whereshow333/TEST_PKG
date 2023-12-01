SELECT distinct(acct_nbr_ori) acct_no_14, 
flg_pr payroll_code
FROM (
    SELECT distinct A.acct_nbr_ori, A.cust_id, B.flg_pr
    FROM SAV_TXN_VIEW.SAV_TXN A
    INNER JOIN (
        SELECT cust_id, flg_pr      --薪轉註記
        FROM WMG_CUST_VIEW.WMG_CUST
        WHERE flg_pr = 'Y'
    ) B
    ON A.cust_id = B.cust_id
    WHERE flg_pr = 'Y'
) T1
