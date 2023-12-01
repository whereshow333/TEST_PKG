SELECT company_uid, access_date, SUM(CASE WHEN category = 'MB_limit' THEN times END) MB_limit, SUM(CASE WHEN category = 'MB_check' THEN times END) MB_check, SUM(CASE WHEN category = 'EB_check' THEN times END) EB_check
FROM(
    SELECT company_uid, access_date, category, COUNT(DISTINCT login_log_key) as times
    FROM (
        SELECT 
            company_uid,
            login_log_key,
            to_date(access_time) AS access_date,
            error_desc,
            menu_id, 
            CASE
                WHEN menu_id IN ('MDS01', 'MDS0101', 'MDS04', 'MDS0401', 'MDS0402') THEN 'MB_check' 
                WHEN menu_id IN ('MPS1302') THEN 'MB_limit' 
            END AS category
        FROM MB_LOG_VIEW.MB_ACCESS_LOG
        WHERE company_uid IN (
            SELECT distinct cust_id 
            FROM HIVE_VIEW.job_cust_id_pred
        )
        AND menu_id in ('MDS01', 'MDS0101', 'MDS04', 'MDS0401', 'MDS0402','MPS1302')
        AND from_unixtime(unix_timestamp(access_date), 'yyyy-MM-dd') BETWEEN (SELECT min(min_date) FROM HIVE_VIEW.job_cust_id_pred) 
        AND (SELECT max(max_date) min_date FROM HIVE_VIEW.job_cust_id_pred)
        UNION ALL
        SELECT 
            company_uid,
            login_log_key,
            to_date(access_time) AS access_date,
            error_desc,
            menu_id, 
            CASE 
                WHEN menu_id in ('CDS0401', 'CDS04', 'CDS01', 'CDS0102', 'CBO03', 'CDF02', 'CDF04') THEN 'EB_check' 
            END as category
        FROM B2C_LOG_VIEW.B2C_ACCESS_LOG
        WHERE company_uid IN (
            SELECT distinct cust_id 
            FROM HIVE_VIEW.job_cust_id_pred
        )
        AND menu_id in ('CDS0401', 'CDS04', 'CDS01', 'CDS0102', 'CBO03', 'CDF02', 'CDF04')
        AND from_unixtime(unix_timestamp(access_date), 'yyyy-MM-dd') BETWEEN (SELECT min(min_date) FROM HIVE_VIEW.job_cust_id_pred) 
        AND (SELECT max(max_date) min_date FROM HIVE_VIEW.job_cust_id_pred)
    ) t
    GROUP BY company_uid, access_date, category
) p
GROUP BY company_uid, access_date