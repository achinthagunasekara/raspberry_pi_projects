SELECT * FROM (
    SELECT region, domain, count_yesterday, count_a_week_ago, count_diff, increased_percentage, Rank()
    OVER (Partition BY region ORDER BY increased_percentage DESC ) AS rank
    FROM (
        SELECT * FROM (
            SELECT yesterday.region,
                yesterday.domains AS domain,
                yesterday.count AS count_yesterday,
                a_week_ago.count AS count_a_week_ago,
                (yesterday.count - a_week_ago.count) AS count_diff,
                (((yesterday.count - a_week_ago.count)*100)/yesterday.count) AS increased_percentage
            FROM (
                SELECT regexp_extract(message, '^([a-z]+-[a-z]+-[1-9])', 1) AS region, regexp_extract(message, 'CONNECT\s([\w-.:]+)\s', 1) AS domains, count(*) AS count
                FROM stage.murron_logs
                WHERE topic = 'whitecastle-squid'
                AND message like '%CONNECT%'
                AND ds = {{a_week_ago}}
                GROUP BY 1,2
                ORDER BY 2 DESC
            ) as a_week_ago
            LEFT JOIN (
                SELECT regexp_extract(message, '^([a-z]+-[a-z]+-[1-9])', 1) AS region, regexp_extract(message, 'CONNECT\s([\w-.:]+)\s', 1) AS domains, count(*) AS count
                FROM stage.murron_logs
                WHERE topic = 'whitecastle-squid'
                AND message like '%CONNECT%'
                AND ds = {{yesterday}}
                GROUP BY 1,2
                ORDER BY 2 DESC
            ) AS yesterday ON (a_week_ago.domains = yesterday.domains)
        )
        WHERE domain IS NOT NULL
        AND count_diff > 0
        /*AND increased_percentage > 5
        AND count_diff > 10000
        ORDER BY count_diff DESC
        */
    )
)
WHERE rank = 1
