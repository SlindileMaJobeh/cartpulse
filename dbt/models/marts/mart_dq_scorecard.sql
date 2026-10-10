select
    dataset,
    rule_name,
    dimension,
    count(distinct run_id)                                   as runs,
    sum(checked_rows)                                        as checked_rows,
    sum(failed_rows)                                         as failed_rows,
    round(1 - sum(failed_rows)::numeric / nullif(sum(checked_rows), 0), 4) as pass_rate,
    max(run_at)                                              as last_run_at
from {{ source('ops', 'dq_results') }}
where run_at >= now() - interval '7 days'
group by 1, 2, 3
