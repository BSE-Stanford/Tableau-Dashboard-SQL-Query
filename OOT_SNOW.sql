SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time asc
    )                       assign_gp_order,
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                                         current_state,
    MIN(start_time)
    OVER(PARTITION BY r.number, r.definition) sr_start_time,
    r.definition,
    r.number,
    r.start_time            assigned_group_start_time,
    r.end_time              assigned_group_end_time,
    r.sys_created_by        assigned_group_by,
    r.sys_created_by_name   assigned_to_group_by_name,
    r.value                 assigned_group,
    r.reopened_counter,
    r.business_unit,
    m.requested_by,
    m.function_or_service,
    --m.sr_created_on_date,
    coalesce(
        m.sys_created_on, m.sr_created_on_date
    )                       sr_created_on_date,
    m.sys_updated_on        latest_updated_on,
    m.state,
    m.priority,
    m.item                  form,
    m.business_service,
    m.classification_code,
    m.short_description,
    m.resolved_by_name,
    coalesce(
        m.u_resolved_at, m.closed_at
    )                       u_resolved_at,
    m.assigned_to           assigned_to_main,
    m.assigned_to_user_name assigned_to_user_name_main,
    m.assignment_group      current_assignment_group,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    m.requested_by_cost_center,
    m.requested_by_department,
    m.requested_by_user_name,
    m.requested_for,
    m.requested_for_cost_center,
    m.requested_for_department,
    m.survey_comments,
    m.survey_response,
    m.urgency,
    current_date as last_refreshed_date
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assignment_group'
    r.definition = 'SU Support Request Assignment Group'
    AND ( m.assignment_group in ('FMS Office of the Treasurer', 'FMS Merchant Services')
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value in ('FMS Office of the Treasurer', 'FMS Merchant Services')
            AND o_r.end_time IS NOT NULL
    ) )
    AND m.sr_created_on_date >= ( current_date - 790 )