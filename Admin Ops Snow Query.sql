SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.field
         ORDER BY
             r.start_time DESC
    )                                         assigned_to_order,
    MIN(start_time)
    OVER(PARTITION BY r.number, r.definition) First_Assigned_date,
    m.number,
    r.start_time                              assigned_to_start_time,
    r.end_time                                assigned_to_end_time,
    r.duration_in_days,
    r.duration_in_hours,
    r.duration_in_minutes,
    r.sys_created_by                          state_start_by,
    r.sys_created_by_name                     state_start_username,
    r.value                                   assigned_to_value,
    r.reopened_counter,
    m.sys_created_on                          sr_created_on_date,
    m.sys_updated_on,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    m.contact_type,
    m.item,
    m.short_description,
    m.resolved_by_name,
    coalesce(
        m.u_resolved_at, m.closed_at
    )                                         u_resolved_at,
    m.need_by,
    m.assigned_to,
    m.assigned_to_user_name,
    m.assignment_group                        current_assignment_group,
    m.state                                   current_request_state,
    m.priority,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    m.requested_by,
    m.requested_by_cost_center,
    m.requested_by_department,
    m.requested_by_user_name,
    m.requested_for,
    m.requested_for_cost_center,
    m.requested_for_department,
    m.requested_for_user_name,
    m.survey_comments,
    m.survey_response,
    current_date as last_refreshed_date
FROM
    support_requests.support_request        m
    LEFT JOIN support_requests.metric_support_request r ON r.number = m.number
WHERE
    r.definition = 'SU Support Request Assigned To'
    AND m.assignment_group = ( 'FMS Administrative Services' )
    AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )