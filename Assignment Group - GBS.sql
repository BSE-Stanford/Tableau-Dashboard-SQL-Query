SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                       assign_gp_order,
    r.definition,
    r.number,
    r.start_time            assigned_group_start_time,
    r.end_time              assigned_group_end_time,
    r.sys_created_by        assigned_group_by,
    r.sys_created_by_name   assigned_to_group_by_name,
    r.value                 assigned_group,
    r.reopened_counter,
    'N/A'                   parent_req_number,
    r.business_unit,
    m.requested_by,
    m.function_or_service,
    m.sr_created_on_date,
    m.sys_updated_on        latest_updated_on,
    m.state,
    m.priority,
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
    m.item
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assignment_group'
    r.definition = 'SU Support Request Assignment Group'
	and  m.item IN ( 'Consult with Stanford Global Business Services', 'Get Section 117 Reporting Assistance', 'Request a Faculty Invitation Letter (Barile Law)',
            'Get Global Consolidation System (GCS) Assistance' )
    /*AND ( m.assignment_group = 'FMS Evolve Financial Reporting (EFR) Support (Secure)'
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value = 'FMS Evolve Financial Reporting (EFR) Support (Secure)'
            AND o_r.end_time IS NOT NULL
    ) )*/
	AND m.sys_updated_on  >= ( current_date - 2000 )
UNION
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                       assign_gp_order,
    r.definition,
    r.number,
    r.start_time            assigned_group_start_time,
    r.end_time              assigned_group_end_time,
    r.sys_created_by        assigned_group_by,
    r.sys_created_by_name   assigned_to_group_by_name,
    r.value                 assigned_group,
    r.reopened_counter,
    t.parent                parent_req_number,
    r.business_unit,
    m.requested_by,
    m.function_or_service,
    m.sr_created_on_date,
    m.sys_updated_on        latest_updated_on,
    t.state,
    t.priority,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                               resolved_by_name,
    t.closed_at                               u_resolved_at,
    t.assigned_to           assigned_to_main,
    t.assigned_to_user_name assigned_to_user_name_main,
    t.assignment_group      current_assignment_group,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    m.requested_by_cost_center,
    t.requested_by_department,
     t.requested_by_sys_created_by             requested_by_user_name,
    concat(
        t.requested_for_first_name, ' ', t.requested_for_last_name
    )                                         requested_for,
    t.requested_for_cost_center,
    t.requested_for_department,
    m.survey_comments,
    m.survey_response,
    m.urgency,
    m.item
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assignment Group'
    --AND t.assignment_group = 'FMS Evolve Financial Reporting (EFR) Support (Secure)'
	AND m.item IN ( 'Consult with Stanford Global Business Services', 'Get Section 117 Reporting Assistance', 'Request a Faculty Invitation Letter (Barile Law)',
            'Get Global Consolidation System (GCS) Assistance' )
		   AND m.sys_updated_on  >= ( current_date - 2000 )