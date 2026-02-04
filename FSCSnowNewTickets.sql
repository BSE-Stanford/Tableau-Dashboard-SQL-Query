SELECT
    /*ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    ) */ 1 AS                      assign_gp_order,
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
    m.sr_created_on_date as sr_created_on_date_org,
    coalesce(
        m.sys_created_on, m.sr_created_on_date
    )                       sr_created_on_date,
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
	m.opened_at,
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
    m.urgency

FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assignment_group'
    r.definition = 'SU Support Request Assignment Group'
    AND ( m.assignment_group in (  'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
    'FMS Financial Support Center (FSC)',  'FMS Financial Support Center (FSC) (Secure)')
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value in  ('FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
    'FMS Financial Support Center (FSC)',  'FMS Financial Support Center (FSC) (Secure)')
            AND o_r.end_time IS NOT NULL
    ) )
    --AND m.sr_created_on_date >= ( current_date - 3 )
	AND extract(year from m.sys_updated_on ) >= (extract( year from current_date )-3)