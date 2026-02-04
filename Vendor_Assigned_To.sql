SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                             current_assigned,
    r.definition,
    r.number,
    r.start_time                  assigned_to_start_time,
    r.end_time                    assigned_to_end_time,
    r.sys_created_by              assigned_to_by,
    r.sys_created_by_name         assigned_to_by_username,
    r.value                       assigned_to,
    r.reopened_counter,
    t.parent                      parent_req_number,
    r.business_unit,
    --m.sr_created_on_date,
    t.sys_created_on              sr_created_on_date,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                   resolved_by_name,
    t.closed_at                   u_resolved_at,
    t.assigned_to                 assigned_to_main,
    t.assigned_to_user_name       assigned_to_user_name_main,
    t.assignment_group            current_assignment_group,
    t.state                       current_state,
    t.priority,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    m.requested_by_cost_center,
    t.requested_by_department,
    t.requested_by_sys_created_by requested_by_user_name,
    concat(
        t.requested_for_first_name, ' ', t.requested_for_last_name
    )                             requested_for,
    t.requested_for_cost_center,
    t.requested_for_department,
    m.survey_comments,
    m.survey_response,
    m.urgency,
    'SR Task Assigned To Team'    AS type,
    'Vendor Services'             AS procurement
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assigned To'
    AND t.assignment_group = 'FMS Vendor Services'
    --AND m.sr_created_on_date >= ( current_date - 3000 )
	AND extract(year from t.sys_updated_on ) >= (extract( year from current_date )-5)

UNION ALL
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                       current_assigned,
    r.definition,
    r.number,
    r.start_time            assigned_to_start_time,
    r.end_time              assigned_to_end_time,
    r.sys_created_by        assigned_to_by,
    r.sys_created_by_name   assigned_to_by_username,
    r.value                 assigned_to,
    r.reopened_counter,
    'N/A'                   parent_req_number,
    r.business_unit,
    m.sys_created_on        sr_created_on_date,
    m.function_or_service,
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
    m.state                 current_state,
    m.priority,
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
    'SR Assigned To Team'   AS type,
    'Vendor Services'       AS procurement
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Request Assigned To'
    AND ( m.assignment_group = 'FMS Vendor Services'
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value = 'FMS Vendor Services'
            AND o_r.end_time IS NOT NULL
    ) )
    --AND m.sr_created_on_date >= ( current_date - 3000 )
	AND extract(year from m.sys_updated_on ) >= (extract( year from current_date )-5)

UNION ALL
SELECT
    1                            current_assigned,
    'Task Assigned'              definition,
    r.number,
    coalesce(
        r.work_start_timestamp, opened_at
    )                            assigned_to_start_time,
    r.work_end_timestamp         assigned_to_end_time,
    r.sys_created_by             assigned_to_by,
    r.sys_created_by_name        assigned_to_by_username,
    r.assigned_to                assigned_to,
    0                            reopened_counter,
    r.parent                     parent_req_number,
    r.assigned_to_business_unit  business_unit,
    r.opened_at                  sr_created_on_date,
    'Reporting'                  function_or_service,
    'EFR Dashboard Reports RITM' business_service,
    'RITM'                       classification_code,
    r.short_description,
    r.assigned_to                resolved_by_name,
    r.work_end_timestamp         u_resolved_at,
    r.assigned_to                assigned_to_main,
    r.assigned_to_user_name      assigned_to_user_name_main,
    r.assignment_group           current_assignment_group,
    r.state                      current_state,
    r.priority,
    r.opened_by_cost_center,
    r.opened_by_department,
    r.opened_by                  opened_by_email,
    r.opened_by                  opened_by_first_name,
    r.opened_by                  opened_by_last_name,
    r.requested_by_cost_center,
    r.requested_by_department,
    r.requested_for              requested_by_user_name,
    r.requested_for,
    r.requested_for_cost_center,
    r.requested_for_department,
    'N/A'                        survey_comments,
    'N/A'                        survey_response,
    r.urgency,
    'Task Assigned To Team'      AS type,
    'Vendor Services'            AS procurement
FROM
    task_bs.task r
WHERE
    assignment_group = 'FMS Vendor Services'
--and parent is not null
    AND number LIKE 'TASK%'
    --AND r.opened_at >= ( current_date - 3000 )
	AND extract(year from R.sys_updated_on ) >= (extract( year from current_date )-5)

UNION ALL
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                             current_assigned,
    r.definition,
    r.number,
    r.start_time                  assigned_to_start_time,
    r.end_time                    assigned_to_end_time,
    r.sys_created_by              assigned_to_by,
    r.sys_created_by_name         assigned_to_by_username,
    r.value                       assigned_to,
    r.reopened_counter,
    t.parent                      parent_req_number,
    r.business_unit,
    --m.sr_created_on_date,
    t.sys_created_on              sr_created_on_date,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                   resolved_by_name,
    t.closed_at                   u_resolved_at,
    t.assigned_to                 assigned_to_main,
    t.assigned_to_user_name       assigned_to_user_name_main,
    t.assignment_group            current_assignment_group,
    t.state                       current_state,
    t.priority,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    m.requested_by_cost_center,
    t.requested_by_department,
    t.requested_by_sys_created_by requested_by_user_name,
    concat(
        t.requested_for_first_name, ' ', t.requested_for_last_name
    )                             requested_for,
    t.requested_for_cost_center,
    t.requested_for_department,
    m.survey_comments,
    m.survey_response,
    m.urgency,
    'SR Task Assigned To Team'    AS type,
    'Supplier Enablement'         AS procurement
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assigned To'
    AND t.assignment_group = 'FMS Supplier Enablement'
    --AND m.sr_created_on_date >= ( current_date - 3000 )
	AND extract(year from t.sys_updated_on ) >= (extract( year from current_date )-5)

UNION ALL
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                       current_assigned,
    r.definition,
    r.number,
    r.start_time            assigned_to_start_time,
    r.end_time              assigned_to_end_time,
    r.sys_created_by        assigned_to_by,
    r.sys_created_by_name   assigned_to_by_username,
    r.value                 assigned_to,
    r.reopened_counter,
    'N/A'                   parent_req_number,
    r.business_unit,
    m.sys_created_on        sr_created_on_date,
    m.function_or_service,
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
    m.state                 current_state,
    m.priority,
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
    'SR Assigned To Team'   AS type,
    'Supplier Enablement'   AS procurement
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Request Assigned To'
    AND ( m.assignment_group = 'FMS Supplier Enablement'
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value = 'FMS Supplier Enablement'
            AND o_r.end_time IS NOT NULL
    ) )
    --AND m.sr_created_on_date >= ( current_date - 3000 )
	AND extract(year from m.sys_updated_on ) >= (extract( year from current_date )-5)

UNION ALL
SELECT
    1                            current_assigned,
    'Task Assigned'              definition,
    r.number,
    coalesce(
        r.work_start_timestamp, opened_at
    )                            assigned_to_start_time,
    r.work_end_timestamp         assigned_to_end_time,
    r.sys_created_by             assigned_to_by,
    r.sys_created_by_name        assigned_to_by_username,
    r.assigned_to                assigned_to,
    0                            reopened_counter,
    r.parent                     parent_req_number,
    r.assigned_to_business_unit  business_unit,
    r.opened_at                  sr_created_on_date,
    'Reporting'                  function_or_service,
    'EFR Dashboard Reports RITM' business_service,
    'RITM'                       classification_code,
    r.short_description,
    r.assigned_to                resolved_by_name,
    r.work_end_timestamp         u_resolved_at,
    r.assigned_to                assigned_to_main,
    r.assigned_to_user_name      assigned_to_user_name_main,
    r.assignment_group           current_assignment_group,
    r.state                      current_state,
    r.priority,
    r.opened_by_cost_center,
    r.opened_by_department,
    r.opened_by                  opened_by_email,
    r.opened_by                  opened_by_first_name,
    r.opened_by                  opened_by_last_name,
    r.requested_by_cost_center,
    r.requested_by_department,
    r.requested_for              requested_by_user_name,
    r.requested_for,
    r.requested_for_cost_center,
    r.requested_for_department,
    'N/A'                        survey_comments,
    'N/A'                        survey_response,
    r.urgency,
    'Task Assigned To Team'      AS type,
    'Supplier Enablement'        AS procurement
FROM
    task_bs.task r
WHERE
    assignment_group = 'FMS Supplier Enablement'
--and parent is not null
    AND number LIKE 'TASK%'
    --AND r.opened_at >= ( current_date - 3000 )
    AND extract(year from R.sys_updated_on ) >= (extract( year from current_date )-5)