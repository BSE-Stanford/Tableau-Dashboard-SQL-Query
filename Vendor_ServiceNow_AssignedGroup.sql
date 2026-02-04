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
    'N/A'                   parent_req_short_description,
    'N/A'                   parent_req_status,
    'N/A'                   parent_req_assign_group,
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
    'SR Assigned To Team'   AS type,
    'Vendor Services'       AS procurement,
    current_date
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assignment_group'
    r.definition = 'SU Support Request Assignment Group'
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
    AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
UNION ALL
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                             assign_gp_order,
    r.definition,
    r.number,
    r.start_time                  assigned_group_start_time,
    r.end_time                    assigned_group_end_time,
    r.sys_created_by              assigned_group_by,
    r.sys_created_by_name         assigned_to_group_by_name,
    r.value                       assigned_group,
    r.reopened_counter,
    t.parent                      parent_req_number,
    m.short_description           AS parent_req_short_description,
    m.state                       parent_req_status,
    m.assignment_group            parent_req_assign_group,
    r.business_unit,
    m.requested_by,
    m.function_or_service,
    coalesce(
        t.sys_created_on, m.sr_created_on_date
    )                             sr_created_on_date,
    m.sys_updated_on              latest_updated_on,
    t.state,
    t.priority,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                   resolved_by_name,
    t.closed_at                   u_resolved_at,
    t.assigned_to                 assigned_to_main,
    t.assigned_to_user_name       assigned_to_user_name_main,
    t.assignment_group            current_assignment_group,
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
    'Vendor Services'             AS procurement,
    current_date
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assignment Group'
    --AND t.assignment_group = 'FMS Vendor Services'
    AND ( t.assignment_group IN ( 'FMS Vendor Services' )
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_task o_r
        WHERE
            o_r.number = t.number
            AND o_r.definition = 'SU Support Req. Task Assignment Group'
            AND o_r.value IN ( 'FMS Vendor Services' )
            AND o_r.end_time IS NOT NULL
    ) )
    --AND m.sr_created_on_date >= ( current_date - 3000 )
    AND EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
UNION ALL
SELECT
    1                            assign_gp_order,
    'Task Assigned'              definition,
    r.number,
    coalesce(
        r.work_start_timestamp, r.opened_at
    )                            assigned_group_start_time,
    r.work_end_timestamp         assigned_group_end_time,
    r.sys_created_by             assigned_group_by,
    r.sys_created_by_name        assigned_to_group_by_name,
    r.assignment_group           assigned_group,
    0                            reopened_counter,
    r.parent                     parent_req_number,
    r.short_description          parent_req_short_description,
    r.state                      parent_req_status,
    r.requested_by_department    parent_req_assign_group,
    r.assigned_to_business_unit  business_unit,
    r.requested_for              requested_by,
    'Reporting'                  function_or_service,
    r.opened_at                  sr_created_on_date,
    r.sys_updated_on             latest_updated_on,
    r.state,
    r.priority,
    'EFR Dashboard Reports RITM' business_service,
    'RITM'                       classification_code,
    r.short_description,
    r.assigned_to                resolved_by_name,
    r.work_end_timestamp         u_resolved_at,
    r.assigned_to                assigned_to_main,
    r.assigned_to_user_name      assigned_to_user_name_main,
    r.assignment_group           current_assignment_group,
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
    'Task Assigned To Team',
    'Vendor Services'            AS procurement,
    current_date
FROM
    task_bs.task r
WHERE
    r.assignment_group = 'FMS Vendor Services'
    --AND r.opened_at >= ( current_date - 3000 )
    AND EXTRACT(YEAR FROM r.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
--and parent is not null
    AND number LIKE 'TASK%'
UNION ALL
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
    'N/A'                   parent_req_short_description,
    'N/A'                   parent_req_status,
    'N/A'                   parent_req_assign_group,
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
    'SR Assigned To Team'   AS type,
    'Supplier Enablement'   AS procurement,
    current_date
FROM
    support_requests.metric_support_request r
    RIGHT JOIN support_requests.support_request        m ON r.number = m.number
WHERE
    --r.field = 'assignment_group'
    r.definition = 'SU Support Request Assignment Group'
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
    AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
UNION ALL
SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                             assign_gp_order,
    r.definition,
    r.number,
    r.start_time                  assigned_group_start_time,
    r.end_time                    assigned_group_end_time,
    r.sys_created_by              assigned_group_by,
    r.sys_created_by_name         assigned_to_group_by_name,
    r.value                       assigned_group,
    r.reopened_counter,
    t.parent                      parent_req_number,
    m.short_description           AS parent_req_short_description,
    m.state                       parent_req_status,
    m.assignment_group            parent_req_assign_group,
    r.business_unit,
    m.requested_by,
    m.function_or_service,
    coalesce(
        t.sys_created_on, m.sr_created_on_date
    )                             sr_created_on_date,
    m.sys_updated_on              latest_updated_on,
    t.state,
    t.priority,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                   resolved_by_name,
    t.closed_at                   u_resolved_at,
    t.assigned_to                 assigned_to_main,
    t.assigned_to_user_name       assigned_to_user_name_main,
    t.assignment_group            current_assignment_group,
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
    'Supplier Enablement'         AS procurement,
    current_date
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assignment Group'
    --AND t.assignment_group = 'FMS Supplier Enablement'
    AND ( t.assignment_group IN ( 'FMS Supplier Enablement' )
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_task o_r
        WHERE
            o_r.number = t.number
            AND o_r.definition = 'SU Support Req. Task Assignment Group'
            AND o_r.value IN ( 'FMS Supplier Enablement' )
            AND o_r.end_time IS NOT NULL
    ) )
   -- AND m.sr_created_on_date >= ( current_date - 3000 )
    AND EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
UNION ALL
SELECT
    1                            assign_gp_order,
    'Task Assigned'              definition,
    r.number,
    coalesce(
        r.work_start_timestamp, r.opened_at
    )                            assigned_group_start_time,
    r.work_end_timestamp         assigned_group_end_time,
    r.sys_created_by             assigned_group_by,
    r.sys_created_by_name        assigned_to_group_by_name,
    r.assignment_group           assigned_group,
    0                            reopened_counter,
    r.parent                     parent_req_number,
    r.short_description          parent_req_short_description,
    r.state                      parent_req_status,
    r.requested_by_department    parent_req_assign_group,
    r.assigned_to_business_unit  business_unit,
    r.requested_for              requested_by,
    'Reporting'                  function_or_service,
    r.opened_at                  sr_created_on_date,
    r.sys_updated_on             latest_updated_on,
    r.state,
    r.priority,
    'EFR Dashboard Reports RITM' business_service,
    'RITM'                       classification_code,
    r.short_description,
    r.assigned_to                resolved_by_name,
    r.work_end_timestamp         u_resolved_at,
    r.assigned_to                assigned_to_main,
    r.assigned_to_user_name      assigned_to_user_name_main,
    r.assignment_group           current_assignment_group,
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
    'Task Assigned To Team',
    'Supplier Enablement'        AS procurement,
    current_date
FROM
    task_bs.task r
WHERE
    assignment_group = 'FMS Supplier Enablement'
    --AND r.opened_at >= ( current_date - 3000 )
    AND EXTRACT(YEAR FROM r.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )
--and parent is not null
    AND number LIKE 'TASK%'