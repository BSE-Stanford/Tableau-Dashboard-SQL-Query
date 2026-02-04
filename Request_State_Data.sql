SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                                         current_state,
    MIN(start_time)
    OVER(PARTITION BY r.number, r.definition) sr_start_time,
    r.number,
    r.start_time                              state_start_time,
    r.end_time                                state_end_time,
    r.duration_in_days,
    r.duration_in_hours,
    r.duration_in_minutes,
    r.sys_created_by                          state_start_by,
    r.sys_created_by_name                     state_start_username,
    r.value                                   state,
    r.reopened_counter,
    t.parent                                  parent_req_number,
    m.sr_created_on_date,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    t.short_description,
    t.closed_by                               resolved_by_name,
    t.closed_at                               u_resolved_at,
    t.assigned_to,
    t.assigned_to_user_name,
    t.assignment_group                        current_assignment_group,
    t.state                                   main_state,
    'N/A'                                     task_created,
    t.priority,
    m.short_description                       AS parent_req_short_description,
    m.state                                   parent_req_status,
    m.opened_by_cost_center,
    m.opened_by_department,
    m.opened_by_email,
    m.opened_by_first_name,
    m.opened_by_last_name,
    t.requested_by_cost_center,
    t.requested_by_department,
    t.requested_by_sys_created_by             requested_by_user_name,
    concat(
        t.requested_for_first_name, ' ', t.requested_for_last_name
    )                                         requested_for,
    t.requested_for_cost_center,
    t.requested_for_department
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task State Change'
    AND t.assignment_group in 
('FMS Financial Support Center-LD (Secure)',
'FMS Systems and Reporting Operations (Secure)',
'FMS Financial Systems and Operations Support (Secure)',
'FMS PTA Manager',
'FMS iJournals/Feeder Request',
'FMS Chart of Accounts',
'FMS Financial Support Center-FFIT (Secure)')
 AND t.sys_updated_on  >= ( current_date - 2000 )
UNION
SELECT DISTINCT
    m.current_state,
    m.sr_start_time,
    m.number,
    m.state_start_time,
    m.state_end_time,
    m.duration_in_days,
    m.duration_in_hours,
    m.duration_in_minutes,
    m.state_start_by,
    m.state_start_username,
    m.state,
    m.reopened_counter,
    m.parent_req_number,
    m.sr_created_on_date,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    m.short_description,
    m.resolved_by_name,
    m.u_resolved_at,
    m.assigned_to,
    m.assigned_to_user_name,
    m.current_assignment_group,
    m.main_state,
    coalesce(
        task.parent, 'N/A'
    ) task_created,
    m.priority,
    m.parent_req_short_description,
    m.parent_req_status,
    m.opened_by_cost_center,
    m.opened_by_department,
    opened_by_email,
    opened_by_first_name,
    opened_by_last_name,
    m.requested_by_cost_center,
    m.requested_by_department,
    m.requested_by_user_name,
    m.requested_for,
    m.requested_for_cost_center,
    m.requested_for_department
FROM
    (
        SELECT
            ROW_NUMBER()
            OVER(PARTITION BY r.number, r.field
                 ORDER BY
                     r.start_time DESC
            )                                         current_state,
            MIN(start_time)
            OVER(PARTITION BY r.number, r.definition) sr_start_time,
            m.number,
            r.start_time                              state_start_time,
            r.end_time                                state_end_time,
            r.duration_in_days,
            r.duration_in_hours,
            r.duration_in_minutes,
            r.sys_created_by                          state_start_by,
            r.sys_created_by_name                     state_start_username,
            r.value                                   state,
            r.reopened_counter,
            'N/A'                                     parent_req_number,
            m.sr_created_on_date,
            m.function_or_service,
            m.business_service,
            m.classification_code,
            m.short_description,
            m.resolved_by_name,
            coalesce(
                m.u_resolved_at, m.closed_at
            )                                         u_resolved_at,
            m.assigned_to,
            m.assigned_to_user_name,
            m.assignment_group                        current_assignment_group,
            m.state                                   main_state,
            m.priority,
            'N/A'                                     AS parent_req_short_description,
            'N/A'                                     parent_req_status,
            m.opened_by_cost_center,
            m.opened_by_department,
            opened_by_email,
            opened_by_first_name,
            opened_by_last_name,
            m.requested_by_cost_center,
            m.requested_by_department,
            m.requested_by_user_name,
            m.requested_for,
            m.requested_for_cost_center,
            m.requested_for_department
        FROM
            support_requests.support_request        m
            LEFT JOIN support_requests.metric_support_request r ON r.number = m.number
        WHERE
   -- r.field = 'state'
            r.definition = 'SU Support Request State Change'
            AND ( m.assignment_group in 
('FMS Financial Support Center-LD (Secure)',
'FMS Systems and Reporting Operations (Secure)',
'FMS Financial Systems and Operations Support (Secure)',
'FMS PTA Manager',
'FMS iJournals/Feeder Request',
'FMS Chart of Accounts',
'FMS Financial Support Center-FFIT (Secure)')
                  OR EXISTS (
                SELECT
                    1
                FROM
                    support_requests.metric_support_request o_r
                WHERE
                    o_r.number = m.number
                    AND o_r.definition = 'SU Support Request Assignment Group'
                    AND o_r.value in 
('FMS Financial Support Center-LD (Secure)',
'FMS Systems and Reporting Operations (Secure)',
'FMS Financial Systems and Operations Support (Secure)',
'FMS PTA Manager',
'FMS iJournals/Feeder Request',
'FMS Chart of Accounts',
'FMS Financial Support Center-FFIT (Secure)')
                    AND o_r.end_time IS NOT NULL
            ) )
	  AND m.sys_updated_on  >= ( current_date - 2000 )
    ) m
    LEFT JOIN (
        SELECT
            number,
            parent
        FROM
            task_bs.task
        UNION
        SELECT
            number,
            parent
        FROM
            support_requests.support_task
    ) task ON ( m.number = task.parent )
UNION
SELECT
    1                              current_state,
    coalesce(
        r.work_start_timestamp, r.opened_at
    )                              sr_start_time,
    r.number,
    coalesce(
        r.work_start_timestamp, r.opened_at
    )                              state_start_time,
    r.work_end_timestamp           state_end_time,
    r.calendar_duration_in_days    duration_in_days,
    r.calendar_duration_in_hours   duration_in_hours,
    r.calendar_duration_in_minutes duration_in_minutes,
    r.sys_created_by               state_start_by,
    r.sys_created_by_name          state_start_username,
    r.state                        state,
    0                              reopened_counter,
    r.parent                       parent_req_number,
    r.opened_at                    sr_created_on_date,
    'Reporting'                    function_or_service,
    'EFR Dashboard Reports RITM'   business_service,
    'RITM'                         classification_code,
    r.short_description,
    r.assigned_to                  resolved_by_name,
    r.work_end_timestamp           u_resolved_at,
    r.assigned_to,
    r.assigned_to_user_name,
    r.assignment_group             current_assignment_group,
    r.state                        main_state,
    'N/A'                          task_created,
    r.priority,
    m.short_description            AS parent_req_short_description,
    m.state                        parent_req_status,
    r.opened_by_cost_center,
    r.opened_by_department,
    r.opened_by                    opened_by_email,
    r.opened_by                    opened_by_first_name,
    r.opened_by                    opened_by_last_name,
    r.requested_by_cost_center,
    r.requested_by_department,
    r.requested_for                requested_by_user_name,
    r.requested_for,
    r.requested_for_cost_center,
    r.requested_for_department
FROM
    task_bs.task r
    LEFT JOIN requests.request_item m ON r.parent = m.number
WHERE
    r.assignment_group in
('FMS Financial Support Center-LD (Secure)',
'FMS Systems and Reporting Operations (Secure)','FMS Financial Systems and Operations Support (Secure)',
'FMS PTA Manager',
'FMS iJournals/Feeder Request',
'FMS Chart of Accounts',
'FMS Financial Support Center-FFIT (Secure)')
--and parent is not null
    AND r.number LIKE 'TASK%'
	   AND r.sys_updated_on  >= ( current_date - 2000 )