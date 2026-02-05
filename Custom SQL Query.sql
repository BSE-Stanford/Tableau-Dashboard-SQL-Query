SELECT
    ROW_NUMBER()
    OVER(PARTITION BY r.number, r.definition
         ORDER BY
             r.start_time DESC
    )                                         AS current_state,
    MIN(start_time)
    OVER(PARTITION BY r.number, r.definition) sr_start_time,
    t.number,
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
    t.sys_created_on                          sr_created_on_date,
    m.function_or_service,
    m.business_service,
    m.classification_code,
    t.closed_by                               resolved_by_name,
    t.closed_at                               u_resolved_at,
    t.assigned_to,
    t.assigned_to_user_name,
    t.assignment_group                        current_assignment_group,
    t.state                                   main_state,
    'N/A'                                     task_created,
    'N/A'                                     task_assigned_group,
    'N/A'                                     task_state,
    t.short_description,
    m.short_description                       AS parent_req_short_description,
    m.state                                   parent_req_status,
    m.assignment_group                        parent_assigned_group,
    t.priority,
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
    t.requested_for_department,
    'SR Task Assigned To Team'                AS type
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    r.definition = 'SU Support Req. Task State Change'
    AND ( ( t.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)',
    'FMS Payroll (Secure)', 'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' ) )
          OR ( m.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)',
          'FMS Payroll (Secure)', 'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' ) )
          OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_task o_r
        WHERE
            o_r.number = t.number
            AND o_r.definition = 'SU Support Req. Task Assignment Group'
            AND o_r.value IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
            'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
            AND o_r.end_time IS NOT NULL
    ) )
	
    --AND t.sys_updated_on  >= ( current_date - 2000)
    AND EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3 )
UNION ALL
SELECT
    o.current_state,
    o.sr_start_time,
    o.number,
    o.state_start_time,
    o.state_end_time,
    o.duration_in_days,
    o.duration_in_hours,
    o.duration_in_minutes,
    o.state_start_by,
    o.state_start_username,
    o.state,
    o.reopened_counter,
    o.parent_req_number,
    o.sr_created_on_date,
    o.function_or_service,
    o.business_service,
    o.classification_code,
    o.resolved_by_name,
    o.u_resolved_at,
    o.assigned_to,
    o.assigned_to_user_name,
    o.current_assignment_group,
    o.main_state,
    coalesce(
        task.number, 'N/A'
    )                     task_created,
    task.assignment_group task_assigned_group,
    task.state            task_state,
    o.short_description,
    o.parent_req_short_description,
    o.parent_req_status,
    o.parent_assigned_group,
    o.priority,
    o.opened_by_cost_center,
    o.opened_by_department,
    o.opened_by_email,
    o.opened_by_first_name,
    o.opened_by_last_name,
    o.requested_by_cost_center,
    o.requested_by_department,
    o.requested_by_user_name,
    o.requested_for,
    o.requested_for_cost_center,
    o.requested_for_department,
    CASE
    WHEN task.number IS NULL THEN
    'SR Assigned To Team'
    ELSE
    'SR with Task Assigned To Team'
    END                   AS type	
    --'SR Assigned To Team' type
FROM
    (
        SELECT
            ROW_NUMBER()
            OVER(PARTITION BY r.number, r.field
                 ORDER BY
                     r.start_time DESC
            )                                         AS current_state,
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
            'N/A'                                     parent_req_number,
            m.sr_created_on_date,
            m.function_or_service,
            m.business_service,
            m.classification_code,
            m.resolved_by_name,
            coalesce(
                m.u_resolved_at, m.closed_at
            )                                         u_resolved_at,
            m.assigned_to,
            m.assigned_to_user_name,
            m.assignment_group                        current_assignment_group,
            m.state                                   main_state,
            m.short_description,
            'N/A'                                     parent_req_short_description,
            'N/A'                                     parent_req_status,
            'N/A'                                     parent_assigned_group,
            m.priority,
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
            r.definition = 'SU Support Request State Change'
            AND ( m.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)',
            'FMS Payroll (Secure)', 'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
                  OR EXISTS (
                SELECT
                    1
                FROM
                    support_requests.metric_support_request o_r
                WHERE
                    o_r.number = m.number
                    AND o_r.definition = 'SU Support Request Assignment Group'
                    AND o_r.value IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)',
                    'FMS Payroll (Secure)', 'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
                    AND o_r.end_time IS NOT NULL
            ) )
	    --AND m.sys_updated_on  >= ( current_date - 2000 )
          AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3 )
    ) o
    LEFT JOIN (
        SELECT
            number,
            parent,
            assignment_group,
            state
        FROM
            support_requests.support_task
    ) task ON ( o.number = task.parent )
UNION ALL
SELECT
    1                              AS current_state,
    coalesce(
        r.work_start_timestamp, r.opened_at_timestamp
    )                              sr_start_time,
    r.number,
    coalesce(
        r.work_start_timestamp, r.opened_at_timestamp
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
    r.assigned_to                  resolved_by_name,
    r.work_end_timestamp           u_resolved_at,
    r.assigned_to,
    r.assigned_to_user_name,
    r.assignment_group             current_assignment_group,
    r.state                        main_state,
    'N/A'                          task_created,
    'N/A'                          task_assigned_group,
    'N/A'                          task_state,
    r.short_description,
    m.short_description            parent_req_short_description,
    m.state                        parent_req_status,
    'N/A'                          parent_assigned_group,
    r.priority,
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
    r.requested_for_department,
    'Task Assigned To Team'        AS type
FROM
    task_bs.task r
    JOIN requests.request_item m ON r.parent = m.number
WHERE
    r.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
    'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
--and parent is not null
    AND r.number LIKE 'TASK%'
    --AND r.sys_updated_on  >= ( current_date - 2000 )
    AND EXTRACT(YEAR FROM r.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3 )