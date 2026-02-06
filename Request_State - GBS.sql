SELECT
    action_table.action AS action,
    t.*
FROM
    (
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
            m.item,
            t.short_description,
            t.closed_by                               resolved_by_name,
            t.closed_at                               u_resolved_at,
            t.assigned_to,
            t.assigned_to_user_name,
            t.assignment_group                        current_assignment_group,
            t.state                                   main_state,
            'N/A'                                     task_created,
            'N/A'                                     task_assigned_group,
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
    --AND t.assignment_group = 'FMS Evolve Financial Reporting (EFR) Support (Secure)'
            AND m.item IN ( 'Consult with Stanford Global Business Services', 'Get Section 117 Reporting Assistance', 'Request a Faculty Invitation Letter (Barile Law)',
            'Get Global Consolidation System (GCS) Assistance' )
            AND m.sys_updated_on >= ( current_date - 2000 )
    ) t
    
CROSS JOIN (
  select 1 as action union all select 2 ) action_table
union
SELECT distinct
    action_table.action                 AS action,
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
    m.item,
    m.short_description,
    m.resolved_by_name,
    m.u_resolved_at,
    m.assigned_to,
    m.assigned_to_user_name,
    m.current_assignment_group,
    m.main_state,
    coalesce(
        task.number, 'N/A'
    )                     task_created,
    task.assignment_group task_assigned_group,
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
            m.item,
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
            AND ( m.item IN ( 'Consult with Stanford Global Business Services', 'Get Section 117 Reporting Assistance', 'Request a Faculty Invitation Letter (Barile Law)',
            'Get Global Consolidation System (GCS) Assistance' )
                  /*OR EXISTS (
                SELECT
                    1
                FROM
                    support_requests.metric_support_request o_r
                WHERE
                    o_r.number = m.number
                    AND o_r.definition = 'SU Support Request Assignment Group'
                    AND o_r.value = 'FMS Evolve Financial Reporting (EFR) Support (Secure)'
                    AND o_r.end_time IS NOT NULL
            )*/ )
			  AND m.sys_updated_on  >= ( current_date - 2000 )
    ) m
    LEFT JOIN (
        SELECT
            number,
            parent,
            assignment_group
        FROM
            task_bs.task
        UNION
        SELECT
            number,
            parent,
            assignment_group
        FROM
            support_requests.support_task
    ) task ON ( m.number = task.parent )
    CROSS JOIN (
  select 1 as action union all select 2 ) action_table