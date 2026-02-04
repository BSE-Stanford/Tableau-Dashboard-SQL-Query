SELECT 
--DISTINCT
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
    o.opened_at,
    o.u_resolved_at,
    o.assigned_to,
    o.assigned_to_user_name,
    o.current_assignment_group,
    o.main_state,
    coalesce(
        task.number, 'N/A'
    )                     task_created,
    task.assignment_group task_assigned_group,
    o.short_description,
    o.parent_req_short_description,
    o.parent_req_status,
    o.priority,
    o.opened_by_cost_center,
    o.opened_by_department,
    o.opened_by_email,
    o.opened_by_first_name,
    o.opened_by_last_name,
    o.requested_by,
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
    END          AS         type	
    --'SR Assigned To Team' type
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
            m.priority,
            m.opened_by_cost_center,
            m.opened_by_department,
            m.opened_at,
            opened_by_email,
            opened_by_first_name,
            opened_by_last_name,
            m.requested_by,
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
              /*    OR EXISTS (
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
            )*/ )
			 AND m.sr_created_on_date >= (current_date - 2190)
    ) o
    LEFT JOIN (
        SELECT
            number,
            parent,
            assignment_group
        FROM
            support_requests.support_task
    ) task ON ( o.number = task.parent )