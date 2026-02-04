SELECT ---DISTINCT
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
    )                      task_created,
    task.assignment_group task_assigned_group,
    task.state             task_state,
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
    m.requested_for_department,
    m.survey_comments,
    m.survey_response,
	m.last_refresh_date,
	m.last_snow_refresh_date
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
            m.sys_created_on sr_created_on_date,
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
            m.requested_for_department,
            m.survey_comments,
            m.survey_response,
            CURRENT_DATE last_refresh_date,
            r.sys_updated_on last_snow_refresh_date
        FROM
            support_requests.support_request        m
            LEFT JOIN support_requests.metric_support_request r ON r.number = m.number
        WHERE
   -- r.field = 'state'
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
            AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 2)
    ) m
    LEFT JOIN (
        /*SELECT
            number,
            parent,
            state,
            assignment_group
        FROM
            task_bs.task
        UNION*/
        SELECT
            number,
            parent,
            state,
            assignment_group
        FROM
            support_requests.support_task st
            where EXTRACT(YEAR FROM st.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 2)
    ) task ON ( m.number = task.parent )