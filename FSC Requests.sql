SELECT
    m.number,
    m.sys_created_on AS sr_created_on_date,
    m.function_or_service,
now(),
    m.business_service,
    m.classification_code,
    m.resolved_by_name,
    coalesce(
        m.u_resolved_at, m.closed_at
    )                     u_resolved_at,
    m.assigned_to,
    m.assigned_to_user_name,
    m.assignment_group    assignment_group,
    m.state               state,
    m.short_description,
    --m.description,
    m.comments,
    coalesce(
        task.number, 'N/A'
    )                     task_created,
    task.assignment_group task_assigned_group,
    task.state            task_state,
    m.work_notes,
    m.contact_type as contact_type_old,
	m.contact_type_upper as contact_type,
    m.close_notes,
    m.sys_class_name,
    m.priority,
    m.calendar_duration_in_days,
    m.calendar_duration_in_hours,
    m.calendar_duration_in_minutes,
    m.item                form,
    m.kcs_solution,
    m.knowledge,
    m.knowledge_base,
    m.survey_response,
    m.survey_comments,
    m.resolved_by_business_unit,
    m.resolved_by_department,
    m.opened_at,
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
	m.survey_comments,
    m.survey_response,
    m.urgency,
    --l.label_name tag_name,
	'NA' as tag_name,
    --l.label_created_by	tag_created_by	
	'NA' as tag_created_by,
	m.sys_updated_on last_snow_refreshed_date
FROM
    support_requests.support_request m
    LEFT JOIN (
        SELECT
            number,
            parent,
            assignment_group,
            state
        FROM
            support_requests.support_task t
		where EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3 )	
    )                                task ON ( m.number = task.parent )
	--LEFT JOIN task_bs.task_labels              l ON m.number = l.number
WHERE
    ( m.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
    'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
      OR EXISTS (
        SELECT
            1
        FROM
            support_requests.metric_support_request o_r
        WHERE
            o_r.number = m.number
            AND o_r.definition = 'SU Support Request Assignment Group'
            AND o_r.value IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
            'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)' )
            AND o_r.end_time IS NOT NULL
    ) )
	
    /*AND ( EXTRACT(YEAR FROM m.sr_created_on_date) = 2024
          OR EXTRACT(YEAR FROM coalesce(
        m.u_resolved_at, m.closed_at
    )) = 2024 )*/
    AND EXTRACT(YEAR FROM m.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3 )