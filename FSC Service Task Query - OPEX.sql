SELECT
    t.number,
    t.sys_created_on                   sr_created_on_date,
    NOW(),
    m.function_or_service,
    m.business_service,
    m.classification_code,
    t.closed_by                        resolved_by_name,
    t.closed_at                        u_resolved_at,
    t.assigned_to,
    t.assigned_to_user_name,
    t.assignment_group,
    t.state ,
    t.short_description,
    t.description,
    t.work_notes_list                  AS work_notes,
    t.contact_type,
    t.close_notes,
    t.sys_class_name,
    t.priority,
    t.calendar_duration                calendar_duration_in_days,
    t.calendar_duration                calendar_duration_in_hours,
    t.calendar_duration                calendar_duration_in_minutes,
    t.closed_by_department_cost_center resolved_by_business_unit,
    t.closed_by_department,
	t.sys_created_by  task_created_by,
	t.sys_created_by_name task_created_by_name,
    t.sys_created_on                   opened_at,
    concat(
        t.requested_by_first_name, ' ', t.requested_by_last_name
    )                                  requested_by,
    t.requested_by_cost_center,
    t.requested_by_department,
    t.requested_by_sys_created_by      requested_by_user_name,
    concat(
        t.requested_for_first_name, ' ', t.requested_for_last_name
    )                                  requested_for,
    t.requested_for_cost_center,
    t.requested_for_department,
    t.parent                           parent_req_number,
    m.short_description                AS parent_req_short_description,
    m.state                            parent_req_status,
    m.assignment_group                 parent_assigned_group,
    t.sys_updated_on ,
	current_date as refresh_date
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
 WHERE
     ( (t.assignment_group IN ( 'FMS Financial Support Center-LD (Secure)', 'FMS Evolve Financial Reporting (EFR) Support (Secure)', 'FMS Payroll (Secure)',
            'FMS Financial Support Center (FSC)', 'FMS Financial Support Center-FFIT (Secure)', 'FMS Financial Support Center (FSC) (Secure)','FMS Supplier Enablement','FMS Vendor Services','FMS Accounts Payable','FMS Business Expense',
       	'FMS Procurement Tax','FMS Payment Operations','FMS Contracts','FMS Purchasing','FMS Credit Card Services' ))
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
      AND EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 3)