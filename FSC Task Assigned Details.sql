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
    current_date,
    r.sys_updated_on
FROM
    support_requests.support_task t
    JOIN support_requests.support_request     m ON m.number = t.parent
    LEFT JOIN support_requests.metric_support_task r ON r.number = t.number
WHERE
    --r.field = 'assigned_to'
    r.definition = 'SU Support Req. Task Assignment Group'
    --AND t.assignment_group = 'FMS Vendor Services'
    AND ( ( t.assignment_group IN ('FMS Supplier Enablement','FMS Vendor Services','FMS Accounts Payable','FMS Business Expense',
       	'FMS Procurement Tax','FMS Payment Operations','FMS Contracts','FMS Purchasing','FMS Credit Card Services') )
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
            AND o_r.value IN ('FMS Supplier Enablement','FMS Vendor Services','FMS Accounts Payable','FMS Business Expense',
       	'FMS Procurement Tax','FMS Payment Operations','FMS Contracts','FMS Purchasing','FMS Credit Card Services')
            AND o_r.end_time IS NOT NULL
    ) )
    --AND m.sr_created_on_date >= ( current_date - 3000 )
    AND EXTRACT(YEAR FROM t.sys_updated_on) >= ( EXTRACT(YEAR FROM current_date) - 5 )