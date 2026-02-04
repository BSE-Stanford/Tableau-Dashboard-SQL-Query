SELECT
MAX(sys_updated_on) last_updated_on,
Support_Request'   table_name
FROM
support_requests.support_request
UNION
SELECT
MAX(sys_updated_on)      last_updated_on,
Metric_Support_Request' table_name
FROM
support_requests.metric_support_request