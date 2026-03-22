-- DROP FUNCTION public.nuh_cashier_financial_coverage_report(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_cashier_financial_coverage_report(p_start timestamp with time zone, p_end timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

WITH benefit_summary AS (
    SELECT 
        ipb.insurance_plan_id,
        cu.hn,
        SUM(COALESCE(ipb.total_benefit, 0) + COALESCE(ipb.total_none_benefit, 0)) AS total_amount,
        SUM(COALESCE(ipb.total_none_benefit, 0)) AS paid_amount,
        SUM(COALESCE(ipb.total_benefit, 0)) AS unpaid_amount
    FROM account_charge_item_insurance_plan_benefit ipb
    LEFT JOIN coverage_usage cu 
        ON cu.id = ipb.coverage_usage_id
    WHERE ipb.deleted_at IS NULL

      -- แก้ปัญหา day-1 โดยกำหนดขอบเขตวันตามเวลา Bangkok ให้ชัดเจน
      AND ipb.created_at >= (
            date_trunc('day', p_start AT TIME ZONE 'Asia/Bangkok')
            AT TIME ZONE 'Asia/Bangkok'
      )
      AND ipb.created_at < (
            (date_trunc('day', p_end AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day')
            AT TIME ZONE 'Asia/Bangkok'
      )

    GROUP BY ipb.insurance_plan_id, cu.hn
),

grouped_data AS (
    SELECT
        ip.name AS insurance_name,
        COUNT(DISTINCT bs.hn) AS patient_count,
        SUM(bs.total_amount) AS total_amount,
        SUM(bs.paid_amount) AS total_paid,
        SUM(bs.unpaid_amount) AS total_unpaid
    FROM benefit_summary bs
    JOIN insurance_plan ip 
        ON ip.id = bs.insurance_plan_id
    GROUP BY ip.name
),

ranked_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS seq_no,
        insurance_name,
        patient_count,
        total_amount,
        total_paid,
        total_unpaid
    FROM grouped_data
)

SELECT jsonb_build_object(
    'meta', jsonb_build_object(
        'report_name', 'รายงานแสดงข้อมูลของค่าใช้จ่ายผู้ป่วยใช้สิทธิ์ต่าง ๆ',
        'start_datetime', p_start,
        'end_datetime', p_end,
        'start_date', to_char((p_start AT TIME ZONE 'Asia/Bangkok')::date, 'YYYY-MM-DD'),
        'end_date', to_char((p_end AT TIME ZONE 'Asia/Bangkok')::date, 'YYYY-MM-DD'),
        'total_records', (SELECT COUNT(*) FROM grouped_data),
        'generated_at', now()
    ),
    'content',
    (
        SELECT jsonb_agg(
            jsonb_build_object(
                'seq_no', seq_no,
                'insurance_name', insurance_name,
                'patient_count', patient_count,
                'total_amount', total_amount,
                'total_paid', total_paid,
                'total_unpaid', total_unpaid
            )
            ORDER BY total_amount DESC
        )
        FROM ranked_data
    )
)
INTO result;

RETURN result;

END;
$function$
;
