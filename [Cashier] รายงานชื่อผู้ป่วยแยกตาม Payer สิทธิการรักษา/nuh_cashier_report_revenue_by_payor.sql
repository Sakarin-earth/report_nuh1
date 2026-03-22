-- DROP FUNCTION public.nuh_cashier_report_revenue_by_payor(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_cashier_report_revenue_by_payor(p_start_ts timestamp with time zone, p_end_ts timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

WITH revenue_data AS (
    SELECT 
        p.name AS payor,
        v.vn,
        v.hn,

        -- ✅ แปลง timezone ให้ตรง
        (v.created_at AT TIME ZONE 'Asia/Bangkok') AS created_at_th,

        ip.code AS insurance_code,
        pc.info->'th'->>'name' AS category_name,
        SUM(rd.total_net_paid) AS amount

    FROM coverage_usage cu
    JOIN visit v ON v.vn = cu.vn
    JOIN payor p ON p.id = cu.payor_id
    LEFT JOIN insurance_plan ip ON ip.id = cu.insurance_plan_id
    JOIN invoice inv ON inv.vn = v.vn AND inv.cancelled_at IS NULL
    JOIN receipt r ON r.invoice_id = inv.id AND r.cancelled_at IS NULL
    JOIN receipt_detail rd ON rd.receipt_id = r.id
    JOIN product_category pc ON pc.id = rd.product_category_id

    WHERE cu.deleted_at IS NULL
      AND v.latest_status_code = 'completed'
      AND v.deleted_at IS NULL

      -- ✅ แก้ timezone ตรงนี้ (สำคัญสุด)
      AND (v.created_at AT TIME ZONE 'Asia/Bangkok') >= (p_start_ts AT TIME ZONE 'Asia/Bangkok')
      AND (v.created_at AT TIME ZONE 'Asia/Bangkok') <  (p_end_ts   AT TIME ZONE 'Asia/Bangkok')

    GROUP BY 
        p.name, v.vn, v.hn, created_at_th,
        ip.code, pc.info->'th'->>'name'
)

SELECT jsonb_build_object(
    'meta', jsonb_build_object(
        'report_name', 'รายงานรายได้แยกตามสิทธิ',
        'start_timestamp', p_start_ts,
        'end_timestamp', p_end_ts
    ),
    'content',
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'payor', payor,
                'vn', vn,
                'hn', hn,

                -- ✅ format หลัง fix timezone แล้ว
                'visit_date', TO_CHAR(created_at_th, 'DD/MM/YYYY'),

                'insurance_code', insurance_code,
                'category_name', category_name,
                'amount', amount
            )
            ORDER BY payor, created_at_th, category_name
        ),
        '[]'::jsonb
    )
)
INTO result
FROM revenue_data;

RETURN result;

END;
$function$
;
