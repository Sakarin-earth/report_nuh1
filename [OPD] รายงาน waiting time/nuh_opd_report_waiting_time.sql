-- DROP FUNCTION public.nuh_opd_report_waiting_time(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_opd_report_waiting_time(p_start_ts timestamp with time zone, p_end_ts timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

WITH visit_data AS (
    SELECT 
        prac.display AS doctor_name,
        e.vn,
        cu.an,
        v.hn,

        -- ✅ แปลง timezone ให้ตรงตั้งแต่ต้น
        (e.created_at AT TIME ZONE 'Asia/Bangkok') AS created_at_th,
        (e.completed_at AT TIME ZONE 'Asia/Bangkok') AS completed_at_th,

        ip.name AS insurance,
        e.diagnosis::text AS icd10,
        e."procedure"::text AS icd9cm

    FROM encounter e
    JOIN visit v ON v.vn = e.vn
    LEFT JOIN practitioner prac ON prac.id = e.practitioner_id
    LEFT JOIN coverage_usage cu ON cu.vn = e.vn AND cu.deleted_at IS NULL
    LEFT JOIN insurance_plan ip ON ip.id = cu.insurance_plan_id

    WHERE v.latest_status_code = 'completed'
      AND e.deleted_at IS NULL
      /*
        ช่วงวันที่ตามปฏิทินไทย (Asia/Bangkok) แบบ [เริ่ม 00:00, สิ้นสุด 23:59:59]
        - API มักส่ง UTC เช่น 2026-03-01T17:00:00Z = วันที่ 2 มี.ค. 00:00 น. ไทย
        - ถ้าเทียบแค่ (created_at AT TIME ZONE ...) < (p_end AT TIME ZONE ...) โดย p_end = เที่ยงคืนวันสุดท้ายที่เลือก
          จะตัดข้อมูลทั้งวันสุดท้ายทิ้ง → ต้องใช้ขอบบนแบบ +1 วัน (ครึ่งช่วงเปิด)
      */
      AND e.created_at >= (
            date_trunc('day', p_start_ts AT TIME ZONE 'Asia/Bangkok')
            AT TIME ZONE 'Asia/Bangkok'
      )
      AND e.created_at < (
            (date_trunc('day', p_end_ts AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day')
            AT TIME ZONE 'Asia/Bangkok'
      )
)

SELECT jsonb_build_object(
    'meta', jsonb_build_object(
        'report_name', 'รายงาน waiting time',
        'timezone', 'Asia/Bangkok',
        'start_date', to_char((date_trunc('day', p_start_ts AT TIME ZONE 'Asia/Bangkok'))::date, 'YYYY-MM-DD'),
        'end_date', to_char((date_trunc('day', p_end_ts AT TIME ZONE 'Asia/Bangkok'))::date, 'YYYY-MM-DD'),
        'start_date_display', to_char((date_trunc('day', p_start_ts AT TIME ZONE 'Asia/Bangkok'))::date, 'DD/MM/YYYY'),
        'end_date_display', to_char((date_trunc('day', p_end_ts AT TIME ZONE 'Asia/Bangkok'))::date, 'DD/MM/YYYY'),
        'start_timestamp', p_start_ts,
        'end_timestamp', p_end_ts,
        'total_records', (SELECT COUNT(*)::int FROM visit_data)
    ),
    'content',
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'doctor_name', vd.doctor_name,
                    'vn', vd.vn,
                    'an', vd.an,
                    'hn', vd.hn,
                    'visit_date', TO_CHAR(vd.created_at_th, 'DD/MM/YYYY'),
                    'time_in', TO_CHAR(vd.created_at_th, 'HH24:MI'),
                    'time_out', TO_CHAR(vd.completed_at_th, 'HH24:MI'),
                    'waiting_time',
                    CASE
                        WHEN vd.completed_at_th IS NOT NULL
                        THEN ROUND(EXTRACT(EPOCH FROM (vd.completed_at_th - vd.created_at_th)) / 60) || ' นาที'
                        ELSE NULL
                    END,
                    'insurance', vd.insurance,
                    'icd10', vd.icd10,
                    'icd9cm', vd.icd9cm
                )
                ORDER BY vd.doctor_name, vd.created_at_th
            )
            FROM visit_data vd
        ),
        '[]'::jsonb
    )
)
INTO result;

RETURN result;

END;
$function$
;
