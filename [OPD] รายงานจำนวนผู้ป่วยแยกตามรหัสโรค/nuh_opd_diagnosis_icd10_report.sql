-- DROP FUNCTION public.nuh_opd_diagnosis_icd10_report(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_opd_diagnosis_icd10_report(p_start timestamp with time zone, p_end timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

WITH month_names AS (
    SELECT ARRAY[
        'ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.',
        'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.'
    ]::text[] AS arr
),
bounds AS (
    SELECT
        (p_start AT TIME ZONE 'Asia/Bangkok')::date AS sd,
        (p_end AT TIME ZONE 'Asia/Bangkok')::date AS ed
),
diagnosis_data AS (
    SELECT
        v.hn AS hn,
        dx->>'code' AS diagnosis_code,
        dx->>'display' AS diagnosis_name,
        TO_CHAR(v.created_at AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY') AS visit_date
    FROM visit v,
         jsonb_array_elements(v.diagnosis) AS dx
    WHERE v.latest_status_code = 'completed'
      AND v.deleted_at IS NULL

      AND v.created_at >= (
            date_trunc('day', p_start AT TIME ZONE 'Asia/Bangkok')
            AT TIME ZONE 'Asia/Bangkok'
      )
      AND v.created_at < (
            (date_trunc('day', (p_end AT TIME ZONE 'Asia/Bangkok')) + INTERVAL '1 day')
            AT TIME ZONE 'Asia/Bangkok'
      )

      AND dx->>'code' IS NOT NULL
),
agg AS (
    SELECT
        diagnosis_code,
        MAX(diagnosis_name) AS diagnosis_name,
        COUNT(*)::bigint AS cnt
    FROM diagnosis_data
    GROUP BY diagnosis_code
)

SELECT jsonb_build_object(
    'meta', jsonb_build_object(
        'hospital_name', 'โรงพยาบาลมหาวิทยาลัยนเรศวร',
        'report_name', 'รายงานจำนวนผู้ป่วยแยกตามรหัสโรค',
        'start_datetime', p_start,
        'end_datetime', p_end,
        'start_date', to_char((SELECT sd FROM bounds), 'YYYY-MM-DD'),
        'end_date', to_char((SELECT ed FROM bounds), 'YYYY-MM-DD'),
        -- หัวข้อวันที่สำเร็จรูป (ไม่ต้อง split ในเทมเพลต — ลดโอกาส PDF/render พัง)
        'period_start_th',
            EXTRACT(DAY FROM (SELECT sd FROM bounds))::int::text || ' ' ||
            (SELECT arr FROM month_names)[EXTRACT(MONTH FROM (SELECT sd FROM bounds))::int] || ' ' ||
            LPAD((((EXTRACT(YEAR FROM (SELECT sd FROM bounds)))::int + 543) % 100)::text, 2, '0'),
        'period_end_th',
            EXTRACT(DAY FROM (SELECT ed FROM bounds))::int::text || ' ' ||
            (SELECT arr FROM month_names)[EXTRACT(MONTH FROM (SELECT ed FROM bounds))::int] || ' ' ||
            LPAD((((EXTRACT(YEAR FROM (SELECT ed FROM bounds)))::int + 543) % 100)::text, 2, '0'),
        'date_range',
            'ตั้งแต่วันที่ ' ||
            to_char(((p_start AT TIME ZONE 'Asia/Bangkok')::date + interval '543 years'), 'DD/MM/YYYY') ||
            ' - ' ||
            to_char(((p_end AT TIME ZONE 'Asia/Bangkok')::date + interval '543 years'), 'DD/MM/YYYY'),
        'total_records', (SELECT COUNT(*) FROM diagnosis_data),
        -- ชาย/หญิง: ยังไม่มี JOIN ตาราง patient ใน repo — ใส่ 0; คอลัมน์รวมใช้ cnt จากการนับรายการวินิจฉัย
        'sum_male', 0,
        'sum_female', 0,
        'sum_cnt', COALESCE((SELECT SUM(cnt) FROM agg), 0),
        'print_date', to_char(now() AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY HH24:MI')
    ),
    'content',
    (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'dx_code', diagnosis_code,
                    'dx_name', diagnosis_name,
                    'male', 0,
                    'female', 0,
                    'cnt', cnt
                )
                ORDER BY diagnosis_code
            ),
            '[]'::jsonb
        )
        FROM agg
    )
)
INTO result;

RETURN result;

END;
$function$
;
