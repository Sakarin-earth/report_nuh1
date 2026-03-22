-- DROP FUNCTION public.nuh_opd_appointment_reschedule_report(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_opd_appointment_reschedule_report(p_start timestamp with time zone, p_end timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

WITH appointment_data AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY a."start") AS seq_no,
        a.patient_hn AS hn,
        TO_CHAR(a."start" AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY') AS latest_appointment_date,
        TO_CHAR(a."start" AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') AS latest_appointment_time,
        a.service_type::text AS nurse_appointment_type,
        prac.display AS doctor_name,
        c.name AS department_name,
        a.current_status::text AS appointment_status,

        -- 🔁 reschedule count
        (
            WITH RECURSIVE chain AS (
                SELECT a2.id, a2.moved_from_appointment_id, 1 AS cnt
                FROM appointment a2
                WHERE a2.id = a.moved_from_appointment_id
                UNION ALL
                SELECT a3.id, a3.moved_from_appointment_id, ch.cnt + 1
                FROM appointment a3
                JOIN chain ch 
                  ON a3.id = ch.moved_from_appointment_id
            )
            SELECT COALESCE(MAX(cnt), 0) FROM chain
        ) AS reschedule_count,

        -- original date
        (
            SELECT TO_CHAR(prev."start" AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY')
            FROM appointment prev 
            WHERE prev.id = a.moved_from_appointment_id
        ) AS original_appointment_date,

        -- rescheduled to
        TO_CHAR(a."start" AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY') AS rescheduled_to_date

    FROM appointment a
    LEFT JOIN practitioner prac ON prac.id = a.practitioner_id
    LEFT JOIN clinic c ON c.id = a.clinic_id

    WHERE a.deleted_at IS NULL
      AND a.moved_from_appointment_id IS NOT NULL

      -- ✅ FIX timezone (-1 day bug)
      AND (a."start" AT TIME ZONE 'Asia/Bangkok') >= date_trunc('day', p_start AT TIME ZONE 'Asia/Bangkok')
      AND (a."start" AT TIME ZONE 'Asia/Bangkok') < date_trunc('day', (p_end AT TIME ZONE 'Asia/Bangkok')) + INTERVAL '1 day'
)

SELECT jsonb_build_object(
    'meta', jsonb_build_object(
        'report_name', 'รายงานเลื่อนนัด',
        'start_datetime', p_start,
        'end_datetime', p_end,
        'total_records', (SELECT COUNT(*) FROM appointment_data),
        'generated_at', now()
    ),
    'content',
    (
        SELECT jsonb_agg(
            jsonb_build_object(
                'seq_no', seq_no,
                'hn', hn,
                'latest_appointment_date', latest_appointment_date,
                'latest_appointment_time', latest_appointment_time,
                'nurse_appointment_type', nurse_appointment_type,
                'doctor_name', doctor_name,
                'department_name', department_name,
                'appointment_status', appointment_status,
                'reschedule_count', reschedule_count,
                'original_appointment_date', original_appointment_date,
                'rescheduled_to_date', rescheduled_to_date
            )
            ORDER BY seq_no
        )
        FROM appointment_data
    )
)
INTO result;

RETURN result;

END;
$function$
;