-- Production per machine and shift: parts, scrap and median cycle time.
-- Spark SQL twin of gold.production_per_grain(parts, SHIFT_KEYS).
-- shift_parts is a temp view of silver parts with shift_date and shift_name.
SELECT
    machine_id,
    shift_date,
    shift_name,
    COUNT(*)                                  AS parts_produced,
    SUM(CASE WHEN is_scrap THEN 1 ELSE 0 END) AS scrap_count,
    MEDIAN(cycle_time_s)                      AS median_cycle_time_s
FROM shift_parts
GROUP BY machine_id, shift_date, shift_name
