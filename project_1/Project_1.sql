
-- Підключитися та дослідити дані
SELECT *
FROM public.cohort_users_raw
limit 10;

SELECT *
FROM public.cohort_events_raw
where event_type is not null
limit 10;

-- Створюємо СТЕ для очищення дат у cohort_users_raw

WITH normalized_users as (    
SELECT
        *,
        replace(
            replace(trim(split_part(signup_datetime, ' ', 1)), '.', '-'),  -- очищаємо signup_datetime від лишніх знаків
            '/', '-'
        ) AS signup_ts_str
    FROM cohort_users_raw
),
cleaned_users AS (
    SELECT
        user_id,
        promo_signup_flag,
        CASE
            WHEN signup_ts_str ~ '^\d{1,2}-\d{1,2}-\d{4}$' -- перевірка формату
            THEN to_date(signup_ts_str, 'DD-MM-YYYY')      -- конвертація в дату
            WHEN signup_ts_str ~ '^\d{1,2}-\d{1,2}-\d{2}$'
            THEN to_date(signup_ts_str, 'DD-MM-YY')
            ELSE NULL
        END AS signup_ts
    FROM normalized_users
)
select *
from cleaned_users
;

-- Створюємо СТЕ для очищення дат у cohort_events_raw

WITH normalized_events AS (   -- очищаємо event_datetime від лишніх знаків
    SELECT
        *,
        replace(
            replace(trim(split_part(event_datetime, ' ', 1)), '.', '-'),
            '/', '-'
        ) AS event_ts_str
    FROM cohort_events_raw
),
cleaned_events AS (
    SELECT
        user_id,
        NULLIF(trim(event_type), '') AS event_type, --- очищаємо від порожніх значень, після JOIN не працює
                CASE
            WHEN event_ts_str ~ '^\d{1,2}-\d{1,2}-\d{4}$'
            THEN to_date(event_ts_str, 'DD-MM-YYYY')
            WHEN event_ts_str ~ '^\d{1,2}-\d{1,2}-\d{2}$'
            THEN to_date(event_ts_str, 'DD-MM-YY')
            ELSE NULL
        END AS event_ts
    FROM normalized_events
)
   select*
   from cleaned_events
   ;

-- 	ОБ'ЄДНАННЯ ТАБЛИЦЬ ТА ПОБУДОВА КОГОРТНОЇ ТАБЛИЦІ

WITH normalized_users AS (
    SELECT
        *,
        replace(
            replace(trim(split_part(signup_datetime, ' ', 1)), '.', '-'),
            '/', '-'
        ) AS signup_ts_str
    FROM cohort_users_raw
),
cleaned_users AS (
    SELECT
        user_id,
        promo_signup_flag,
        CASE
            WHEN signup_ts_str ~ '^\d{1,2}-\d{1,2}-\d{4}$'  
            THEN to_date(signup_ts_str, 'DD-MM-YYYY')      
            WHEN signup_ts_str ~ '^\d{1,2}-\d{1,2}-\d{2}$'
            THEN to_date(signup_ts_str, 'DD-MM-YY')
            ELSE NULL
        END AS signup_ts
    FROM normalized_users
),

normalized_events AS (
    SELECT
        *,
        replace(
            replace(trim(split_part(event_datetime, ' ', 1)), '.', '-'),
            '/', '-'
        ) AS event_ts_str
    FROM cohort_events_raw
),
cleaned_events AS (
    SELECT
        user_id,
        NULLIF(trim(event_type), '') AS event_type, --- очищаємо від порожніх значень, після JOIN не працює
                CASE
            WHEN event_ts_str ~ '^\d{1,2}-\d{1,2}-\d{4}$'
            THEN to_date(event_ts_str, 'DD-MM-YYYY')
            WHEN event_ts_str ~ '^\d{1,2}-\d{1,2}-\d{2}$'
            THEN to_date(event_ts_str, 'DD-MM-YY')
            ELSE NULL
        END AS event_ts
    FROM normalized_events
),

joined_ts AS (  --- переводимо дату у формат рік-місяць
    SELECT
        u.user_id,
        u.promo_signup_flag,
        e.event_type,
        e.event_ts,

        date_trunc('month', u.signup_ts)::date AS cohort_month, --- витягуємо когортний місяць

        ((
            date_part('year', e.event_ts) - date_part('year', u.signup_ts)  
        ) * 12
        +
        (
            date_part('month', e.event_ts) - date_part('month', u.signup_ts) 
        ))::int AS month_offset  --- різниця в місяцях (реєстрація 0 , наступні місяці 1,2,3...)

    FROM cleaned_users u
    JOIN cleaned_events e   --- об'єднуємо таблиці
        ON u.user_id = e.user_id

    WHERE
        u.signup_ts IS NOT null  --- виконуємо умови фільтрації
        AND e.event_ts IS NOT null
        AND e.event_type <> 'test_event'
        
       
)
SELECT
    promo_signup_flag,   --- фінальна агрегована таблиця
    cohort_month,
    month_offset,
    COUNT(DISTINCT user_id) AS users_total
FROM joined_ts
WHERE
  event_ts  BETWEEN '2025-01-01' AND '2025-06-30'
  GROUP BY
    promo_signup_flag,
    cohort_month,
    month_offset    
ORDER BY
    promo_signup_flag,
    cohort_month,
    month_offset
;


