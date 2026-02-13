WITH
airport as (
Select
airport_code,
TRIM(SPLIT_PART(airport_city_state, ',', 2)) AS state
FROM flight_delays.dim_airport_info
), 

monthly as (
Select
date_trunc('month', FL_Date) as flight_month,
a.airport_code,
a.state,
COUNT(*) as total_flights,
SUM(DEP_Delay) as total_dep_delay,

from flight_delays.fact_delays_currated fd
JOIN airport a ON fd.ORIGIN = a.airport_code

where Delay_Category = 'System Controlled' and DEP_Delay > 0 
group by 1,2,3),

rolling as (
SELECT *,
CASE 
  WHEN 
    COUNT(*) OVER (
      partition by airport_code
      order by flight_month
      rows between  5 preceding  and current row
    ) = 6
  THEN 1 END AS rolling_6mo_flag,
  
CASE 
  WHEN 
    COUNT(*) OVER (
      partition by airport_code
      order by flight_month
      rows between  11 preceding  and current row
    ) = 12
  THEN 1 END AS rolling_12mo_flag,
  
Sum(total_flights) over (
  partition by airport_code
  order by flight_month
  rows between 5 preceding and current row
) AS rolling_6mo_total_flight,

Sum(total_dep_delay) over (
  partition by airport_code
  order by flight_month
  rows between 5 preceding and current row
) AS rolling_6mo_dep_delay,

Sum(total_flights) over (
  partition by airport_code
  order by flight_month
  rows between 11 preceding and current row
) AS rolling_12mo_total_flight,

Sum(total_dep_delay) over (
  partition by airport_code
  order by flight_month
  rows between 11 preceding and current row
) AS rolling_12mo_dep_delay

from monthly)

SELECT
flight_month,
airport_code,
state,
total_flights,
total_dep_delay,
ROUND(total_dep_delay/total_flights,2) AS delay_rate,
CASE WHEN rolling_6mo_flag = 1 THEN rolling_6mo_total_flight END AS rolling_6mo_total_flight,
CASE WHEN rolling_6mo_flag = 1 THEN rolling_6mo_dep_delay END AS rolling_6mo_dep_delay,
CASE WHEN rolling_6mo_flag = 1 THEN round(rolling_6mo_dep_delay/rolling_6mo_total_flight,2) END AS rolling_6mo_delay_rate,
CASE WHEN rolling_12mo_flag = 1 THEN ROUND((rolling_12mo_dep_delay-rolling_6mo_dep_delay)/(rolling_12mo_total_flight-rolling_6mo_total_flight),2) END AS prior_6mo_delay_rate,
CASE WHEN rolling_12mo_flag = 1 THEN rolling_12mo_total_flight END AS rolling_12mo_total_flight,
CASE WHEN rolling_12mo_flag = 1 THEN rolling_12mo_dep_delay END AS rolling_12mo_dep_delay,
CASE WHEN rolling_12mo_flag = 1 THEN round(rolling_12mo_dep_delay/rolling_12mo_total_flight,2) END AS rolling_12mo_delay_rate,
ROUND(rolling_6mo_delay_rate-prior_6mo_delay_rate,2) as improvement_rate_non_overlap_shortterm,
ROUND(rolling_6mo_delay_rate-rolling_12mo_delay_rate,2) as improvent_rate_bias_overlap_longterm


FROM rolling
where airport_code = 'DCA' and year(flight_month)=2023
order by airport_code,flight_month asc;




