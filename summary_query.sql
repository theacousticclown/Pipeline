SELECT 
  EXTRACT(DATE FROM hourly_timestamp) AS forecast_date,
  MAX(temperature_celsius) AS peak_temperature,
  AVG(relative_humidity_percentage) AS average_humidity,
  MAX(heat_stress_index) AS peak_heat_stress
FROM 
  `marketing_tech_pipeline.weather_metrics_hourly`
GROUP BY 
  forecast_date
ORDER BY 
  forecast_date DESC
LIMIT 7;