
def get_articles_metrics_query(project_id, dataset_id, connection_id):
    """
    Generate SQL for evaluating news articles with AI-generated metrics.

    Parameters:
    - project_id: str. Google Cloud project identifier.
    - dataset_id: str. BigQuery dataset containing the source table.
    - connection_id: str. Vertex AI connection used for AI.GENERATE functions.
    """
    return f'''

SELECT
  article_name,
  city,
  published,

  AI.GENERATE_BOOL(
    FORMAT("""
Does this article suggest that delivery drivers are experiencing financial pressure or debt due to work-related expenses?

Article name: %s
Content: %s
""", article_name, content),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ) AS financial_pressure,

  AI.GENERATE_BOOL(
    FORMAT("""
Does this article describe unsafe or dangerous working conditions for delivery drivers?

Article name: %s
Content: %s
""", article_name, content),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ) AS unsafe_conditions,

  AI.GENERATE_BOOL(
    FORMAT("""
Does this article mention lack of support, unfair treatment, or neglect from the platform (like iFood) toward gig workers?

Article name: %s
Content: %s
""", article_name, content),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ) AS lack_of_support,

  AI.GENERATE_BOOL(
    FORMAT("""
Does this article suggest that delivery drivers are overworked or at risk of burnout?

Article name: %s
Content: %s
""", article_name, content),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ) AS overwork_burnout

FROM `{project_id}.{dataset_id}.news_content_usa`
WHERE content IS NOT NULL AND content != '';

'''

def get_drivers_metrics_query(project_id, dataset_id, connection_id):
    """
    Construct SQL to compute driver metrics and stress scores using AI.

    Parameters:
    - project_id: str. Google Cloud project identifier.
    - dataset_id: str. BigQuery dataset containing driver and ride tables.
    - connection_id: str. Vertex AI connection used for AI.GENERATE functions.
    """
    return f"""

-- CREATE OR REPLACE TABLE MARTS_DATA.tbl_drivers_metrics AS 

WITH summary_drivers AS (
  SELECT drivers.Driver_ID,
        drivers.Age,
        drivers.City,
        drivers.Experience_Years,
        drivers.Average_Rating,
        rides.Date,
        COUNT(rides.Ride_ID) AS total_rides,
        ROUND(SUM(rides.fare),2) AS total_fare,
        SUM(rides.Duration_min) AS total_duration_min
  FROM `{project_id}.{dataset_id}.drivers_data` drivers
  LEFT JOIN `{project_id}.{dataset_id}.rides_data` rides
  ON drivers.Driver_ID = rides.Driver_ID
  GROUP BY drivers.Driver_ID,
           drivers.Age,
           drivers.City,
           drivers.Experience_Years,
           drivers.Average_Rating,
           rides.Date
  ORDER BY total_fare DESC
),

drivers_metrics AS (
  SELECT driver_ID,
         Age,
         City,
         Experience_Years,
         ANY_VALUE(Average_Rating) AS Average_Rating,
         COUNT(DISTINCT Date) AS active_days,
         SUM(total_rides) AS total_rides_all_days,
         MAX(total_rides) AS max_rides,
         AVG(total_rides) AS avg_rides,
         MIN(total_fare) AS min_fare,
         MAX(total_fare) AS max_fare,
         STDDEV(total_fare) AS stddev_fare,
         SUM(total_duration_min) AS total_duration_min_all_days
  FROM summary_drivers
  GROUP BY driver_ID, Age, City, Experience_Years
)

SELECT driver_ID,
       City,
       Age,
       AI.GENERATE_DOUBLE(
         prompt => FORMAT(\"""
Example:
A 44-year-old driver from Los Angeles with 11 years of experience, a 4.9 average rating, 7 rides in 5 active days, worked a total of 402 minutes. The minimum fare was $20.45 and the maximum was $215.37, with a fare standard deviation of $75. This driver has a stress level of 0.2.

Now assess the following driver:
A %d-year-old driver from %s with %d years of experience, a %.1f rating, %d rides in %d active days, worked a total of %d minutes. The minimum fare was $%.2f and the maximum was $%.2f, with a fare standard deviation of $%.2f. What is the estimated stress level (from 0 to 1)?
\""",
           Age,
           City,
           Experience_Years,
           Average_Rating,
           total_rides_all_days,
           active_days,
           total_duration_min_all_days,
           min_fare,
           max_fare,
           stddev_fare
         ),
         connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
         endpoint => 'gemini-2.0-flash'
       ).result AS stress_score,

  AI.GENERATE(
    prompt => FORMAT(\"""
Given the following metrics, write a short 2-sentence explanation describing this driver's work routine and stress rationale.

Driver Profile:
- Age: %d
- City: %s
- Experience: %d years
- Average rating: %.1f
- Active days: %d
- Total rides: %d
- Total minutes worked: %d
- Fare range: $%.2f to $%.2f
- Fare standard deviation: $%.2f

Your response should mention the workload and variability in earnings, and whether the stress level is likely high or low.
\""",
      Age,
      City,
      Experience_Years,
      Average_Rating,
      active_days,
      total_rides_all_days,
      total_duration_min_all_days,
      min_fare,
      max_fare,
      stddev_fare
    ),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ).result AS stress_reason
FROM drivers_metrics
"""


def get_driver_reason_tags_query(project_id, dataset_id, connection_id):
    """Generate SQL to tag driver stress reasons with keywords."""
    return f"""
SELECT driver_ID,
  AI.GENERATE(
    prompt => FORMAT(\"""
Given the stress reason described by the driver, return 2 to 3 keywords that reflect the health state of the driver.
Return like example below on a limit of 2 words by keyword.

Keyword1, Keyword2, Keyword3

Please see the stress text below:
%s
\""",
      stress_reason
    ),
    connection_id => 'projects/{project_id}/locations/us/connections/{connection_id}',
    endpoint => 'gemini-2.0-flash'
  ).result AS stress_report_tags
FROM `{project_id}.{dataset_id}.drivers_metrics`
"""
