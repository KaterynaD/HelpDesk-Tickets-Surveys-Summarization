
--SP to refresh monthly summaries
CREATE OR REPLACE PROCEDURE mytest_db.tickets.summarize_survey(month_id integer)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
  num_deleted integer default 0;
  num_inserted integer default 0;
  rs RESULTSET;
  var_month_id integer;
BEGIN

  rs := (
        select distinct cast(to_char(ReceivedTime, 'YYYYMM') as int) month_id
        from mytest_db.tickets.tickets_surveys 
        where (cast(to_char(ReceivedTime, 'YYYYMM') as int)=:month_id or :month_id is null )
        );
  
  FOR record IN rs DO
  
    var_month_id := record.month_id;
     
    delete from mytest_db.tickets.surveys_monthly_summaries
    where month_id=:var_month_id;

    num_deleted := num_deleted + SQLROWCOUNT;

    --Insert new

    insert into mytest_db.tickets.surveys_monthly_summaries
    with data as (
    SELECT to_char(ReceivedTime, 'YYYYMM') as month_id, 
    case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end TicketGroup,
    count(*) Monthly_Comments_Cnt,
    LISTAGG(TicketComment, ' ') WITHIN GROUP(ORDER BY ReceivedTime) AS MonthlyComments
    FROM mytest_db.tickets.tickets_surveys
    WHERE cast(to_char(ReceivedTime, 'YYYYMM') as int) = :var_month_id
    and TicketComment is not null
    GROUP BY
    to_char(ReceivedTime, 'YYYYMM'), 
    case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end
                 )
    select
      month_id,
      TicketGroup,
      Monthly_Comments_Cnt,
      SNOWFLAKE.CORTEX.SENTIMENT(MonthlyComments) Monthly_Sentiment,
      SNOWFLAKE.CORTEX.SUMMARIZE(MonthlyComments) Monthly_Comments_Summary
    from data;

    num_inserted := num_inserted + SQLROWCOUNT;
    
  END FOR;

 RETURN 'Deleted: ' ||  num_deleted || '. Inserted: ' || num_inserted || '.';
  

END;

-- Create a task to run every month to update summaries for a previous month: 
CREATE OR REPLACE TASK mytest_db.tickets.summarize_survey_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles' -- Runs once a month
AS
CALL mytest_db.tickets.summarize_survey(cast(to_char(ADD_MONTHS(CURRENT_DATE, -12),'YYYYMM') as int));



-- Creates a Stored Procedure to extract monthly surveys summaries
CREATE OR REPLACE PROCEDURE mytest_db.tickets.extract_survey_summaries(month_id integer)
RETURNS TABLE ()
LANGUAGE sql 
AS
BEGIN
    
DECLARE res RESULTSET DEFAULT (
    SELECT *
    FROM mytest_db.tickets.surveys_monthly_summaries
    WHERE (month_id=:month_id or (month_id=cast(to_char(ADD_MONTHS(CURRENT_DATE, -12),'YYYYMM') as int) and (:month_id is null or :month_id=190001)))
    ORDER BY TicketGroup
    );
BEGIN 
    RETURN table(res);
END;
END;




-- Create Snowpark Python Stored Procedure to format email and send it
CREATE OR REPLACE PROCEDURE mytest_db.tickets.send_surveys_summaries()
RETURNS string
LANGUAGE python
runtime_version = 3.9
packages = ('snowflake-snowpark-python')
handler = 'send_email'
AS
$$
def send_email(session):
    session.call('mytest_db.tickets.extract_survey_summaries',190001).collect()


    html_table = session.sql("select * from table(result_scan(last_query_id(-1)))").to_pandas().to_html()
    # https://codepen.io/labnol/pen/poyPejO?editors=1000
    html_table = html_table.replace('class="dataframe"', 'style="border: solid 2px #DDEEEE; border-collapse: collapse; border-spacing: 0; font: normal 14px Roboto, sans-serif;"')
    html_table = html_table.replace('<th>', '<th style="background-color: #DDEFEF; border: solid 1px #DDEEEE; color: #336B6B; padding: 10px; text-align: left; text-shadow: 1px 1px 1px #fff;">')
    html_table = html_table.replace('<td>', '<td style="    border: solid 1px #DDEEEE; color: #333; padding: 10px; text-shadow: 1px 1px 1px #fff;">')
      
    session.call('system$send_email',
        'my_email_int',
        'drogaieva@gmail.com',
        'Email Alert: Prev Month Surveys Summaries',
        html_table,
        'text/html')
$$;




-- Orchestrating the Tasks: 
CREATE OR REPLACE TASK mytest_db.tickets.send_surveys_summaries_task
    warehouse = compute_wh
    AFTER mytest_db.tickets.summarize_survey_task
    AS CALL mytest_db.tickets.send_surveys_summaries();


-- Steps to resume and then immediately execute the task DAG:  
ALTER TASK mytest_db.tickets.send_surveys_summaries_task RESUME;
ALTER TASK mytest_db.tickets.summarize_survey_task RESUME;
EXECUTE TASK mytest_db.tickets.summarize_survey_task;

ALTER TASK mytest_db.tickets.summarize_survey_task SUSPEND;
ALTER TASK mytest_db.tickets.send_surveys_summaries_task SUSPEND;



