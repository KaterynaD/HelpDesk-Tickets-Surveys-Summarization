create or replace view mytest_db.tickets.tickets_surveys_sentiment as
select 
TicketId,
Rating,
Resolution,
Timeliness,
Knowledge	,
Courtesy,
TicketComment,
case when TicketComment is not null then SNOWFLAKE.CORTEX.SENTIMENT(TicketComment) end Sentiment,
Requester_Name,
Requester_Email,
Company,
TicketGroup,
Agent
from mytest_db.tickets.tickets_surveys
;

select * 
from mytest_db.tickets.tickets_surveys_sentiment
limit 100 offset 546