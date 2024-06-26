create or replace stage control_db.external_stages.ticket_survey_stage 
STORAGE_INTEGRATION = aws_kd_projects_stg_int
url='s3://kd-projects/other/survey/';


--ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'mm/dd/yyyy hh24:mi'

CREATE OR REPLACE EXTERNAL TABLE mytest_db.tickets.tickets_surveys
(ReceivedTime	TIMESTAMP_NTZ(9)	 as 	TO_TIMESTAMP_NTZ(value:c1::STRING,'mm/dd/yyyy hh24:mi')	,
TicketId	INTEGER	 as 	(value:c2::INTEGER)	,
Rating	VARCHAR	 as 	(value:c3::STRING)	,
Resolution	VARCHAR	 as 	(value:c4::STRING)	,
Timeliness	VARCHAR	 as 	(value:c5::STRING)	,
Knowledge	VARCHAR	 as 	(value:c6::STRING)	,
Courtesy	VARCHAR	 as 	(value:c7::STRING)	,
TicketComment	VARCHAR	 as 	(value:c8::TEXT)	,
Requester_Name	VARCHAR	 as 	(value:c9::STRING)	,
Requester_Email	VARCHAR	 as 	(value:c10::STRING)	,
Company	VARCHAR	 as 	(value:c11::STRING)	,
TicketGroup	VARCHAR	 as 	(value:c12::STRING)	,
Agent	VARCHAR	 as 	(value:c13::STRING)	
)
		 WITH LOCATION = @control_db.external_stages.ticket_survey_stage
		 FILE_FORMAT = (FORMAT_NAME='control_db.file_formats.csv_format_not_compressed' 
         SKIP_HEADER = 1 
         FIELD_OPTIONALLY_ENCLOSED_BY='"'
         )
         ;


create or replace TABLE MYTEST_DB.TICKETS.SURVEYS_MONTHLY_SUMMARIES (
	MONTH_ID INTEGER,
	TICKETGROUP VARCHAR(100),
	MONTHLY_COMMENTS_CNT INTEGER,
	MONTHLY_SENTIMENT FLOAT,
	MONTHLY_COMMENTS_SUMMARY VARCHAR(16777216)
);

