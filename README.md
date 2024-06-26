The source files are downloaded from Freshdesk once a month and saved in an S3 bucket.

Snowflake objects:

1. External table for the source data
2. View with individual survey responses sentiments
3. Table and SQL stored procedure to populate monthly aggregation: number of surveys, aggregated surveys summary and sentiment
4. Task to update the table with monthly summaries
5. SQL Stored procedure to extract monthly data
6. Task and Python stored procedure to send nice formatted email with the summaries

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Surveys-Summarization/assets/16999229/3f1a36d3-1ae4-4b84-8484-05237e04b806)

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Surveys-Summarization/assets/16999229/f40baa3c-eaa0-4344-92b9-bb391ac8eaeb)
