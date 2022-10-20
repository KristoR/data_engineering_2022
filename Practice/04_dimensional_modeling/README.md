
### SQL Server DWH example

`docker compose -f mssql/docker-compose.yml up -d`

Add sample DW database to the tmp folder.

Sample data:
https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2019.bak

You can use SSMS to restore the `.bak` file as a database:
https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16

#### Notes and questions
* What data should be loaded first, dimensions or facts?
* Generating surrogate primary keys - multiple source systems, handling validity
* Nulls
  * Fact tables
    * OK for metrics
    * NOK for dimensions (FK)
  * Dimension tables
    * NOK

### Coming up with facts and dimensions
#### Postgres Dellstore

Review how to generate facts and dimensions from example database data.

`docker compose -f pg/docker-compose.yml up -d`

#### Google 
https://ga-dev-tools.web.app/dimensions-metrics-explorer/

#### Facebook ads
https://developers.facebook.com/docs/marketing-api/insights/parameters/v15.0

### Dimensional modeling techniques
https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/

### Assignment 1

What could be the facts and dimensions?

https://www.mysqltutorial.org/wp-content/uploads/2009/12/MySQL-Sample-Database-Schema.png

### Assignment 2 

What could be the facts and dimensions?

https://www.researchgate.net/profile/Teodora-Buda/publication/315535249/figure/fig1/AS:476869121318912@1490705866145/The-Financial-database-schema_W640.jpg

https://raw.githubusercontent.com/ldaniel/Predictive-Analytics/master/enunciation/PKDD'99%C2%A0Discovery%C2%A0Challenge.pdf
