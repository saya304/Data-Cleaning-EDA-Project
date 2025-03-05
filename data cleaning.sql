--DATA CLEANING SQL QUERIES

--viewing the rows and columns and analyisng data. Making note of the changes to be made.
select * from layoff_raw;

-- Making changes to the raw data is not best practice. So we are creating a staging / duplicate table and performing data cleaning there.

--create a similar table like 'layoff_raw'.
create or replace table layoff_staging
like layoff_raw;

-- Copying all data from 'layoff_raw' to 'layoff_staging'
insert into layoff_staging
select * from layoff_raw

select * from layoff_staging
--------------------------------------------------------------------------------------------------------------------------------------------------------
--1. REMOVING DUPLICATES

--create a CTE to display all the duplicate rows. This is done by using window function 'row_number()' to display row number. 
--The duplicate rows are numbered '2' 
with duplicate_cte as (
select *,
row_number () over (partition by company, location, industry, total_laid_off, date, stage, country, funds_raised_millions order by country) as row_num
from LAYOFF_STAGING
)

select * from duplicate_cte
where row_num >1

-- Create a new duplicate table 'layoff_'
create TABLE WORLD_LAYOFF.LAYOFF_TABLE.LAYOFF_STAGING2 (
	COMPANY VARCHAR(16777216),
	LOCATION VARCHAR(16777216),
	INDUSTRY VARCHAR(16777216),
	TOTAL_LAID_OFF VARCHAR(16777216),
	PERCENTAGE_LAID_OFF VARCHAR(16777216),
	DATE VARCHAR(16777216),
	STAGE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	FUNDS_RAISED_MILLIONS VARCHAR(16777216),
    row_num INT
);

select * from layoff_staging2

insert into layoff_staging2
select *,
row_number () over (partition by company, location, industry, total_laid_off, date, stage, country, funds_raised_millions order by country) as row_num
from LAYOFF_STAGING

select * from layoff_staging2
where row_num >1

delete
from layoff_staging2
where row_num >1

--------------------------------------------------------------------------------------------------------------------------------------------------------

--2. STANDARDIZE THE DATA
--checked if there is any issue with the data in company. seems like there is no error for now
select company, trim(company)
from layoff_staging2
order by company

-- there were many duplicate industries with various names
select distinct industry
from layoff_staging2
order by 1
-- for example there were three industries 'Crypto', Crypto Currency and Cryptocurrency.
select *
from layoff_staging2
where industry like 'Crypto%'

-- so we update all to reflect as Crypto
update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%'

--
select distinct country
from layoff_staging2
order by 1

select distinct country, rtrim(country,'.')
from layoff_staging2


-- drop / change duplicate country names; changed 'United States.' to 'United States'
update layoff_staging2
set country = rtrim(country,'.')
where country like 'United States%'

--transform the format of date column
select date, to_date (date, 'YYYY-MM-DD') 
from layoff_staging2
order by 1

update layoff_staging2
set date_dup = to_date (date_dup, 'YYYY-MM-DD')

--In snowflake, it is not possible to directly change data type from VARCHAR to DATE. 
--So we create a duplicate column, copy the data and delete the original column
--Step 1: Rename the Existing Column
alter table layoff_staging2 rename column date to date_dup;

--Step 2: Add a New Column with the Desired Data Type
alter table layoff_staging2 add column layoff_date DATE;

--Step 3: Copy Data from the Old Column to the New One
update layoff_staging2
set layoff_date = date_dup--::varchar(50);

--Step 4: Drop the Old Column
alter table layoff_staging2 drop column date_dup;

-- Changing the datatype of 'Total_laid_off' from VARCHAR to NUMBER

ALTER TABLE layoff_staging2 ADD COLUMN laid_off_new NUMBER;

UPDATE layoff_staging2
SET laid_off_new = TRY_CAST(total_laid_off AS NUMBER(10,2));

ALTER TABLE layoff_staging2 DROP COLUMN total_laid_off;

ALTER TABLE layoff_staging2 RENAME COLUMN laid_off_new TO total_laid_off;

--Changing the datatype of 'percentage_laid_off' from VARCHAR to NUMBER

ALTER TABLE layoff_staging2 ADD COLUMN percent_new NUMBER;

UPDATE layoff_staging2
SET percent_new = TRY_CAST(percentage_laid_off AS NUMBER(10,2));

ALTER TABLE layoff_staging2 DROP COLUMN percentage_laid_off;

ALTER TABLE layoff_staging2 RENAME COLUMN percent_new TO percentage_laid_off;

--Changing the datatype of 'funds_rasied_millions' from VARCHAR to NUMBER

ALTER TABLE layoff_staging2 ADD COLUMN funds_new NUMBER;

UPDATE layoff_staging2
SET funds_new = TRY_CAST(FUNDS_RAISED_MILLIONS AS NUMBER(10,2)); 

ALTER TABLE layoff_staging2 DROP COLUMN FUNDS_RAISED_MILLIONS;

ALTER TABLE layoff_staging2 RENAME COLUMN funds_new TO FUNDS_RAISED_MILLIONS;

--------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. NULL OR BLANK VALUES

-- Using JOIN to find the industry that were blank
select t1.company,t1.location, t1.industry, t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
on t1.company = t2.company
where (t1.industry IS NULL or t1.industry = 'NULL')
and t2.industry is not null

UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '' or industry = 'NULL';

-- Update/Fill rows where 'industry' is blank or NULL. Update with joins is not possible in Snowflake. So we use MERGE
MERGE INTO layoff_staging2 AS target
USING (
    SELECT company, industry
    FROM (
        SELECT company, industry,
               ROW_NUMBER() OVER (PARTITION BY company ORDER BY industry) AS rn
        FROM layoff_staging2
        WHERE industry IS NOT NULL
    ) sub
    WHERE rn = 1  -- Pick only one industry per company
) AS source
ON target.company = source.company
AND (target.industry IS NULL OR target.industry = 'NULL' OR target.industry = '')
WHEN MATCHED THEN 
UPDATE SET target.industry = source.industry;

--set varchar 'NULL' to value NULL
select * 
from layoff_staging2 
where total_laid_off = 'NULL';

UPDATE layoff_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

select * 
from layoff_staging2 
where percentage_laid_off ='NULL';

UPDATE layoff_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL';

--------------------------------------------------------------------------------------------------------------------------------------------------------

--4. REMOVE ANY ROWS / COLUMNS

--Most of the Exploratory Data Analysis is based on the 'total_laid_off' and 'percentage_laid_off'. 
--Remove the columns that have both the two columns blank or NULL
select * 
from layoff_staging2
where (total_laid_off is null or total_laid_off = ''or total_laid_off = 'NULL' )
and (percentage_laid_off is null or percentage_laid_off ='' or percentage_laid_off ='NULL')

DELETE FROM layoff_staging2
WHERE (total_laid_off is null or total_laid_off = ''or total_laid_off = 'NULL' )
and (percentage_laid_off is null or percentage_laid_off ='' or percentage_laid_off ='NULL')

-- Drop column 'row_num'
alter table layoff_staging2 drop column row_num
