select * from layoff_staging2;

--Looking at the maximum total laid off and percentage laid off
select max(total_laid_off), max(percentage_laid_off)
from layoff_staging2;

-- company that had percentage_laid_off = 1 ie the entire company was laid off
select * 
from layoff_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

--Amount raised by the company that were entirely laid off
select * 
from layoff_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc nulls last;

-- Finding the laid off number by company
select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc nulls last;

-- The date range of the layoffs in this data
select min (layoff_date), max(layoff_date)
from layoff_staging2;

--Total laid off based on Industry
select industry, sum(total_laid_off)
from layoff_staging2
group by industry
order by 2 desc nulls last;

--Total laid off based on Country
select country, sum(total_laid_off)
from layoff_staging2
group by country
order by 2 desc nulls last;

select year(layoff_date), sum(total_laid_off)
from layoff_staging2
group by year(layoff_date)
order by 1 desc nulls last;

select stage, sum(total_laid_off)
from layoff_staging2
group by stage
order by 2 desc nulls last;

select substr(layoff_date,1,7) as layoff_month, sum(total_laid_off)
from layoff_staging2
where layoff_month is not null
group by layoff_month
order by 1;

----------------------------------------------------------------------------------------------------------------
--Rolling total of layoffs per month
with rolling_total as 
(
select substr(layoff_date,1,7) as layoff_month, sum(total_laid_off) as total_off
from layoff_staging2
where layoff_month is not null
group by layoff_month
order by 1
)
select layoff_month, total_off, sum(total_off) over(order by layoff_month) as Rolling_total
from rolling_total;

select company,year(layoff_date), sum(total_laid_off)
from layoff_staging2
group by company, year (layoff_date)
order by 3 desc nulls last;

-----------------------------------------------------------------------------------------------------------------
-- Ranking the top 5 companies with the highest laid off each year (2020 - 2023)
with company_year (company, years, total_laid_off) as 
(
select company,year(layoff_date), sum(total_laid_off)
from layoff_staging2
group by company, year (layoff_date)
), 

company_year_rank as 
(
select *,
dense_rank() over (partition by years order by total_laid_off DESC nulls last) as ranking
from company_year
where years is not null
)

select * 
from company_year_rank
where ranking <=5;
