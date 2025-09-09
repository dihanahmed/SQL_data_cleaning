select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

select *
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


select min(layoffs_date), max(layoffs_date)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select year(`layoffs_date`), sum(total_laid_off)
from layoffs_staging2
group by year(`layoffs_date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select substring(layoffs_date,1,7) as month, sum(total_laid_off)
from layoffs_staging2
where substring(layoffs_date,1,7) is not null
group by month
order by 1 asc;


with rolling_total as
(

select substring(layoffs_date,1,7) as month, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(layoffs_date,1,7) is not null
group by month
order by 1 asc
)

select month, total_off, sum(total_off) over(order by month) as rolling
from rolling_total;



select company, sum( total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select company, year(layoffs_date), sum( total_laid_off)
from layoffs_staging2
group by company, year(layoffs_date)
order by 3 desc ;



with company_year (company,year, total_laid_off) as 
(

select company, year(layoffs_date), sum( total_laid_off)
from layoffs_staging2
group by company, year(layoffs_date)


), company_year_rank as
(
select *, DENSE_RANK() over (partition by year order by total_laid_off desc) as ranking
from company_year
where year is not null
-- order by ranking asc 
)

select * 
from company_year_rank
where ranking<=5;