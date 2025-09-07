
select * 
from layoffs; 

-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns or rows that aren't necessary


create table layoffs_staging -- copying a table like layoffs so that the raw data will be intact
like layoffs;

select * 
from layoffs_staging; 

insert layoffs_staging -- inserting all data from layoffs into layoffs_staging
select *
from layoffs;

