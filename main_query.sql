


-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns or rows that aren't necessary


SHOW VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'secure_file_priv';

USE world_layoffs;
TRUNCATE TABLE layoffs;



-- Option B: Windows line endings (CRLF)
LOAD DATA INFILE '/usr/local/mysql-8.0.42-macos15-arm64/mysql-files/layoffs.csv'
INTO TABLE world_layoffs.layoffs
FIELDS TERMINATED BY ','  ENCLOSED BY '"'  ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(company, location, industry, @tot, @pct, @dt, stage, country, @funds)
SET total_laid_off        = NULLIF(@tot,''),
    percentage_laid_off   = NULLIF(@pct,''),
    layoff_date           = STR_TO_DATE(@dt, '%m/%d/%Y'),
    funds_raised_millions = NULLIF(@funds,'');

-- --------------------------------------------------------------------

select *
from layoffs;

create table layoffs_staging -- copying a table like layoffs so that the raw data will be intact
like layoffs;

select * 
from layoffs_staging; 

insert layoffs_staging -- inserting all data from layoffs into layoffs_staging
select *
from layoffs;

-- filtering out duplicate with CTE----------------------------------------

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off,
stage,country,funds_raised_millions ) row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;
-- ---------------------------------------------------------------------------

select count(*)
from layoffs;

select count(*)
from layoffs_staging;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off, `date`,
stage,country,funds_raised_millions ) row_num
from layoffs_staging;

delete  
from layoffs_staging2
where row_num>1;