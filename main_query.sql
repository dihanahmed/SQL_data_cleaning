


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

-- (1) filtering out duplicate with CTE-------------------------------------------------
-- So if there are duplicates (same company, location, industry, etc.), 
-- one will get row_num = 1, the next row_num = 2, etc.
with duplicate_cte as
(
select *,
row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off,`layoff_date`,
stage,country,funds_raised_millions ) row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;
-- ---------------------------------------------------------------------------

select *
from layoffs_staging
where company= 'Cazoo';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `layoffs_date` text,
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
partition by company,location, industry, total_laid_off, percentage_laid_off,`layoff_date`,
stage,country,funds_raised_millions ) row_num
from layoffs_staging;

-- to disable safe updates so That I can delete using primary key in where clause
SET SQL_SAFE_UPDATES = 0;


delete
from layoffs_staging2
where row_num>1;

select *
from layoffs_staging2
where row_num>1;


-- (2)standardizing data ----finding other issues in the data and fixing it--------------------



select company,trim(company)
from layoffs;

select *
from layoffs_staging2
where industry like 'Crypto%';

    -- triming white spaces -----
update layoffs_staging2
set company= trim(company);

    -- rename all industry which start naming "Crypto ..." into only "Crypto"
update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%';

select industry
from layoffs_staging2;

select distinct country 
from layoffs_staging2
order by 1; -- order by first column

   -- to remove "." from "United States"-----------------------layoffs_date
update layoffs_staging2
set country ="United States"
where country like "United States%";

select country
from layoffs_staging2
where country ="United States";

   -- changing "layoffs_date" from text to date type--

select `layoffs_date`,
str_to_date(`layoffs_date`,'%Y-%m-%d')
from layoffs_staging2;

update layoffs_staging2
set `layoffs_date`= str_to_date(`layoffs_date`,'%Y-%m-%d');

select layoffs_date
from layoffs_staging2;

         -- here's the changing happened by alter table
alter table layoffs_staging2
modify column `layoffs_date` date;



-- (3) NULL and Blank values handling ------------------------------------------------------
select * 
from layoffs_staging2
where total_laid_off  is null
and percentage_laid_off is null;

update layoffs_staging2
set industry= null
where industry='';

select *
from layoffs_staging2
where  industry is null
or industry ='';

select *
from layoffs_staging2
where company = 'Airbnb';


select st1.industry, st2.industry
from layoffs_staging2 as st1
join layoffs_staging2 as st2
	on st1.company=st2.company
where st1.industry is null
and st2.industry is not null;

update layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company= st2.company
set st1.industry= st2.industry
where st1.industry is null
and st2.industry is not null;

select company, industry
from layoffs_staging2
where industry is null;

select *
from layoffs_staging2
where company like "Bally%";


-- (4)deleting some unnecessary columns -----------------------------------------------------

-- select * 
-- from layoffs_staging2
-- where total_laid_off  is null
-- and percentage_laid_off is null;


-- delete
-- from layoffs_staging2
-- where total_laid_off  is null
-- and percentage_laid_off is null;


-- select *
-- from layoffs_staging2;

-- alter table layoffs_staging2
-- drop column row_num;