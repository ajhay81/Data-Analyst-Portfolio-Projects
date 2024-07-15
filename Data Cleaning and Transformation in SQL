-- DATA CLEANING

SELECT * 
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns

/* -- 1. Remove duplicates */

-- Duplicate raw table
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Search for duplicate data with row number

SELECT * 
FROM layoffs_staging;

SELECT *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

with duplicate_cte as
(
SELECT *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- Create a new table that already has row_num
CREATE TABLE `layoffs_staging_2` (
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

SELECT * 
FROM layoffs_staging_2;

insert into layoffs_staging_2
SELECT *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

select *
from layoffs_staging_2
where row_num > 1;

delete
from layoffs_staging_2
where row_num > 1;

select *
from layoffs_staging_2;

-- 2. Standardize data

select company, trim(company)
from layoffs_staging_2;

update layoffs_staging_2
set company = trim(company);

-- Just look at the industry column
select distinct industry
from layoffs_staging_2
order by 1;

-- We will justify the type of Cryptocurrency industry
select *
from layoffs_staging_2
where industry like 'crypto%';

-- Converting all types of industry to Crypto
update layoffs_staging_2
set industry = 'Crypto'
where industry like 'crypto%';

/* Just look/check the location column */
select distinct country
from layoffs_staging_2
order by 1;

/* Found the word United "States." so it will just be updated to United States*/
update layoffs_staging_2
set country = 'United States'
where country like 'United States%';

select *
from layoffs_staging_2
where country like 'United States%';

/* Change the date column which was originally in text format to date format */

select `date`,
str_to_date(`date`, '%m/%d/%Y,')
from layoffs_staging_2;

update layoffs_staging_2
set `date` = str_to_date(`date`, '%m/%d/%Y,');

alter table layoffs_staging_2
modify column `date` date;

select *
from layoffs_staging_2;

/* -- 3. Null values or blank values -- */

/* Check for null and empty values ​​in total laid off and percentage laid off */
select *
from layoffs_staging_2
where total_laid_off is null 
and percentage_laid_off is null;

/* Check for null and empty values ​​in industry */
select *
from layoffs_staging_2
where industry is null
or industry = '' order by 1;

select *
from layoffs_staging_2
where company like 'Bally%';

update layoffs_staging_2
set industry = null
where industry = '';

/* Industry columns that have content are joined to columns that have no content */

select t1.industry, t2.industry
from layoffs_staging_2 t1
join layoffs_staging_2 t2
	on t1.company = t2.company
where (t1.industry is null  or t1.industry = '')
and t2.industry is not null;	

update layoffs_staging_2 t1 
join layoffs_staging_2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;	

select *
from layoffs_staging_2;

/*## Total laid off and percentage laid off cannot be done because there is no total company data column  */

/*## 4. Remove any columns   */
select *
from layoffs_staging_2
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs_staging_2
where total_laid_off is null 
and percentage_laid_off is null;

/* Drop the row_num column because it is no longer needed */
alter table layoffs_staging_2
drop column row_num;

select *
from layoffs_staging_2;

