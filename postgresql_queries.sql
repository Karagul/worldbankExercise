

-- drop all before loading
drop table countrydata CASCADE ;
drop table incomeData CASCADE ;
drop table lendingData CASCADE ;
drop table CountryIncomeLending CASCADE ;
drop table gepdatatemp CASCADE ;
drop table gepdata CASCADE ;


------------------------------------------------------------------------------------------------------------
 --List countries with income level of "Upper middle income".
SELECT a.countryid,
       countrycode,
       country
FROM   countrydata a
       JOIN countryincomelending b
         ON a.countryid = b.countryid
WHERE  b.incomelevelid IN (SELECT DISTINCT incomelevelid
                           FROM   incomedata
                           WHERE  incomelevel = 'Upper middle income')

------------------------------------------------------------------------------------------------------------
--List countries with income level of "Low income" per region.
SELECT a.countryid,
       countrycode,
       country,
       countryregion
FROM   countrydata a
       JOIN countryincomelending b
         ON a.countryid = b.countryid
WHERE  b.incomelevelid IN (SELECT DISTINCT incomelevelid
                           FROM   incomedata
                           WHERE  incomelevel = 'Low income')

------------------------------------------------------------------------------------------------------------
--Find the region with the highest proportion of "High income" countries.
SELECT countryregion,
       Count(a.countryid)
FROM   countrydata a
       JOIN countryincomelending b
         ON a.countryid = b.countryid
WHERE  b.incomelevelid IN (SELECT DISTINCT incomelevelid
                           FROM   incomedata
                           WHERE  incomelevel = 'High income')
GROUP  BY countryregion
ORDER  BY Count(a.countryid) DESC

------------------------------------------------------------------------------------------------------------
--Calculate cumulative/running value of GDP per region ordered by income from lowest to highest and country name.

-- cleanup
DELETE FROM gepdata
WHERE  countryid = 'country code'

--cleanup
DELETE FROM gepdata
WHERE  gdp = ''

DELETE FROM gepdata
WHERE  Cast(gdp AS DECIMAL) > 1000

SELECT DISTINCT a.countryid,
                c.incomelevel,
                year,
                gdp,
                Sum(Cast(gdp AS DECIMAL))
                  OVER (
                    ORDER BY a.countryid, year) AS RunningTotal
FROM   gepdata a
       JOIN countryincomelending b
         ON a.countryid = b.countryid
       JOIN incomedata c
         ON b.incomelevelid = c.incomelevelid
ORDER  BY incomelevel,
          countryid,
          year

------------------------------------------------------------------------------------------------------------
--Calculate percentage difference in value of GDP year-on-year per country.
SELECT DISTINCT a.countryid,
                c.incomelevel,
                year,
                gdp,
                Sum(Cast(gdp AS DECIMAL))
                  OVER (
                    ORDER BY a.countryid, year) AS RunningTotal
INTO   temp_runninggdp
FROM   gepdata a
       JOIN countryincomelending b
         ON a.countryid = b.countryid
       JOIN incomedata c
         ON b.incomelevelid = c.incomelevelid
ORDER  BY incomelevel,
          countryid,
          year


------------------------------------------------------------------------------------------------------------
--List 3 countries with lowest GDP per region.
SELECT countryregion,
       country,
       gdpavg
FROM   (SELECT x.*,
               Row_number()
                 OVER (
                   partition BY countryregion
                   ORDER BY gdpavg DESC) AS Row_ID
        FROM   (SELECT c.countryregion                     AS countryregion,
                       c.country,
                       Round(Avg(Cast(gdp AS DECIMAL)), 2) AS gdpavg
                FROM   gepdata a
                       JOIN countryincomelending b
                         ON a.countryid = b.countryid
                       JOIN countrydata c
                         ON b.countryid = c.countryid
                GROUP  BY c.countryregion,
                          c.country
                ORDER  BY gdpavg,
                          c.countryregion,
                          c.country) x) AS A
WHERE  row_id <= 3
ORDER  BY countryregion,
          country  

