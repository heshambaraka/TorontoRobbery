
------------------------------------
--Dates
DROP TABLE IF EXISTS #Robbery_Dates
SELECT Conformed_Date, Conformed_Date_INT, Conformed_Date_Display, Calendar_Day_Of_Week, Calendar_Year, Calendar_Quarter, Calendar_Quarter_Display
   , Calendar_Month, Calendar_Month_Display, Is_Holiday, Holiday_Name, Is_ON_Holiday, Calendar_Part_Of_Week, Time_Of_Month, Calendar_Month_Year
   , Calendar_Month_Year_SORT, Calendar_Quarter_Year
INTO #Robbery_Dates
FROM Data_Warehouse.dbo.VIEW_D_Dates
WHERE 1=1
   AND Conformed_Date IN (
      SELECT DISTINCT CAST(occurrencedate AS DATE) FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK) UNION
      SELECT DISTINCT CAST(reporteddate AS DATE) FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
   )
ORDER BY 1
--SELECT * FROM #Robbery_Dates


------------------------------------
--Offences
DROP TABLE IF EXISTS #Offences
SELECT DISTINCT ucr_ext AS Offence_ID, Offence
INTO #Offences
FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
--SELECT TOP 200 * FROM #Offences


------------------------------------
--Premises
DROP TABLE IF EXISTS #Premise_Types
SELECT DISTINCT IDENTITY(INT, 1, 1) AS Premise_Type_ID, premisetype AS Premise_Type
INTO #Premise_Types
FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
--SELECT TOP 200 * FROM #Premise_Types


------------------------------------
--Handle Locations
DROP TABLE IF EXISTS #Offence_Coordinates
SELECT DISTINCT Lat, Long
INTO #Offence_Coordinates
FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
WHERE 1=1
   AND (ISNULL(TRY_CAST(Lat AS FLOAT),0) <> 0  OR ISNULL(TRY_CAST(Long AS FLOAT),0) <> 0)
--SELECT TOP 200 * FROM #Offence_Coordinates


DROP TABLE IF EXISTS #Postal_Codes_RAW
SELECT #Offence_Coordinates.Lat AS Latitude
   , #Offence_Coordinates.Long AS Longitude
   , D_Postal_Codes_Locations.Postal_Codes_Locations_ID
   , Postal_Code
   , TRY_CAST(TRY_CAST(Geography::STGeomFromText('POINT(' + CAST(#Offence_Coordinates.Long AS VARCHAR(30)) +' ' +  CAST(#Offence_Coordinates.Lat AS VARCHAR(30)) +')', 4617) AS Geography).STDistance (CAST(Geography::STGeomFromText('POINT(' + CAST(D_Postal_Codes_Locations.Longitude AS VARCHAR(30)) +' ' +  CAST(D_Postal_Codes_Locations.Latitude AS VARCHAR(30)) +')', 4617) AS Geography)) AS INT) AS Distance_Metres
INTO #Postal_Codes_RAW
FROM #Offence_Coordinates
CROSS JOIN (
   SELECT *
   FROM Data_Warehouse.dbo.D_Postal_Codes_Locations WITH(NOLOCK)
   WHERE 1=1
      AND Province_Name = 'Ontario'
      AND CMA_Name = 'Toronto'
      AND Latitude <> 0
      AND Longitude <> 0
) D_Postal_Codes_Locations
WHERE 1=1
   AND TRY_CAST(TRY_CAST(Geography::STGeomFromText('POINT(' + CAST(#Offence_Coordinates.Long AS VARCHAR(30)) +' ' +  CAST(#Offence_Coordinates.Lat AS VARCHAR(30)) +')', 4617) AS Geography).STDistance (CAST(Geography::STGeomFromText('POINT(' + CAST(D_Postal_Codes_Locations.Longitude AS VARCHAR(30)) +' ' +  CAST(D_Postal_Codes_Locations.Latitude AS VARCHAR(30)) +')', 4617) AS Geography)) AS INT) < 20000
--SELECT * FROM #Postal_Codes_RAW

DROP TABLE IF EXISTS #Postal_Codes
SELECT Latitude, Longitude, Postal_Code, Postal_Codes_Locations_ID, Distance_Metres
INTO #Postal_Codes
FROM (
   SELECT Latitude, Longitude, Postal_Code, Postal_Codes_Locations_ID, Distance_Metres
      , ROW_NUMBER() OVER (PARTITION BY Latitude, Longitude ORDER BY Distance_Metres) AS Distance_Metres_RANK
   FROM #Postal_Codes_RAW
) _postal_codes
WHERE 1=1
   AND _postal_codes.Distance_Metres_RANK = 1
--SELECT * FROM #Postal_Codes


------------------------------------
--Weather by Postal_Code
DROP TABLE IF EXISTS #Weather
SELECT D_Weather.Conformed_Date, #Postal_Codes.Postal_Code
   , Mean_Temp_C, Mean_Precipitation_mm
INTO #Weather
FROM [Data_Warehouse].[dbo].[D_Weather] WITH(NOLOCK)
INNER JOIN #Robbery_Dates ON #Robbery_Dates.Conformed_Date = D_Weather.Conformed_Date
INNER JOIN #Postal_Codes ON #Postal_Codes.Postal_Codes_Locations_ID = D_Weather.Postal_Codes_Locations_ID
WHERE 1=1
   AND From_Weather_History = 1
--SELECT * FROM #Weather ORDER BY 4


------------------------------------
--Time of Day
DROP TABLE IF EXISTS #Time_Of_Day
SELECT N-1 AS Time_Of_Day_ID
   , IIF(N-1 BETWEEN 0 AND 6, 'Early Morning', 
      IIF(N-1 BETWEEN 7 AND 11, 'Morning',
         IIF(N-1 BETWEEN 12 AND 16, 'Afternoon',
            IIF(N-1 BETWEEN 17 AND 19, 'Evening', 'Night'
            )
         )
      )
     ) AS Time_Of_Day
   , IIF(N-1 BETWEEN 0 AND 6 OR N-1 BETWEEN 19 AND 23, 'Darkness', 'Daylight') AS Sky_Light_Category
INTO #Time_Of_Day
FROM Data_Warehouse.config.Tally WITH(NOLOCK)
WHERE N BETWEEN 1 AND 24
--SELECT * FROM #Time_Of_Day


------------------------------------
--Neighbourhood Data
--SELECT *
--FROM BOGUS.dbo.toronto_neighbourhoods

DROP TABLE IF EXISTS #Neighbourhood
SELECT _hood.Hood_ID AS Neighbourhood_ID
   , Data_Warehouse.dbo.fnReplace_Random_Word(CAST(_hood.Neighbourhood AS VARCHAR(100)), '(', ')', '') AS Neighbourhood
   , [toronto_hood].PopulationCount
   , [toronto_hood].PopulationDensity_Per_SqKM
   , [toronto_hood].PopulationCount_0_14
   , [toronto_hood].PopulationCount_15_24
   , [toronto_hood].PopulationCount_25_54
   , [toronto_hood].PopulationCount_55_64
   , [toronto_hood].[PopulationCount_65+]
INTO #Neighbourhood
FROM BOGUS.[dbo].[toronto_hood]
INNER JOIN (
   SELECT DISTINCT Hood_ID, Neighbourhood
   FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
) _hood ON toronto_hood.Hood_ID = _hood.Hood_ID


--SELECT 
--WHERE Neighbourhood LIKE 'Anne%'



------------------------------------
------------------------------------
--Fact data
DROP TABLE IF EXISTS #Fact_Toronto_Robberies
SELECT Index_ AS Record_ID
   , CAST(occurrencedate AS DATETIME) + CAST(CAST(RIGHT('00' + CAST(occurrencehour AS VARCHAR(2)), 2) + ':00:00' AS TIME(0)) AS DATETIME) AS Occurrence_DateTime
   , CAST(reporteddate AS DATETIME) + CAST(CAST(RIGHT('00' + CAST(reportedhour AS VARCHAR(2)), 2) + ':00:00' AS TIME(0)) AS DATETIME) AS Reported_DateTime

   -- Measures
   , DATEDIFF(WEEK, CAST(occurrencedate AS DATE), CAST(reporteddate AS DATE)) AS Reported_After_Occurence_WEEKS
   , DATEDIFF(DAY, CAST(occurrencedate AS DATE), CAST(reporteddate AS DATE)) AS Reported_After_Occurence_DAYS
   , DATEDIFF(HOUR, CAST(occurrencedate AS DATETIME) + CAST(CAST(RIGHT('00' + CAST(occurrencehour AS VARCHAR(2)), 2) + ':00:00' AS TIME(0)) AS DATETIME), CAST(reporteddate AS DATETIME) + CAST(CAST(RIGHT('00' + CAST(reportedhour AS VARCHAR(2)), 2) + ':00:00' AS TIME(0)) AS DATETIME)) AS Reported_After_Occurence_HOURS

   -- Dimensions
   , CAST(occurrencedate AS DATE) AS Occurence_Date
   , occurrencehour AS Occurred_Time_Of_Day_ID
   , CAST(reporteddate AS DATE) AS Reported_Date
   , reportedhour AS Reported_Time_Of_Day_ID
   , Hood_ID AS Neighbourhood_ID
   , ucr_ext AS Offence_ID
   , #Premise_Types.Premise_Type_ID
   , #Postal_Codes.Postal_Code AS Occurrence_Postal_Code
   , LEFT(#Postal_Codes.Postal_Code, 3) AS Occurrence_FSA
   --, Data_Warehouse.dbo.fnReplace_Random_Word(CAST(Neighbourhood AS VARCHAR(100)), '(', ')', '') AS Neighbourhood
 --  , *
	--, Lat AS Latitude
	--, Long AS Longitude
--INTO #Fact_Toronto_Robberies
FROM [BOGUS].[dbo].[Toronto_Robberies] WITH(NOLOCK)
INNER JOIN #Premise_Types ON [Toronto_Robberies].premisetype = #Premise_Types.Premise_Type
LEFT  JOIN #Postal_Codes ON [Toronto_Robberies].lat = #Postal_Codes.Latitude
   AND [Toronto_Robberies].long = #Postal_Codes.Longitude
--LEFT  JOIN #Weather ON #Postal_Codes.Postal_Code = #Weather.Postal_Code
--   AND CAST(occurrencedate AS DATE) = #Weather.Conformed_Date
ORDER BY occurrencedate

--SELECT TOP 20 * 
--FROM Data_Warehouse.dbo.D_Times


