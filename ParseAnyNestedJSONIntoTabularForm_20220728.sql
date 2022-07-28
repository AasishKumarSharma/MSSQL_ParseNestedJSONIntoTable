-- =============================================
-- Author:	Aasish Kumar Sharma
-- Create date: 2022-03-02
-- Description:	Related MSSQL JSON Parser Engine. MSSQL Version 2017+
--				Parses JSON string (JSON data) from a JSON log table for each header (nested level) into a proper tabular form. 
--				Then upserts them into respective table.
-- Conditions: 	Nesting Level: Upto 10 levels (can be increased).
-- =============================================
	BEGIN TRY


		-- Sample JSON log table with sample data:
		--------------------------------------------		
		DECLARE @JSONLog TABLE
		(
			[JSONLogId] INT NOT NULL PRIMARY KEY,
			[Date] DATETIME NOT NULL DEFAULT(GETDATE()),
			[JsonData] NVARCHAR(MAX) NOT NULL DEFAULT(N'')
		)
		
		-- Insert sample JSON data.
		INSERT INTO @JSONLog(JSONLogId, JsonData) 
		VALUES(1, N'{
			"id": 2,
			"info": {
			  "name": "John",
			  "surname": "Smith"
			},
			"age": 25
		  }')
  
		  ,(2, N'{"INFO1": {"id": 1, "info": {"name": "John1", "surname": "Smith"}, "age": 25},
		  "INFO2": {"id": 2, "info": {"name": "Jane", "surname": "Smith", "skills": ["SQL", "C#", "Azure"]}, "dob": "2005-11-04T12:00:00", "has_friendly": true, "friendof": {"id": 3, "info": {"name": "John2", "surname": "Smith"}, "age": 25, "has_friendly": true, "friendof": {"id": 4, "info": {"name": "John3", "surname": "Smith"}, "age": 25, "has_friendly": true, "friendof": {"id": 5, "info": {"name": "John4", "surname": "Smith"}, "age": 25, "other_details":{"phone": "(03) 9555 7448", "website": "http://www.playballbasketball.com/"}, "has_friendly": true, "friendof": {"id": 6, "info": {"name": "John5", "surname": "Smith"}, "age": 25, "has_friendly": true, "friendof": {"id": 7, "info": {"name": "John6", "surname": "Smith"}, "age": 25, "has_friendly": true, "friendof": {"id": 8, "info": {"name": "John7", "surname": "Smith"}, "age": 25, "has_friendly": true, "friendof": {"id": 9, "info": {"name": "John8", "surname": "Smith"}, "age": 25, "has_friendly": false} } } } } } }  }  } ');
	
		-- Check JSON data.
		SELECT *,ISJSON(JsonData) AS [ISJSON] FROM @JSONLog;

		
		-- Create temporary table to insert parsed JSON data into tabular format.
		IF OBJECT_ID(N'tempdb..JSONDataTable') IS NOT NULL
		BEGIN
			DROP TABLE JSONDataTable
		END

		-- Parse JSON Data into tabular form.
		;WITH JSONParserCTE(
			[JSONLogId]
			, [Date] 
			, [JsonData]
			, [HeaderName]
			, [JSONPath]
			, [key]
			, [value]
			, [type]
			, [NestingLevel]
			, [HasNesting]
		)
		AS (
			SELECT
				[JSONLogId]
				, [Date] 
				, [JsonData] = MP.[JsonData]
				, [HeaderName] = CAST(J.[key] AS NVARCHAR(255)) 
				, [JSONPath] = CAST(CONCAT('$."', J.[key], '"') AS NVARCHAR(255))
				, [key] = CAST([key] AS NVARCHAR(255)) 
				, [value] = CAST([value] AS NVARCHAR(MAX)) 
				, [type] = CAST([type] AS INT) 
				, [NestingLevel] = CAST(0 AS INT) 
				, [HasNesting] = CAST(
					(CASE [type]
						WHEN 4 THEN 1 
						WHEN 5 THEN 1
						ELSE 0
					END) 
				AS BIT)  
			FROM
				@JSONLog AS MP
				CROSS APPLY OPENJSON(MP.JsonData) AS J --@json
			WHERE 
				ISJSON(mp.JsonData) > 0 			

			UNION ALL 

			SELECT  
				J.[JSONLogId]
				, J.[Date] 
				, J.[JsonData]
				, [HeaderName] = CAST((J.[HeaderName] + '_' + J1.[key]) AS NVARCHAR(255))  
				, [JSONPath] = CAST(CONCAT(J.[JSONPath], '."', J1.[key], '"') AS NVARCHAR(255)) 
				, CAST(J1.[key] AS NVARCHAR(255)) AS [key]
				, CAST(J1.[value] AS NVARCHAR(MAX)) AS [value]
				, CAST(J1.[type] AS INT) AS [type]
				, [NestingLevel] =  CAST([NestingLevel] AS NVARCHAR(255)) + CAST(1 AS INT) 
				, [HasNesting] = CAST(
					(CASE J1.[type]
						WHEN 4 THEN 1 
						WHEN 5 THEN 1
						ELSE 0
					END) 
				AS BIT)  
			FROM 
				JSONParserCTE AS J 
				CROSS APPLY OPENJSON(J.[JsonData], CAST('' + [JSONPath] AS NVARCHAR(255))) AS J1
			WHERE 
				J.[HasNesting] = 1 
				AND [NestingLevel] < 10	
		)

		-- Insert into temporary table.
		SELECT 
			[JSONLogId]
			, [Date] 
			, [ParsedRowNumber] = ROW_NUMBER() OVER(PARTITION BY [JSONLogId] ORDER BY [JSONLogId])
			, [HeaderName]
			, [JSONPath]
			, [key]
			, [value]
			, [type]
			, [NestingLevel]
			, [HasNesting]
			INTO #JSONDataTable
		FROM 
			JSONParserCTE AS J
		WHERE 
			[HasNesting] = 0 -- Remove rows with arrary and object datatype.
		 ORDER BY 
			[JSONLogId]
			, [RowNumber] 
		
		-- View JSON data in the table.
		SELECT 
			[JSONLogId]
			, [Date] 
			, [ParsedRowNumber]
			, [HeaderName]
			, [JSONPath]
			, [Key]
			, [Value]
			, [Type]
			, [NestingLevel]
			, [HasNesting]
		FROM 
			#JSONDataTable;
			
		
		-- Drop temporary table.
		IF OBJECT_ID(N'tempdb..JSONDataTable') IS NOT NULL
		BEGIN
			DROP TABLE JSONDataTable
		END

		

	END TRY
	
	BEGIN CATCH
		
		-- Report error number and error message.
		SELECT
			 ERROR_NUMBER() AS ErrorNumber,
			 ERROR_MESSAGE() AS ErrorMessage
		
	END CATCH




-- This is a reference table to understand the type field.
DECLARE @JSONParserDataType TABLE
(
	[JSONParserDataTypeId] [int] PRIMARY KEY NOT NULL,
	[JSONParserDataTypeName] [nvarchar](150) NOT NULL,
	[JSONParserFullDataTypeName] [nvarchar](150) NOT NULL DEFAULT (N''),
	[CastableToSQLServerDataType] [nvarchar](150) NOT NULL DEFAULT (N'')
); 

INSERT @JSONParserDataType ([JSONParserDataTypeId], [JSONParserDataTypeName], [JSONParserFullDataTypeName], [CastableToSQLServerDataType]) 
VALUES (0, N'null', N'Null_value', N'NULL'),
	(1, N'string', N'String_value', N'NVARCHAR(MAX)'),
	(2, N'number', N'DoublePrecisionFloatingPoint_value', N'NUMERIC(18,5)'),
	(3, N'true/false', N'BooleanTrue_value', N'BIT'),
	(4, N'array', N'Array_value', N'NVARCHAR(MAX)'),
	(5, N'object', N'Object_value', N'NVARCHAR(MAX)');

SELECT * FROM @JSONParserDataType;
