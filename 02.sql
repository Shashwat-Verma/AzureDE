-- Lab - SQL Pool - External Tables - CSV

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssw0rd@123';

-- Here we are using the Storage account key for authorization (Access Key)

CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
  IDENTITY = 'adlsdemo97',
  SECRET = 'IgWwisxE9bIfEgv5v521p5nE9gb2dX8rl9ZsFDiJAXRv4Y2n+IXderAkHeMJHYLbc1hOFiRPcwZT+AStzQuYeA==';

-- In the SQL pool, we can use Hadoop drivers to mention the source
-- abfss://containerName@storageAccountName.dfs.core.windows.net

CREATE EXTERNAL DATA SOURCE log_data
WITH (    LOCATION   = 'abfss://datasets@adlsdemo97.dfs.core.windows.net',
          CREDENTIAL = AzureStorageCredential,
          TYPE = HADOOP
)

CREATE EXTERNAL FILE FORMAT TextFileFormat WITH (  
      FORMAT_TYPE = DELIMITEDTEXT,  
    FORMAT_OPTIONS (  
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2))


CREATE EXTERNAL TABLE [logdata]
(
    [Id] [int] NULL,
	[Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [varchar](40) NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL
)
WITH (
 LOCATION = 'logdata/Log.csv',
    DATA_SOURCE = log_data,  
    FILE_FORMAT = TextFileFormat
)



SELECT * FROM logdata


SELECT [Operationname] , COUNT([Operationname]) as [Operation Count]
FROM logdata
GROUP BY [Operationname]
ORDER BY [Operation Count]