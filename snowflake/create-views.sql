CREATE OR REPLACE VIEW PROD.V_SURVEYS as 
WITH _JSON AS (
SELECT
    PARSE_JSON(RECORD_CONTENT) AS SRC
FROM STAGE.SURVEYS
),

_STAGE_TABLE AS (
SELECT DISTINCT
  REPLACE(SRC:"Data",'"','') AS TIMESTAMP,
  REPLACE(SRC:"IdSensore",'"','') AS ID_SENSORE,
  REPLACE(SRC:"Stato",'"','') AS STATO,
  REPLACE(SRC:"Valore",'"','') AS VALORE,
  REPLACE(SRC:"idOperatore",'"','') AS ID_OPERATORE
FROM _JSON SRC
),

_PROD_TABLE AS (
SELECT 
    TO_TIMESTAMP_NTZ(CASE
        WHEN TIMESTAMP = '' THEN '01/01/1900 00:00:00'
        ELSE TIMESTAMP
    END,'DD/MM/YYYY HH:MI:SS') AS TIMESTAMP,
    CASE
        WHEN ID_SENSORE='' THEN NULL
        ELSE ID_SENSORE
    END ::INT AS ID_SENSORE,
    CASE 
        WHEN STATO='' THEN NULL
        ELSE STATO
    END AS STATO,
    CASE 
        WHEN VALORE='' THEN NULL
        ELSE VALORE
    END::NUMERIC(38,2) AS VALORE,
    CASE
        WHEN ID_OPERATORE='' THEN NULL
        ELSE ID_OPERATORE
    END ::INT AS ID_OPERATORE
FROM _STAGE_TABLE)

SELECT 
    TIMESTAMP,
    ID_SENSORE,
    STATO,
    VALORE,
    ID_OPERATORE
FROM _PROD_TABLE;

CREATE OR REPLACE VIEW PROD.V_SURVEYS_STATION as 
WITH _JSON AS (
SELECT
    PARSE_JSON(RECORD_CONTENT) AS SRC
FROM STAGE.SURVEYS_STATION
),

_STAGE_TABLE AS (
SELECT DISTINCT
  REPLACE(SRC:"Comune",'"','') AS COMUNE,
  REPLACE(SRC:"DataStart",'"','') AS DATA_START,
  REPLACE(SRC:"DataStop",'"','') AS DATA_STOP,
  REPLACE(SRC:"IdSensore",'"','') AS ID_SENSORE,
  REPLACE(SRC:"Idstazione",'"','') AS ID_STATZIONE,
  REPLACE(SRC:"Limiti amministrativi 2014 delle province di Regione Lombardia",'"','') AS LIMITI_2014,
  REPLACE(SRC:"Limiti amministrativi 2015 delle province di Regione Lombardia",'"','') AS LIMITI_2015,
  REPLACE(SRC:"NomeStazione",'"','') AS NOME_STAZIONE,
  REPLACE(SRC:"NomeTipoSensore",'"','') AS NOME_TIPO_SENSORE,
  REPLACE(SRC:"Provincia",'"','') AS PROVINCIA,
  REPLACE(SRC:"Quota",'"','') AS QUOTA,
  REPLACE(SRC:"Storico",'"','') AS STORICO,
  REPLACE(SRC:"UTM_Est",'"','') AS UTM_EST,
  REPLACE(SRC:"UnitaMisura",'"','') AS UNITA_MISURA,
  REPLACE(SRC:"Utm_Nord",'"','') AS UTM_NORD,
  REPLACE(SRC:"lat",'"','') AS LAT,
  REPLACE(SRC:"lng",'"','') AS LONG,
  REPLACE(SRC:"location",'"','') AS LOCATION
FROM _JSON SRC
),

_PROD_TABLE AS (
SELECT 
    CASE 
        WHEN COMUNE='' THEN NULL
        ELSE COMUNE
    END AS COMUNE,
    TO_DATE(CASE
        WHEN DATA_START = '' THEN '01/01/1900'
        ELSE DATA_START
    END,'DD/MM/YYYY') AS DATA_START,
    TO_DATE(CASE
        WHEN DATA_STOP = '' THEN '31/12/9999'
        ELSE DATA_STOP
    END,'DD/MM/YYYY') AS DATA_STOP,
    CASE
        WHEN ID_SENSORE='' THEN NULL
        ELSE ID_SENSORE
    END ::INT AS ID_SENSORE,
    CASE 
        WHEN ID_STATZIONE='' THEN NULL
        ELSE ID_STATZIONE
    END::INT AS ID_STATZIONE,
    CASE 
        WHEN LIMITI_2014='' THEN NULL
        ELSE LIMITI_2014
    END::NUMERIC(38,2) AS LIMITI_2014,
    CASE
        WHEN LIMITI_2015='' THEN NULL
        ELSE LIMITI_2015
    END::NUMERIC(38,2) AS LIMITI_2015,
    CASE 
        WHEN NOME_STAZIONE='' THEN NULL
        ELSE NOME_STAZIONE
    END AS NOME_STAZIONE,
    CASE 
        WHEN NOME_TIPO_SENSORE='' THEN NULL
        ELSE NOME_TIPO_SENSORE
    END AS NOME_TIPO_SENSORE,
    CASE 
        WHEN PROVINCIA='' THEN NULL
        ELSE PROVINCIA
    END AS PROVINCIA,
    CASE 
        WHEN QUOTA='' THEN NULL
        WHEN QUOTA='NULL' THEN NULL
        ELSE QUOTA
    END::INT AS QUOTA,
    CASE 
        WHEN STORICO='' THEN NULL
        ELSE STORICO
    END AS STORICO,
    CASE 
        WHEN UTM_EST='' THEN NULL
        ELSE UTM_EST
    END AS UTM_EST,
    CASE 
        WHEN UNITA_MISURA='' THEN NULL
        ELSE UNITA_MISURA
    END AS UNITA_MISURA,
    CASE 
        WHEN UTM_NORD='' THEN NULL
        ELSE UTM_NORD
    END AS UTM_NORD,
    CASE 
        WHEN LAT='' THEN NULL
        ELSE LAT
    END AS LAT,
    CASE 
        WHEN LONG='' THEN NULL
        ELSE LONG
    END AS LONG,
    CASE 
        WHEN LOCATION='' THEN NULL
        ELSE LOCATION
    END AS LOCATION
FROM _STAGE_TABLE)

SELECT 
    COMUNE,
    DATA_START,
    DATA_STOP,
    ID_SENSORE,
    ID_STATZIONE,
    LIMITI_2014,
    LIMITI_2015,
    NOME_STAZIONE,
    NOME_TIPO_SENSORE,
    PROVINCIA,
    QUOTA,
    STORICO,
    UTM_EST,
    UNITA_MISURA,
    UTM_NORD,
    LAT,
    LONG,
    LOCATION
FROM _PROD_TABLE;

CREATE OR REPLACE VIEW PROD.V_PBI_SURVEYS as 
SELECT S.TIMESTAMP,
S.ID_SENSORE,
S.STATO,
S.VALORE,
S.ID_OPERATORE,
SST.COMUNE,
SST.DATA_START,
SST.DATA_STOP,
SST.ID_STATZIONE,
SST.LIMITI_2014,
SST.LIMITI_2015,
SST.NOME_STAZIONE,
SST.NOME_TIPO_SENSORE,
SST.PROVINCIA,
SST.QUOTA,
SST.STORICO,
SST.UTM_EST,
SST.UNITA_MISURA,
SST.UTM_NORD,
SST.LAT,
SST.LONG,
SST.LOCATION
FROM PROD.V_SURVEYS S
LEFT JOIN PROD.V_SURVEYS_STATION SST
    ON S.ID_SENSORE=SST.ID_SENSORE;

CREATE OR REPLACE VIEW PROD.V_PBI_MONOSSIDO_DI_CARBONIO as 
SELECT 
    TIMESTAMP, 
    ID_SENSORE, 
    STATO, 
    COMUNE, 
    VALORE, 
    AVG(VALORE) OVER (PARTITION BY ID_SENSORE ORDER BY TIMESTAMP) AS AVG_SENSORE,
    UNITA_MISURA, 
    LAT, 
    LONG, 
    LOCATION 
FROM PROD.V_PBI_SURVEYS
WHERE NOME_TIPO_SENSORE='Monossido di Carbonio'
AND VALORE<>-9999
ORDER BY TIMESTAMP DESC;

CREATE OR REPLACE VIEW PROD.V_PBI_PM10 AS 
SELECT 
    TIMESTAMP, 
    ID_SENSORE, 
    STATO, 
    COMUNE, 
    VALORE, 
    AVG(VALORE) OVER (PARTITION BY ID_SENSORE ORDER BY TIMESTAMP) AS AVG_SENSORE,
    UNITA_MISURA, 
    LAT, 
    LONG, 
    LOCATION 
FROM PROD.V_PBI_SURVEYS
WHERE NOME_TIPO_SENSORE='PM10 (SM2005)'
AND VALORE<>-9999
ORDER BY TIMESTAMP DESC;

CREATE OR REPLACE VIEW PROD.V_PBI_PM2_5 AS
SELECT 
    TIMESTAMP, 
    ID_SENSORE, 
    STATO, 
    COMUNE, 
    VALORE, 
    AVG(VALORE) OVER (PARTITION BY ID_SENSORE ORDER BY TIMESTAMP) AS AVG_SENSORE,
    UNITA_MISURA, 
    LAT, 
    LONG, 
    LOCATION 
FROM PROD.V_PBI_SURVEYS
WHERE NOME_TIPO_SENSORE='Particelle sospese PM2.5'
AND VALORE<>-9999
ORDER BY TIMESTAMP DESC;