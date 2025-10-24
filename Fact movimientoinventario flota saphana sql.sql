WITH "InventarioSerializable" AS (
    SELECT
        SRI1."WhsCode" AS "Cod_Bodega",
        OSRI."ItemCode" AS "Cod_Producto",
        OSRN."DistNumber" AS "Cod_SerieProducto",
        CASE WHEN SRI1."Direction" = 0 THEN 1 ELSE 0 END AS "Flag_Direccion",
        SRI1."BaseType" AS "Cod_TipoTransaccion",
        SRI1."BaseEntry" AS "Cod_Documento",
        1 AS "Cant_MovInventario",
        SRI1."DocDate" AS "Fecha_Movimiento",
        OSRI."SuppSerial" AS "Txt_SuppSerial",
        SRI1."CardCode" AS "Cod_SocioNegocio",
        SRI1."BaseLinNum" AS "BaseLinNum",
        1 AS "Flag_ProductoSerializable",
        1 AS "Flag_MovimientoSerializable",
        1 AS "Cant_SaldoInventarioBodegaProductoSerie"
    FROM "TECNOFAST"."SRI1"
    INNER JOIN "TECNOFAST"."OSRI" ON TRIM(OSRI."ItemCode") = TRIM(SRI1."ItemCode") AND OSRI."SysSerial" = SRI1."SysSerial"
    INNER JOIN "TECNOFAST"."OSRN" ON TRIM(SRI1."ItemCode") = TRIM(OSRN."ItemCode") AND OSRN."SysNumber" = SRI1."SysSerial"
    INNER JOIN "TECNOFAST"."OITM" ON OSRI."ItemCode" = OITM."ItemCode"
    WHERE OITM."ItemType" = 'I' AND (OITM."QryGroup1" = 'Y' OR OITM."QryGroup2" = 'Y')
),

"InventarioNoSerializable" AS (
    SELECT
        OINM."Warehouse" AS "Cod_Bodega",
        OINM."ItemCode" AS "Cod_Producto",
        OINM."ItemCode" AS "Cod_SerieProducto",
        CASE WHEN OINM."InQty" > 0 THEN 1 ELSE 0 END AS "Flag_Direccion",
        OINM."TransType" AS "Cod_TipoTransaccion",
        OINM."CreatedBy" AS "Cod_Documento",
        CASE WHEN OINM."InQty" > 0 THEN OINM."InQty" ELSE OINM."OutQty" END AS "Cant_MovInventario",
        OINM."DocDate" AS "Fecha_Movimiento",
        NULL AS "Txt_SuppSerial",
        OINM."CardCode" AS "Cod_SocioNegocio",
        OINM."DocLineNum" AS "BaseLinNum",
        0 AS "Flag_ProductoSerializable",
        0 AS "Flag_MovimientoSerializable",
        SUM(OINM."InQty" - OINM."OutQty") OVER (
            PARTITION BY OINM."ItemCode"
            ORDER BY OINM."DocDate", OINM."TransType", OINM."TransNum"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "Cant_SaldoInventarioBodegaProductoSerie"
    FROM "TECNOFAST"."OINM"
    INNER JOIN "TECNOFAST"."OITM" ON OINM."ItemCode" = OITM."ItemCode"
    WHERE OITM."U_Familia" = 2 AND OITM."ManSerNum" = 'N' AND OITM."SellItem" = 'N'
),

"MovimientoInventarioBase" AS (
    SELECT * FROM "InventarioSerializable"
    UNION ALL
    SELECT * FROM "InventarioNoSerializable"
),

"MovimientoInventarioFinal" AS (
    SELECT
        mi."Cod_Bodega",
        mi."Cod_SerieProducto",
        mi."Cod_Producto",
        mi."Flag_Direccion",
        mi."Cod_TipoTransaccion",
        mi."Cod_Documento",
        mi."Cant_MovInventario",
        TO_VARCHAR(mi."Fecha_Movimiento", 'YYYYMMDD') AS "Id_FechaMovimiento",
        COALESCE(OCRD."CardCode", '-2') AS "Cod_SocioNegocio",
        mi."Txt_SuppSerial",
        mi."Flag_MovimientoSerializable",
        CASE 
            WHEN mi."Cod_TipoTransaccion" = 59 THEN TO_VARCHAR(OIGN."DocDate", 'YYYYMMDD')
            WHEN mi."Cod_TipoTransaccion" = 60 THEN TO_VARCHAR(OIGE."DocDate", 'YYYYMMDD')
            WHEN mi."Cod_TipoTransaccion" = 67 THEN TO_VARCHAR(OWTR."DocDate", 'YYYYMMDD')
            ELSE TO_VARCHAR(NULL, 'YYYYMMDD')
        END AS "Id_FechaTransferencia",
        COALESCE(WTR1."ItemCode", '') AS "Cod_ProductoTransferencia",
        COALESCE(OIGE."FolioNum", OIGN."FolioNum", OWTR."FolioNum", 0) AS "Ext_NroFolioDocumento",
        COALESCE(TO_VARCHAR(OIGE."DocDate", 'YYYYMMDD'), TO_VARCHAR(OIGN."DocDate", 'YYYYMMDD'), TO_VARCHAR(OWTR."DocDate", 'YYYYMMDD')) AS "Ext_FechaDocumento",
        COALESCE(OIGE."DocSubType", OIGN."DocSubType", OWTR."DocSubType", '0') AS "Ext_SubTipoDocumento",
        mi."Flag_ProductoSerializable",
        CAST(mi."Cant_SaldoInventarioBodegaProductoSerie" AS DECIMAL(19,6)) AS "Cant_SaldoInventarioBodegaProductoSerie",
        CASE 
            WHEN mi."Cod_Bodega" LIKE 'ARR%' THEN 
                CASE 
                    WHEN mi."Cod_TipoTransaccion" = 59 THEN IGN1."U_COriginal"
                    WHEN mi."Cod_TipoTransaccion" = 60 THEN IGE1."U_COriginal"
                    WHEN mi."Cod_TipoTransaccion" = 67 THEN WTR1."U_COriginal"
                    ELSE 0
                END
            ELSE 0
        END AS "Ext_CodContrato",
        CASE 
            WHEN mi."Cod_Bodega" LIKE 'ARR%' THEN 
                CASE 
                    WHEN mi."Cod_TipoTransaccion" = 59 THEN ORDR_OIGN."DocNum"
                    WHEN mi."Cod_TipoTransaccion" = 60 THEN ORDR_OIGE."DocNum"
                    WHEN mi."Cod_TipoTransaccion" = 67 THEN ORDR_OWTR."DocNum"
                    ELSE 0
                END
            ELSE 0
        END AS "Ext_NroContrato",
        'Chile' AS "Cod_FilialEmpresa"
    FROM "MovimientoInventarioBase" mi
    LEFT JOIN "TECNOFAST"."OCRD" ON mi."Cod_SocioNegocio" = OCRD."CardCode"
    LEFT JOIN "TECNOFAST"."OIGE" ON mi."Cod_Documento" = OIGE."DocEntry" AND mi."Cod_TipoTransaccion" = 60
    LEFT JOIN "TECNOFAST"."IGE1" ON OIGE."DocEntry" = IGE1."DocEntry" AND IGE1."LineNum" = mi."BaseLinNum"
    LEFT JOIN "TECNOFAST"."ORDR" ORDR_OIGE ON IGE1."U_COriginal" = ORDR_OIGE."DocEntry"
    LEFT JOIN "TECNOFAST"."OIGN" ON mi."Cod_Documento" = OIGN."DocEntry" AND mi."Cod_TipoTransaccion" = 59
    LEFT JOIN "TECNOFAST"."IGN1" ON OIGN."DocEntry" = IGN1."DocEntry" AND IGN1."LineNum" = mi."BaseLinNum"
    LEFT JOIN "TECNOFAST"."ORDR" ORDR_OIGN ON IGN1."U_COriginal" = ORDR_OIGN."DocEntry"
    LEFT JOIN "TECNOFAST"."OWTR" ON mi."Cod_Documento" = OWTR."DocEntry" AND mi."Cod_TipoTransaccion" = 67
    LEFT JOIN "TECNOFAST"."WTR1" ON OWTR."DocEntry" = WTR1."DocEntry" AND WTR1."LineNum" = mi."BaseLinNum"
    LEFT JOIN "TECNOFAST"."ORDR" ORDR_OWTR ON WTR1."U_COriginal" = ORDR_OWTR."DocEntry"
)
SELECT * FROM "MovimientoInventarioFinal"
ORDER BY "Id_FechaMovimiento", "Cod_Producto", "Cod_SerieProducto";
