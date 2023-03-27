import pyodbc
import pandas as pd
import datetime
import sqlalchemy
import urllib

class SQLdatabase:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.quoted = 'DRIVER={SQL SERVER};' + 'SERVER={};'.format(self.server) \
                 + 'DATABASE={};Trusted_connection = yes'.format(self.database)

    def sql_conn(self):
        try:
            self.conn = pyodbc.connect(self.quoted)
            # print('Connected')
        except Exception as e:
            print('task is terminated')
        return self.conn

    def sql_read(self, read_statement):
        conn = self.sql_conn()
        SQL_Query = pd.read_sql_query(read_statement, conn)
        conn.close()
        # print(SQL_Query)
        return SQL_Query

    def sql_insert(self, database, df):
        conn = self.sql_conn()
        cursor = conn.cursor()
        insert_statement = """
                            INSERT INTO [dbo].[{}]
                            VALUES ( ?""".format(database)
        for col in range(1, df.shape[1]):
            insert_statement += ", ?"
        insert_statement += " )"
        try:
            for row in range(0, df.shape[0]):
                row_data = tuple(df.values[row])
                cursor.execute(insert_statement, row_data)
        except Exception as e:
            cursor.rollback()
            print('transaction rolled back')
            print(e)
        else:
            print('records inserted successfully')
            cursor.commit()
            cursor.close()
        finally:
            print('connection closed')
            conn.close()

    def sql_lastrowcheck(self, col, table):
        conn = self.sql_conn()
        read_statement = '''SELECT MAX({}) FROM [dbo].[{}]'''.format(col, table)
        SQL_Query = pd.read_sql_query(read_statement, conn)
        conn.close()
        return SQL_Query.values


if __name__ == '__main__':
    #define datavbase
    CTITBKK = SQLdatabase('STP-NB003\MSSQLSERVER22DEV', 'CTITBKK8') #origin database_BI
    replicate_BI = SQLdatabase('STP-NB003\MSSQLSERVER22DEV', 'replicate_CTITBKK8_BI') #replication of CTITBKK8_BI


    ## Job control group
    jmjm1_lastrow = replicate_BI.sql_lastrowcheck('id', 'JobMaster_SealCostAmt_Table')
    jmjm1_lastrow_skip = jmjm1_lastrow + 17 #skip first 17 rows
    jobmaster_statement =   '''SELECT TOP(10) jm1.JobNo
                                , ISNULL(jm1.JobType, '') as JobType
                                , ISNULL(jm1.JobDate, '') as JobDate
                                , ISNULL(jm1.ShipmentType, '') as ShipmentType
                                , convert(numeric(13,2), ISNULL(nl1.TotalSalesAmt, 0)) as TotalSalesAmt
                                , convert(numeric(13,2), ISNULL(nl1.TotalCostAmt, 0)) as TotalCostAmt
                                , convert(numeric(13,2), ISNULL(nl1.TotalPSalesAmt, 0)) as TotalPSalesAmt
                                , convert(numeric(13,2), ISNULL(nl1.TotalPCostAmt, 0)) as TotalPCostAmt
                                , convert(numeric(13,2), ISNULL(nl1.TotalCBM, 0)) as TotalCBM
                                , convert(numeric(13,2), ISNULL(nl1.TotalProfitAmt, 0)) as TotalProfitAmt
                                , convert(numeric(13,2), ISNULL(nl1.TotalProfitPercent, 0)) as TotalProfitPercent
                                , convert(numeric(13,2), ISNULL(nl1.TotalProfitPerCBM, 0)) as TotalProfitPerCBM
                                , jm1.CreateDateTime
                            FROM [dbo].[jmjm1] as jm1
                            LEFT JOIN [dbo].[jpnl1] as nl1 on jm1.JobNo = nl1.JobNo
                            WHERE jm1.StatusCode != 'DEL' AND jm1.JobNo NOT IN (
							    select TOP({}) jm.JobNo FROM [dbo].[jmjm1] as jm)'''.format(int(jmjm1_lastrow_skip))
    df_jobmaster = CTITBKK.sql_read(jobmaster_statement) #Jobmaster table + SealCostAmt table
    row_id = [] #create row id
    jobmaster_row = df_jobmaster.shape[0]
    for row in range(0,jobmaster_row):
        jmjm1_lastrow += 1
        row_id.append(int(jmjm1_lastrow))
    df_jobmaster['id'] = row_id #add new column
    jobmaster_datelist = ['CreateDateTime']
    for datefeature in jobmaster_datelist:
        df_jobmaster[datefeature] = df_jobmaster[datefeature].astype(object) \
            .where(df_jobmaster[datefeature].notnull(), None)
    if jobmaster_row > 0:
        print('jobmaster : {} new records'.format(jobmaster_row))
        print('writing data to JobMaster_SealCostAmt_Table')
        replicate_BI.sql_insert('JobMaster_SealCostAmt_Table', df_jobmaster)
    else:
        print('jobmaster : do not have a new record')

    jobnolist = tuple(df_jobmaster['JobNo'].values)
    jobcosting_statement =  '''SELECT JobNo
                                , LineItemNo
                                , ItemCode
                                , NULLIF(Description, '') as Description
                                , NULLIF(CustomerName, '') as CustomerName
                                , NULLIF(CostAccCode, '') as CostAccCode
                                , convert(numeric(13,2), ISNULL(CostAmt, 0)) as CostAmt
                                , NULLIF(CostCurrCode, '') as CostCurrCode
                                , NULLIF(CostVatCode, '') as CostVatCode
                            FROM [dbo].[jmjm2] WHERE JobNo IN {}'''.format(jobnolist)
    df_jobcosting = CTITBKK.sql_read(jobcosting_statement) #Job Costing table
    jobcosting_row = df_jobcosting.shape[0]
    if jobcosting_row > 0:
        print('jobcosting : new {} record'.format(jobcosting_row))
        print('writing data to JobCosting_Table')
        replicate_BI.sql_insert('JobCosting_Table', df_jobcosting)
    else:
        print('jobcosting : do not have a new record')


    ## Air export group
    aeaw1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'AirExportAWB_Aecs_Table')
    AirExportAWB_statement = '''SELECT TOP(10) aw1.TrxNo
                                , aw1.JobNo
                                , aw1.AwbNo
                                , NULLIF(cs1.AwbCode, '') as AwbCode
                                , NULLIF(cs1.AwbType, '') as AwbType
                                , NULLIF(aw1.MAwbNo, '') as MAwbNo
                                , NULLIF(aw1.AgentCode, '') as AgentCode
                                , NULLIF(aw1.AirportDestName, '') as AirportDestName
                                , NULLIF(aw1.DestName, '') as DestName
                                , NULLIF(aw1.ArrivalDateTime, '') as ArrivalDateTime
                                , NULLIF(aw1.BookingNo, '') as BookingNo
                                , NULLIF(aw1.ConsigneeName, '') as ConsigneeName
                                , NULLIF(aw1.CustomerName, '') as CustomerName
                                , NULLIF(aw1.DeliveryAgentName, '') as DeliveryAgentName
                                , NULLIF(aw1.DeliveryType, '') as DeliveryType
                                , NULLIF(aw1.ShipmentType, '') as ShipmentType
                                , NULLIF(aw1.ShipMode, '') as ShipMode
                                , NULLIF(aw1.ShipperName, '') as ShipperName
                                , convert(numeric(13,2), ISNULL(aw1.Dimension, 0)) as Dimension
                                , NULLIF(aw1.DimType, '') as DimType
                                , convert(numeric(13,2), ISNULL(aw1.Pcs, 0)) as Pcs
                                , NULLIF(cs1.ConfirmOnBord, '') as ConfirmOnBord
                                , NULLIF(cs1.FlightNo, '') as FlightNo
                                , NULLIF(cs1.CSFlightDate, '') as CSFlightDate
                                , convert(numeric(13,2), ISNULL(cs1.CSChargeWeight, 0)) as CSChargeWeight
                                , convert(numeric(13,2), ISNULL(cs1.KNetAmt, 0)) as KNetAmt
                                , convert(numeric(13,2), ISNULL(cs1.LAgentSellAmt, 0)) as LAgentSellAmt
                                , convert(numeric(13,2), ISNULL(cs1.MDiffForVat, 0)) as MDiffForVat
                                , convert(numeric(13,2), ISNULL(cs1.NAdjCost, 0)) as NAdjCost
                                , convert(numeric(13,2), ISNULL(cs1.OAdjSell, 0)) as OAdjSell
                                , NULLIF(cs1.VoucherNo, '') as VoucherNo
                            FROM [dbo].[aeaw1] as aw1
                            LEFT JOIN [dbo].[Aecs1] as cs1 on aw1.AwbNo = cs1.AwbNo
                            WHERE aw1.TrxNo > {} ORDER BY aw1.TrxNo '''.format(int(aeaw1_lastrow))
    df_AirExportAWB = CTITBKK.sql_read(AirExportAWB_statement) #Air Export AWB table
    df_AirExportAWB_rmd = df_AirExportAWB.drop_duplicates(subset=['JobNo'], keep='last')
    df_AirExportAWB_rmd = df_AirExportAWB_rmd.drop_duplicates(subset=['AwbNo'], keep='last')
    AirExportAWB_datelist = ['ArrivalDateTime', 'CSFlightDate']
    for datefeature in AirExportAWB_datelist:
        df_AirExportAWB_rmd[datefeature] = df_AirExportAWB_rmd[datefeature].astype(object) \
            .where(df_AirExportAWB_rmd[datefeature].notnull(), None)
    AirExportAWB_row = df_AirExportAWB_rmd.shape[0]
    if AirExportAWB_row > 0:
        print('AirExportAWB : {} new records'.format(AirExportAWB_row))
        print('writing data to AirExportAWB_Aecs_Table')
        replicate_BI.sql_insert('AirExportAWB_Aecs_Table', df_AirExportAWB_rmd)
    else:
        print('AirExportAWB : do not have a new record')

    AEawblist = tuple(df_AirExportAWB_rmd['AwbNo'].values)
    AEDimension_statement = '''SELECT AwbNo
                                , LineItemNo
                                , convert(numeric(13,2), ISNULL(GrossWeight, 0)) as GrossWeight
                                , convert(numeric(13,2), ISNULL(Dimension, 0)) as Dimension
                                , convert(numeric(13,2), ISNULL(Height, 0)) as Height
                                , convert(numeric(13,2), ISNULL(Length, 0)) as Length
                                , convert(numeric(13,2), ISNULL(Width, 0)) as Width
                                , convert(numeric(13,2), ISNULL(Pcs, 0)) as Pcs
                            FROM [dbo].[aeaw2] WHERE AwbNo IN {}'''.format(AEawblist)
    df_AEDimension = CTITBKK.sql_read(AEDimension_statement) #Air Export AWB Dimension Table
    AEDimension_row = df_AEDimension.shape[0]
    if AEDimension_row > 0:
        print('AEDimension : {} new records'.format(AEDimension_row))
        print('writing data to AirExportAWBDimension_Table')
        replicate_BI.sql_insert('AirExportAWBDimension_Table', df_AEDimension)
    else:
        print('AEDimension : do not have a new record')


    ## Air Import Group
    aiaw1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'AirImportAWB_Table')
    AirImportAWB_statement =    '''SELECT TOP(10) TrxNo
                                    , JobNo
                                    , AwbNo
                                    , NULLIF(MAwbNo, '') as MAwbNo
                                    , NULLIF(HAwbNo, '') as HAwbNo
                                    , NULLIF(ArrivalDateTime, '') as ArrivalDateTime
                                    , convert(numeric(13,2), ISNULL(AvailablePcs, 0)) as AvailablePcs
                                    , convert(numeric(13,2), ISNULL(ChargeWeight, 0)) as ChargeWeight
                                    , NULLIF(CommodityDescription, '') as CommodityDescription
                                    , NULLIF(ConsigneeName, '') as ConsigneeName
                                    , NULLIF(CustomerName, '') as CustomerName
                                    , NULLIF(DeliveryAgentName, '') as DeliveryAgentName
                                    , NULLIF(JobDate, '') as JobDate
                                    , NULLIF(OriginName, '') as OriginName
                                    , NULLIF(ShipmentType, '') as ShipmentType
                                    , NULLIF(ShipMode, '') as ShipMode
                                    , NULLIF(ShipperName, '') as ShipperName
                                FROM [dbo].[aiaw1]
                                WHERE TrxNo > {} ORDER BY TrxNo '''.format(int(aiaw1_lastrow))
    df_AirImportAWB = CTITBKK.sql_read(AirImportAWB_statement) #Air Import AWB table
    df_AirImportAWB_rmd = df_AirImportAWB.drop_duplicates(subset=['JobNo'], keep='last')
    AirImportAWB_datelist = ['ArrivalDateTime', 'JobDate']
    for datefeature in AirImportAWB_datelist:
        df_AirImportAWB_rmd[datefeature] = df_AirImportAWB_rmd[datefeature].astype(object) \
            .where(df_AirImportAWB_rmd[datefeature].notnull(), None)
    AirImportAWB_row = df_AirImportAWB_rmd.shape[0]
    if AirImportAWB_row > 0:
        print('AirImportAWB : {} new records'.format(AirImportAWB_row))
        print('writing data to AirImportAWB_Table')
        replicate_BI.sql_insert('AirImportAWB_Table', df_AirImportAWB_rmd)
    else:
        print('AirImportAWB : do not have a new record')


    ## Sea export group
    sebl1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'SeaExportBL_SeaExportBooking_Table')
    SeaExportBL_statement =     '''SELECT TOP(10) bl1.TrxNo
                                    , bl1.JobNo
                                    , NULLIF(bl1.BookingNo, '') as BookingNo
                                    , NULLIF(bl1.BlNo, '') as BlNo
                                    , NULLIF(bl1.BlIssuePlace, '') as BlIssuePlace
                                    , NULLIF(bl1.BlIssueDate, '') as BlIssueDate
                                    , NULLIF(bl1.OBLNo, '') as OBLNo
                                    , NULLIF(bl1.OriginalBlNo, '') as OriginalBlNo
                                    , NULLIF(bl1.BookingContainer, '') as BookingContainer
                                    , NULLIF(bk1.BookingDateTime, '') as BookingDateTime
                                    , NULLIF(bk1.BookingCustomerName, '') as BookingCustomerName
                                    , NULLIF(bk1.BookingSeqNo, '') as BookingSeqNo
                                    , NULLIF(bl1.CommodityDescription, '') as CommodityDescription
                                    , NULLIF(bl1.ConsigneeName, '') as ConsigneeName
                                    , NULLIF(bl1.CustomerName, '') as CustomerName
                                    , NULLIF(bl1.DeliveryAgentName, '') as DeliveryAgentName
                                    , NULLIF(bk1.DeliveryType, '') as DeliveryType
                                    , NULLIF(bl1.DestCargoType, '') as DestCargoType
                                    , NULLIF(bl1.OriginName, '') as OriginName
                                    , NULLIF(bl1.PortOfLoadingName, '') as PortOfLoadingName
                                    , NULLIF(bl1.DestName, '') as DestName
                                    , NULLIF(bl1.PortOfDischargeName, '') as PortOfDischargeName
                                    , NULLIF(bl1.PlaceOfDelivery, '') as PlaceOfDelivery
                                    , NULLIF(bk1.EtaDate, '') as EtaDate
                                    , NULLIF(bl1.EtdDate, '') as EtdDate
                                    , NULLIF(bl1.PlaceOfReceipt, '') as PlaceOfReceipt
                                    , NULLIF(bl1.MotherVesselName, '') as MotherVesselName
                                    , NULLIF(bl1.VesselName, '') as VesselName
                                    , NULLIF(bl1.ShipmentType, '') as ShipmentType
                                    , NULLIF(bl1.ShipMode, '') as ShipMode
                                    , NULLIF(bl1.ShipperName, '') as ShipperName
                                    , NULLIF(bl1.VoyageNo, '') as VoyageNo
                                    , NULLIF(bl1.YardName, '') as YardName
                                FROM [dbo].[sebl1] as bl1
                                LEFT JOIN [dbo].[sebk1] as bk1 on bl1.BookingNo = bk1.BookingNo
                                WHERE bl1.TrxNo > {} ORDER BY bl1.TrxNo '''.format(int(sebl1_lastrow))
    df_SeaExportBL = CTITBKK.sql_read(SeaExportBL_statement) #Sea Export B/L + Sea Export Booking table
    df_SeaExportBL_rmd = df_SeaExportBL.drop_duplicates(subset=['JobNo'], keep='last')
    df_SeaExportBL_rmd = df_SeaExportBL_rmd.drop_duplicates(subset=['BlNo'], keep='last')
    SeaExportBL_datelist = ['BlIssueDate', 'BookingDateTime', 'EtaDate', 'EtdDate']
    for datefeature in SeaExportBL_datelist:
        df_SeaExportBL_rmd[datefeature] = df_SeaExportBL_rmd[datefeature].astype(object)\
            .where(df_SeaExportBL_rmd[datefeature].notnull(), None)
    SeaExportBL_row = df_SeaExportBL_rmd.shape[0]
    if SeaExportBL_row > 0:
        print('SeaExportBL : {} new records'.format(SeaExportBL_row))
        print('writing data to SeaExportBL_SeaExportBooking_Table')
        replicate_BI.sql_insert('SeaExportBL_SeaExportBooking_Table', df_SeaExportBL_rmd)
    else:
        print('SeaExportBL : do not have a new record')

    SEbllist = tuple(df_SeaExportBL_rmd['BlNo'].values)
    SECargoDetail_statement =   '''SELECT NULLIF(BlNo, '') as  BlNo
                                    , LineItemNo
                                    , NULLIF(ContainerNo, '') as ContainerNo
                                    , NULLIF(SealNo, '') as SealNo
                                    , NULLIF(ContainerType, '') as ContainerType
                                    , convert(numeric(13,2), ISNULL(GrossWeight, 0)) as GrossWeight
                                    , convert(numeric(13,2), ISNULL(Pcs, 0)) as Pcs
                                FROM [dbo].[sebl2] WHERE BlNo IN {} AND NULLIF(BlNo, '') IS NOT NULL'''.format(SEbllist)
    df_SECargoDetail = CTITBKK.sql_read(SECargoDetail_statement) #Sea Export B/L Cargo Detail table
    SECargoDetail_row = df_SECargoDetail.shape[0]
    if SECargoDetail_row > 0:
        print('SECargoDetail : {} new records'.format(SECargoDetail_row))
        print('writing data to SeaExportBLCargoDetail_Table')
        replicate_BI.sql_insert('SeaExportBLCargoDetail_Table', df_SECargoDetail)
    else:
        print('SECargoDetail : do not have a new record')


    ## sea import group
    sibl1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'SeaImportB_Table')
    SeaImportBL_statement =     '''SELECT TOP(20) TrxNo
                                    , JobNo
                                    , NULLIF(BlNo, '') as BlNo
                                    , NULLIF(HBlNo, '') as HBlNo
                                    , NULLIF(OBlNo, '') as OBlNo
                                    , NULLIF(CargoType, '') as CargoType
                                    , NULLIF(CommodityCode, '') as CommodityCode
                                    , NULLIF(CommodityDescription, '') as CommodityDescription
                                    , NULLIF(ConsigneeName, '') as ConsigneeName
                                    , NULLIF(CustomerName, '') as CustomerName
                                    , NULLIF(DeliveryOrderReadyRemark, '') as DeliveryOrderReadyRemark
                                    , NULLIF(OriginName, '') as OriginName
                                    , NULLIF(PortOfLoadingName, '') as PortOfLoadingName
                                    , NULLIF(DestName, '') as DestName
                                    , NULLIF(PortOfDischargeName, '') as PortOfDischargeName
                                    , NULLIF(PlaceOfDelivery, '') as PlaceOfDelivery
                                    , NULLIF(EtaDate, '') as EtaDate
                                    , NULLIF(EtdDate, '') as EtdDate
                                    , NULLIF(PlaceOfReceipt, '') as PlaceOfReceipt
                                    , convert(numeric(13,2), ISNULL(MasterGrossWeight, 0)) as MasterGrossWeight
                                    , convert(numeric(13,2), ISNULL(MasterPcs, 0)) as MasterPcs
                                    , convert(numeric(13,2), ISNULL(MasterVolume, 0)) as MasterVolume
                                    , NULLIF(MotherVesselName, '') as MotherVesselName
                                    , NULLIF(MotherVoyage, '') as MotherVoyage
                                    , NULLIF(NotifyName, '') as NotifyName
                                    , NULLIF(ShipmentType, '') as ShipmentType
                                    , NULLIF(ShipMode, '') as ShipMode
                                    , NULLIF(ShippinglineCode, '') as ShippinglineCode
                                    , NULLIF(VesselName, '') as VesselName
                                    , NULLIF(VoyageNo, '') as VoyageNo
                                 FROM [dbo].[sibl1]
                                 WHERE TrxNo > {} ORDER BY TrxNo '''.format(int(sibl1_lastrow))
    df_SeaImportBL = CTITBKK.sql_read(SeaImportBL_statement) #Sea Import B/L Table
    df_SeaImportBL_rmd = df_SeaImportBL.drop_duplicates(subset=['JobNo'], keep='last')
    SeaImportBL_datelist = ['EtaDate', 'EtdDate']
    for datefeature in SeaImportBL_datelist:
        df_SeaImportBL_rmd[datefeature] = df_SeaImportBL_rmd[datefeature].astype(object) \
            .where(df_SeaImportBL_rmd[datefeature].notnull(), None)
    SeaImportBL_row = df_SeaImportBL_rmd.shape[0]
    if SeaImportBL_row > 0:
        print('SeaImportBL : {} new records'.format(SeaImportBL_row))
        print('writing data to SeaImportB_Table')
        replicate_BI.sql_insert('SeaImportB_Table', df_SeaImportBL_rmd)
    else:
        print('SeaImportBL : do not have a new record')

    SItrxlist = tuple(df_SeaImportBL_rmd['TrxNo'].values)
    SICargoDetail_statemant =   '''SELECT TrxNo
                                    , LineItemNo
                                    , NULLIF(GoodsDescription01, '') as GoodsDescription01
                                    , NULLIF(ContainerNo, '') as ContainerNo
                                    , NULLIF(ContainerType, '') as ContainerType
                                    , NULLIF(SealNo, '') as SealNo
                                 FROM [dbo].[sibl2] WHERE TrxNo in {} ORDER BY TrxNo'''.format(SItrxlist)
    df_SICargoDetail = CTITBKK.sql_read(SICargoDetail_statemant) #Sea Import B/L Cargo Detail Table
    SICargoDetail_row = df_SICargoDetail.shape[0]
    if SICargoDetail_row > 0:
        print('SICargoDetail : {} new records'.format(SICargoDetail_row))
        print('writing data to SeaImportBLCargoDetail_Table')
        replicate_BI.sql_insert('SeaImportBLCargoDetail_Table', df_SICargoDetail)
    else:
        print('SICargoDetail : do not have a new record')


    ## Custom privilege of air export and sea export group
    sacb1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'CustomPrivilege_AE_SE_Table')
    BookingCustom_Statement =   '''SELECT TOP(10) TrxNo
                                    , BookingNo
                                    , NULLIF(CustomsPrivilege, '') as CustomsPrivilege
                                    , NULLIF(PaperlessTerminal, '') as PaperlessTerminal
                                    , NULLIF(ApproveBy, '') as ApproveBy
                                    , NULLIF(ApproveDateTime, '') as ApproveDateTime
                                 FROM [dbo].[sacb1]
                                 WHERE BookingNo IS NOT NULL AND TrxNo > {} ORDER BY TrxNo '''.format(int(sacb1_lastrow))
    df_BookingCustom = CTITBKK.sql_read(BookingCustom_Statement) #Custom privilege of air export and sea export  table
    df_BookingCustom_rmd = df_BookingCustom.drop_duplicates(subset=['BookingNo'], keep='last')
    BookingCustom_datelist = ['ApproveDateTime']
    for datefeature in BookingCustom_datelist:
        df_BookingCustom_rmd[datefeature] = df_BookingCustom_rmd[datefeature].astype(object) \
            .where(df_BookingCustom_rmd[datefeature].notnull(), None)
    BookingCustom_row = df_BookingCustom_rmd.shape[0]
    if BookingCustom_row > 0:
        print('BookingCustom : {} new records'.format(BookingCustom_row))
        print('writing data to CustomPrivilege_AE_SE_Table')
        replicate_BI.sql_insert('CustomPrivilege_AE_SE_Table', df_BookingCustom_rmd)
    else:
        print('BookingCustom : do not have a new record')


    ## Purchase group
    plvi1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'VendorInvoice_plvx_Table')
    VendorInvoice_statement =   '''SELECT TOP(10) vi1.TrxNo
                                    , vi1.RefNo
                                    , vi1.JobNo
                                    , NULLIF(vi1.InvoiceNo, '') as InvoiceNo
                                    , NULLIF(vi1.ApplyToInvoice, '') as ApplyToInvoice
                                    , NULLIF(vi1.AccCode, '') as AccCode
                                    , NULLIF(vi1.BankCode, '') as BankCode
                                    , NULLIF(vi1.VatCode, '') as VatCode
                                    , convert(numeric(13,2), ISNULL(vi1.VatAmt, 0)) as VatAmt
                                    , convert(numeric(13,2), ISNULL(vi1.InvoiceAndVatLocalAmt, 0)) as InvoiceAndVatLocalAmt
                                    , NULLIF(vi1.InvoiceDate, '') as InvoiceDate
                                    , NULLIF(vi1.VoucherNo, '') as VoucherNo
                                    , NULLIF(vi1.VoucherType, '') as VoucherType
                                    , NULLIF(vx1.VendorVatReceiptNo, '') as VendorVatReceiptNo
                                    , NULLIF(vx1.VendorName, '') as VendorName
                                    , convert(numeric(13,2), ISNULL(vx1.PaidAmt, 0)) as PaidAmt
                                    , convert(numeric(13,2), ISNULL(vx1.TotalExmAmt, 0)) as TotalExmAmt
                                    , convert(numeric(13,2), ISNULL(vx1.TotalStdAmt, 0)) as TotalStdAmt
                                    , convert(numeric(13,2), ISNULL(vx1.TotalVatAmt, 0)) as TotalVatAmt
                                    , convert(numeric(13,2), ISNULL(vx1.TotalZeroAmt, 0)) as TotalZeroAmt
                                    , NULLIF(vx1.CurrCode, '') as CurrCode
                                    , NULLIF(vx1.PayRefNo, '') as PayRefNo
                                FROM [dbo].[plvi1] as vi1
                                LEFT JOIN [dbo].[plvx1] as vx1 on vi1.RefNo = vx1.RefNo
                                WHERE vi1.TrxNo > {} ORDER BY vi1.TrxNo '''.format(int(plvi1_lastrow))
    df_VendorInvoice = CTITBKK.sql_read(VendorInvoice_statement) #Vendor Invoice table
    df_VendorInvoice_rmd = df_VendorInvoice.drop_duplicates(subset=['RefNo'], keep='last')
    VendorInvoice_datelist = ['InvoiceDate']
    for datefeature in VendorInvoice_datelist:
        df_VendorInvoice_rmd[datefeature] = df_VendorInvoice_rmd[datefeature].astype(object) \
            .where(df_VendorInvoice_rmd[datefeature].notnull(), None)
    VendorInvoice_row = df_VendorInvoice_rmd.shape[0]
    if VendorInvoice_row > 0:
        print('VendorInvoice : {} new records'.format(VendorInvoice_row))
        print('writing data to VendorInvoice_plvx_Table')
        replicate_BI.sql_insert('VendorInvoice_plvx_Table', df_VendorInvoice_rmd)
    else:
        print('VendorInvoice : do not have a new record')

    VItrxlist = tuple(df_VendorInvoice_rmd['TrxNo'].values)
    VIDetail_statemant =   '''SELECT TrxNo
                                , LineItemNo
                                , NULLIF(ItemCode, '') as ItemCode
                                , NULLIF(Description, '') as Description
                                , convert(numeric(13,2), ISNULL(Amt, 0)) as Amt
                                , NULLIF(CurrCode, '') as CurrCode
                                , convert(numeric(13,2), ISNULL(LocalAmt, 0)) as LocalAmt
                                , convert(numeric(13,2), ISNULL(Qty, 0)) as Qty
                                , convert(numeric(13,2), ISNULL(VatAmt, 0)) as VatAmt
                                , NULLIF(VatCode, '') as VatCode
                            FROM [dbo].[plvi2] WHERE TrxNo in {} ORDER BY TrxNo'''.format(VItrxlist)
    df_VIDetail = CTITBKK.sql_read(VIDetail_statemant) #Vendor Invoice Detail table
    VIDetail_row = df_VIDetail.shape[0]
    if VIDetail_row > 0:
        print('VIDetail : {} new records'.format(VIDetail_row))
        print('writing data to VendorInvoice_Detail_Table')
        replicate_BI.sql_insert('VendorInvoice_Detail_Table', df_VIDetail)
    else:
        print('VIDetail : do not have a new record')

    ## Credit Invoice group
    ivcr1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'CreditInvoice_Table')
    CreditInvoice_statement =   '''SELECT TOP(10) TrxNo
                                    , RefNo
                                    , NULLIF(JobNo, '') as JobNo
                                    , NULLIF(InvoiceNo, '') as InvoiceNo
                                    , NULLIF(ApplyToInvoice, '') as ApplyToInvoice
                                    , NULLIF(AccCode, '') as AccCode
                                    , NULLIF(BankCode, '') as BankCode
                                    , NULLIF(AwbOrBlNo, '') as AwbOrBlNo
                                    , NULLIF(BillingPartyCode, '') as BillingPartyCode
                                    , NULLIF(CustomerName, '') as CustomerName
                                    , NULLIF(OriginName, '') as OriginName
                                    , NULLIF(PortOfLoadingName, '') as PortOfLoadingName
                                    , NULLIF(DestName, '') as DestName
                                    , NULLIF(PortOfDischargeName, '') as PortOfDischargeName
                                    , NULLIF(EtaDate, '') as EtaDate
                                    , NULLIF(EtdDate, '') as EtdDate
                                    , convert(numeric(13,2), ISNULL(Pcs, 0)) as Pcs
                                    , convert(numeric(13,2), ISNULL(InvoiceAndVatLocalAmt, 0)) as InvoiceAndVatLocalAmt
                                    , convert(numeric(13,2), ISNULL(TotalExmAmt, 0)) as TotalExmAmt
                                    , convert(numeric(13,2), ISNULL(TotalLocalVatAmt, 0)) as TotalLocalVatAmt
                                    , convert(numeric(13,2), ISNULL(TotalStdAmt, 0)) as TotalStdAmt
                                    , convert(numeric(13,2), ISNULL(TotalZeroAmt, 0)) as TotalZeroAmt
                                    , NULLIF(ShipperCode, '') as ShipperCode
                                    , NULLIF(VesselName, '') as VesselName
                                FROM [dbo].[ivcr1]
                                WHERE TrxNo > {} ORDER BY TrxNo '''.format(int(ivcr1_lastrow))
    df_CreditInvoice = CTITBKK.sql_read(CreditInvoice_statement) #Credit Invoice table
    df_CreditInvoice_rmd = df_CreditInvoice.drop_duplicates(subset=['RefNo'], keep='last')
    CreditInvoice_datelist = ['EtaDate', 'EtdDate']
    for datefeature in CreditInvoice_datelist:
        df_CreditInvoice_rmd[datefeature] = df_CreditInvoice_rmd[datefeature].astype(object) \
            .where(df_CreditInvoice_rmd[datefeature].notnull(), None)
    CreditInvoice_row = df_CreditInvoice_rmd.shape[0]
    if CreditInvoice_row > 0:
        print('CreditInvoice : {} new records'.format(CreditInvoice_row))
        print('writing data to CreditInvoice_Table')
        replicate_BI.sql_insert('CreditInvoice_Table', df_CreditInvoice_rmd)
    else:
        print('CreditInvoice : do not have a new record')

    CItrxlist = tuple(df_CreditInvoice_rmd['TrxNo'].values)
    CIDetail_statemant =    '''SELECT TrxNo
                                , LineItemNo
                                , NULLIF(ItemCode, '') as ItemCode
                                , NULLIF(Description, '') as Description
                                , NULLIF(BillTypeFlag, '') as BillTypeFlag
                                , convert(numeric(13,2), ISNULL(LocalAmt, 0)) as LocalAmt
                                , convert(numeric(13,2), ISNULL(Qty, 0)) as Qty
                                , NULLIF(VatCode, '') as VatCode
                            FROM [dbo].[ivcr2] WHERE TrxNo in {} ORDER BY TrxNo'''.format(CItrxlist)
    df_CIDetail = CTITBKK.sql_read(CIDetail_statemant) #Credit Invoice Detail table
    CIDetail_row = df_CIDetail.shape[0]
    if CIDetail_row > 0:
        print('CIDetail : {} new records'.format(CIDetail_row))
        print('writing data to CreditInvoice_Detail_Table')
        replicate_BI.sql_insert('CreditInvoice_Detail_Table', df_CIDetail)
    else:
        print('CIDetail : do not have a new record')

    slcr1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'CustomerReceipt_Table')
    CustomerReceipt_statement = '''SELECT TOP(10) TrxNo
                                    , RefNo
                                    , NULLIF(BankAccCode, '') as BankAccCode
                                    , NULLIF(BankCode, '') as BankCode
                                    , NULLIF(BillCollectorCode, '') as BillCollectorCode
                                    , NULLIF(ChequeNo, '') as ChequeNo
                                    , NULLIF(CustomerName, '') as CustomerName
                                    , NULLIF(PayMode, '') as PayMode
                                    , NULLIF(ReceiptNo, '') as ReceiptNo
                                    , NULLIF(ReceiptType, '') as ReceiptType
                                    , convert(numeric(13,2), ISNULL(ReceiptLocalAmt, 0)) as ReceiptLocalAmt
                                    , NULLIF(ReceiptDate, '') as ReceiptDate
                                FROM [dbo].[slcr1]
                                WHERE TrxNo > {} ORDER BY TrxNo '''.format(int(slcr1_lastrow))
    df_CustomerReceipt = CTITBKK.sql_read(CustomerReceipt_statement) #Customer Receipt table
    df_CustomerReceipt_rmd = df_CustomerReceipt.drop_duplicates(subset=['RefNo'], keep='last')
    CustomerReceipt_datelist = ['ReceiptDate']
    for datefeature in CustomerReceipt_datelist:
        df_CustomerReceipt_rmd[datefeature] = df_CustomerReceipt_rmd[datefeature].astype(object) \
            .where(df_CustomerReceipt_rmd[datefeature].notnull(), None)
    CustomerReceipt_row = df_CustomerReceipt_rmd.shape[0]
    if CustomerReceipt_row > 0:
        print('CustomerReceipt : {} new records'.format(CustomerReceipt_row))
        print('writing data to CustomerReceipt_Table')
        replicate_BI.sql_insert('CustomerReceipt_Table', df_CustomerReceipt_rmd)
    else:
        print('CustomerReceipt : do not have a new record')

    CRtrxlist = tuple(df_CustomerReceipt_rmd['TrxNo'].values)
    CRDetail_statement =    '''SELECT TrxNo
                                , LineItemNo
                                , NULLIF(Description, '') as Description
                                , NULLIF(AccCode, '') as AccCode
                                , NULLIF(PaidInvoiceNo, '') as PaidInvoiceNo
                                , convert(numeric(13,2), ISNULL(PaidInvoiceTrxNo, 0)) as PaidInvoiceTrxNo
                                , convert(numeric(13,2), ISNULL(ReceiptLocalAmt, 0)) as ReceiptLocalAmt
                            FROM [dbo].[slcr2] WHERE TrxNo in {} ORDER BY TrxNo'''.format(CRtrxlist)
    df_CRDetail = CTITBKK.sql_read(CRDetail_statement) #Customer Receipt Detail table
    CRDetail_row = df_CRDetail.shape[0]
    if CRDetail_row > 0:
        print('CRDetail : {} new records'.format(CRDetail_row))
        print('writing data to CustomerReceipt_Detail_Table')
        replicate_BI.sql_insert('CustomerReceipt_Detail_Table', df_CRDetail)
    else:
        print('CRDetail : do not have a new record')

    ## Sales Ledger Transaction group
    sltx1_lastrow = replicate_BI.sql_lastrowcheck('TrxNo', 'SalesLedgerTransaction_Table')
    SalesLedgerTransaction_statement =  '''SELECT TOP(10) TrxNo
                                            , RefNo
                                            , NULLIF(CustomerName, '') as CustomerName
                                            , NULLIF(InvoiceOrChequeNo, '') as InvoiceOrChequeNo
                                            , NULLIF(ApplyToInvoice, '') as ApplyToInvoice
                                            , convert(numeric(13,2), ISNULL(LocalAmt, 0)) as LocalAmt
                                        FROM [dbo].[sltx1] 
                                        WHERE TrxNo > {} ORDER BY TrxNo '''.format(int(sltx1_lastrow))
    df_SalesLedgerTransaction = CTITBKK.sql_read(SalesLedgerTransaction_statement) #SalesLedgerTransaction_Table
    SalesLedgerTransaction_row = df_SalesLedgerTransaction.shape[0]
    if SalesLedgerTransaction_row > 0:
        print('SalesLedgerTransaction : {} new records'.format(SalesLedgerTransaction_row))
        print('writing data to SalesLedgerTransaction_Table')
        replicate_BI.sql_insert('SalesLedgerTransaction_Table', df_SalesLedgerTransaction)
    else:
        print('SalesLedgerTransaction : do not have a new record')