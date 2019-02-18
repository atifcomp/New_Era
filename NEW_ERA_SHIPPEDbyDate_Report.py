# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:09:57 2019

@author: amomin
"""
import pandas as pd
import pyodbc
from datetime import datetime,timedelta,date

from openpyxl.styles import Font, Color
from openpyxl.styles import colors

cnxn_string = 'DSN=WMP04_NEWERA; UID=%s; PWD=%s' % ('ashah', '123sAchin!123')
db_connection = pyodbc.connect(cnxn_string)
db_cursor = db_connection.cursor()

sourceFile = r'C:\Users\amomin\Desktop\Training\proj 01 Matt'

#sql = """
#SELECT TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM') AS SHIP_MONTH,
#    TRUNC(O.ACTUALSHIPDATE) AS SHIP_DATE,
#    COUNT(DISTINCT O.ORDERKEY) AS ORDERS,
#    COUNT(OD.ORDERKEY) as LINES,
#    SUM(OD.SHIPPEDQTY) AS SHIPPEDQTY
#FROM ORDERS_DW@WMP04 O 
#JOIN ORDERDETAIL_DW@WMP04 OD ON O.ORDERKEY = OD.ORDERKEY
#    WHERE O.STATUS=95 AND OD.SHIPPEDQTY>0 AND (TRUNC(O.ACTUALSHIPDATE) < TRUNC(SYSDATE))
#GROUP BY TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM') ,TRUNC(O.ACTUALSHIPDATE)
#ORDER BY TRUNC(O.ACTUALSHIPDATE)
#"""

sql = """
SELECT   TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM') AS SHIP_MONTH,
         TRUNC(O.ACTUALSHIPDATE) AS SHIP_DATE,
         CASE WHEN O.TYPE IN ('HOTSEALCPN', 'MANUAL') THEN 'HEATSEAL BASE INV' ELSE 'OUTBOUND' END ORDER_TYPE,
         OD.LOTTABLE01 PLANT_CODE,
         COUNT(DISTINCT O.ORDERKEY) AS ORDERS,
         COUNT(OD.ORDERKEY) AS LINES,
         SUM(OD.SHIPPEDQTY) AS SHIPPEDQTY
    FROM ORDERS_DW@WMP04 O JOIN ORDERDETAIL_DW@WMP04 OD ON O.ORDERKEY = OD.ORDERKEY
   WHERE O.STATUS = 95
     AND OD.SHIPPEDQTY > 0
     AND O.ACTUALSHIPDATE < TRUNC(SYSDATE)
GROUP BY TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM'),
         TRUNC(O.ACTUALSHIPDATE),
         CASE WHEN O.TYPE IN ('HOTSEALCPN', 'MANUAL') THEN 'HEATSEAL BASE INV' ELSE 'OUTBOUND' END,
         OD.LOTTABLE01
ORDER BY TRUNC(O.ACTUALSHIPDATE) DESC        
"""

shipped_df = pd.read_sql (sql, db_connection)

shipped_df['SHIP_DATE'] = shipped_df['SHIP_DATE'].dt.date


shipped_df_outbound = shipped_df[shipped_df['ORDER_TYPE']=='OUTBOUND']
shipped_df_heatseal = shipped_df[shipped_df['ORDER_TYPE']=='HEATSEAL BASE INV']


#For OutBound orders
# making monthly report and adding it to the daily level data and sorting it, with blank first so that
# MTD total comes first in the sequence, then filling the blank SHIP_DATE with MTD total to display
shipped_df_outbound_month = shipped_df_outbound.groupby(['SHIP_MONTH','PLANT_CODE']).agg({'SHIPPEDQTY':sum, 'LINES':sum,'ORDERS':sum}).reset_index().sort_values('SHIP_MONTH', ascending = False).round(decimals = 2)
shippedReportOutbound = shipped_df_outbound.append(shipped_df_outbound_month).sort_values(['SHIP_MONTH', 'SHIP_DATE'], ascending = [False, False], na_position = 'first')

#filling the blank SHIP_DATE column with appropriate plant code name
shippedReportOutbound['SHIP_DATE']=shippedReportOutbound.apply(lambda x: 'MTD '+x['PLANT_CODE']+' Total' if pd.isna(x['SHIP_DATE']) else x['SHIP_DATE'],axis=1)

#rearranging the order of the variables
shippedReportOutbound = shippedReportOutbound[['SHIP_MONTH','SHIP_DATE','PLANT_CODE','ORDERS','LINES','SHIPPEDQTY']]

#Putting Data of Outbound to report
writer = pd.ExcelWriter(sourceFile + r'\NEW ERA SHIPPED REPORT.xlsx',engine='xlsxwriter') 
shippedReportOutbound.to_excel(writer,sheet_name='SHIPPED_OUTBOUND', startrow = 0, index = False)

#For HEATSEAL BASE INV Orders
shipped_df_heatseal_month = shipped_df_heatseal.groupby(['SHIP_MONTH']).agg({'SHIPPEDQTY':sum, 'LINES':sum,'ORDERS':sum}).reset_index().sort_values('SHIP_MONTH', ascending = False).round(decimals = 2)
shippedReportheatseal = shipped_df_heatseal.append(shipped_df_heatseal_month).sort_values(['SHIP_MONTH', 'SHIP_DATE'], ascending = [False, False], na_position = 'first')
shippedReportheatseal['SHIP_DATE'] = shippedReportheatseal['SHIP_DATE'].fillna('MTD Total')
shippedReportheatseal = shippedReportheatseal[['SHIP_MONTH','SHIP_DATE','ORDERS','LINES','SHIPPEDQTY']]

#Putting Data of heatseal to report
shippedReportheatseal.to_excel(writer,sheet_name='SHIPPED_HEAT_SEAL', startrow = 0, index = False)



#Center All colums
formatCenter = writer.book.add_format()
formatCenter.set_center_across()
formatPercent = writer.book.add_format({'num_format': '0.0%', 'align': 'center'})
formatNum = writer.book.add_format({'num_format': '#,##0', 'align': 'center'})

#center align                                    
def set_center(sheetname,cols):
    writer.sheets[sheetname].set_column(cols, None, formatCenter)
#center align                                    
def format_num(sheetname,cols):
    writer.sheets[sheetname].set_column(cols, None, formatNum)

#function to adjust column width, taking inputs of df and sheet name    
def adjust_cols_width(df,sheet_name):    
    width_list = [max([len(str(s))+3 for s in df[col].values] + [len(col)+3]) for col in df.columns]
    for i in range(0,len(width_list)):
        writer.sheets[sheet_name].set_column(i, i, width_list[i])


set_center('SHIPPED_OUTBOUND','A:F')
format_num('SHIPPED_OUTBOUND','D:F')
adjust_cols_width(shippedReportOutbound,'SHIPPED_OUTBOUND') 


set_center('SHIPPED_HEAT_SEAL','A:F')
format_num('SHIPPED_HEAT_SEAL','D:F')
adjust_cols_width(shippedReportheatseal,'SHIPPED_HEAT_SEAL') 


writer.save()


#
## making monthly report and adding it to the daily level data and sorting it, with blank first so that
## MTD total comes first in the sequence, then filling the blank SHIP_DATE with MTD total to display
#dfShippedMonth = shipped_df.groupby(['SHIP_MONTH']).agg({'SHIPPEDQTY':sum, 'LINES':sum,'ORDERS':sum}).reset_index().sort_values('SHIP_MONTH', ascending = False).round(decimals = 2)
#shippedFinalReport = shipped_df.append(dfShippedMonth).sort_values(['SHIP_MONTH', 'SHIP_DATE'], ascending = [False, False], na_position = 'first')
#shippedFinalReport['SHIP_DATE'] = shippedFinalReport['SHIP_DATE'].fillna('MTD Total')
#
##rearranging the order of the variables
#shippedFinalReport = shippedFinalReport[['SHIP_MONTH','SHIP_DATE','ORDERS','LINES','SHIPPEDQTY']]

#Report Genration
#writer = pd.ExcelWriter(sourceFile + r'\NEW ERA SHIPPED REPORT.xlsx',engine='xlsxwriter') 
#shippedFinalReport.to_excel(writer,sheet_name='SHIPPED', startrow = 0, index = False)

#
##Center All colums
#formatCenter = writer.book.add_format()
#formatCenter.set_center_across()
#formatPercent = writer.book.add_format({'num_format': '0.0%', 'align': 'center'})
#formatNum = writer.book.add_format({'num_format': '#,##0', 'align': 'center'})
#
##center align                                    
#def set_center(sheetname,cols):
#    writer.sheets[sheetname].set_column(cols, None, formatCenter)
##center align                                    
#def format_num(sheetname,cols):
#    writer.sheets[sheetname].set_column(cols, None, formatNum)
#
##function to adjust column width, taking inputs of df and sheet name    
#def adjust_cols_width(df,sheet_name):    
#    width_list = [max([len(str(s))+3 for s in df[col].values] + [len(col)+3]) for col in df.columns]
#    for i in range(0,len(width_list)):
#        writer.sheets[sheet_name].set_column(i, i, width_list[i])

#formating operations triggered
#set_center('SHIPPED','A:E')
#format_num('SHIPPED','C:E')
#adjust_cols_width(shippedFinalReport,'SHIPPED') 
#
#
#writer.save()






