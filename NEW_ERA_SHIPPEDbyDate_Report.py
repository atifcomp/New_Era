from datetime import datetime,timedelta,date
import pandas as pd
import os.path
import os, errno


from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models import DAG
from airflow.hooks.oracle_hook import OracleHook

sourceFile = r'/Reports/New_Era/New_Era_Shipped/'
todayDate = str(datetime.today().date())

try:
    os.makedirs(sourceFile)
except OSError as e:
    if e.errno != errno.EEXIST:
        raise


def report(**kwargs):
    hook = OracleHook("New Era - WMP04")
    
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
         AND O.STORERKEY = '1168'
         AND O.ACTUALSHIPDATE < TRUNC(SYSDATE)
    GROUP BY TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM'),
             TRUNC(O.ACTUALSHIPDATE),
             CASE WHEN O.TYPE IN ('HOTSEALCPN', 'MANUAL') THEN 'HEATSEAL BASE INV' ELSE 'OUTBOUND' END,
             OD.LOTTABLE01
    ORDER BY TRUNC(O.ACTUALSHIPDATE) DESC        
    """

    shipped_df = hook.get_pandas_df(sql=sql)

    shipped_df['SHIP_DATE'] = shipped_df['SHIP_DATE'].dt.date


    shipped_df_outbound = shipped_df[shipped_df['ORDER_TYPE']=='OUTBOUND']
    shipped_df_heatseal = shipped_df[shipped_df['ORDER_TYPE']=='HEATSEAL BASE INV']

    #Pull in YDAY detail for outbound orders
    sql = """
    SELECT  TO_CHAR(O.ACTUALSHIPDATE, 'YYYY-MM') AS SHIP_MONTH,
         TRUNC(O.ACTUALSHIPDATE) AS SHIP_DATE,
         O.TYPE ORDER_TYPE,
         OD.LOTTABLE01 PLANT_CODE,
         O.ORDERKEY XPO_ORDERKEY,
         O.EXTERNORDERKEY EXTERNORDERKEY,
         OD.SKU,
         OD.SHIPPEDQTY
    FROM ORDERS_DW@WMP04 O JOIN ORDERDETAIL_DW@WMP04 OD ON O.ORDERKEY = OD.ORDERKEY
   WHERE O.STATUS = 95
     AND OD.SHIPPEDQTY > 0
     AND O.STORERKEY = '1168'
     AND TRUNC(O.ACTUALSHIPDATE) = TRUNC(SYSDATE - 1)
     AND O.TYPE NOT IN ('HOTSEALCPN', 'MANUAL')
ORDER BY O.ORDERKEY,
         OD.SKU       
    """

    shippedYdayDetail = hook.get_pandas_df(sql=sql)

    shippedYdayDetail['SHIP_DATE'] = shippedYdayDetail['SHIP_DATE'].dt.date

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
    writer = pd.ExcelWriter(sourceFile + r'NEW ERA SHIPPED REPORT_'+todayDate+'.xlsx',engine='xlsxwriter') 
    shippedReportOutbound.to_excel(writer,sheet_name='SHIPPED_OUTBOUND', startrow = 0, index = False)

    #For HEATSEAL BASE INV Orders
    shipped_df_heatseal_month = shipped_df_heatseal.groupby(['SHIP_MONTH']).agg({'SHIPPEDQTY':sum, 'LINES':sum,'ORDERS':sum}).reset_index().sort_values('SHIP_MONTH', ascending = False).round(decimals = 2)
    shippedReportheatseal = shipped_df_heatseal.append(shipped_df_heatseal_month).sort_values(['SHIP_MONTH', 'SHIP_DATE'], ascending = [False, False], na_position = 'first')
    shippedReportheatseal['SHIP_DATE'] = shippedReportheatseal['SHIP_DATE'].fillna('MTD Total')
    shippedReportheatseal = shippedReportheatseal[['SHIP_MONTH','SHIP_DATE','ORDERS','LINES','SHIPPEDQTY']]

    #Putting Data of heatseal to report
    shippedReportheatseal.to_excel(writer,sheet_name='SHIPPED_HEAT_SEAL', startrow = 0, index = False)
    
    #Add Yday Detail Data
    shippedYdayDetail.to_excel(writer,sheet_name='YDAY SHIPPED_OUTBOUND DETAIL', startrow = 0, index = False)

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

    
    set_center('YDAY SHIPPED_OUTBOUND DETAIL','A:H')
    format_num('YDAY SHIPPED_OUTBOUND DETAIL','H:H')
    adjust_cols_width(shippedYdayDetail,'YDAY SHIPPED_OUTBOUND DETAIL') 

    writer.save()
    


    
    


#Typically, we won't need to change these
default_args = {
    'owner': 'xposcanalytics',
    'depends_on_past': False,
    'start_date': datetime(2019,2,11),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


#Assign DAG Name here and set schedule (DAG name must have no spaces), Crontab.guru website can help with scheduling
dag = DAG(dag_id='NEW_ERA_SHIPPED', default_args=default_args, schedule_interval='40 5 * * 0-6')


#This calls the report function, typically won't need to change
create_report = PythonOperator(
    task_id = "create_report",
    provide_context=True,
    python_callable=report,
    email_on_failure=True,
    email='xposcanalytics@xpo.com',
    dag=dag
)


#Build email as needed
email = EmailOperator(task_id='send-report',
                      #to=['matthew.clark@xpo.com'],
                      to=['lance.young@xpo.com','Dennis.A.Smith@xpo.com','michael.heim@xpo.com','samuel.sylvester@xpo.com', 'Isabel.RiesgoSantos@xpo.com','bard.darhower@xpo.com','Rhea.Pinkney@xpo.com', 'Christine.Fierle@neweracap.com', 'Greg.Mergel@neweracap.com'],
                      #cc = ['atif.momin@xpo.com','xposcanalytics@xpo.com'],
                      subject='New Era Shipped Report ' + todayDate,
                      html_content='All, attached you will find the New Era Shipped Report for ' + todayDate + '. {{ var.value.signature }}' ,
                      files=[sourceFile + r'NEW ERA SHIPPED REPORT_'+todayDate+'.xlsx'],
                      dag=dag)



#Define task flow
create_report >> email  