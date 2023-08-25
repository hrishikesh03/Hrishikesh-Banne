import pyodbc
import os
import pandas as pd
import numpy as np

os.chdir('F:\Projects\RIPPLR\SALES')
hub_dir = pd.read_excel('F:\Projects\RIPPLR\Hub_directory.xlsx', 'Sheet1')
hub_dir['Hub_id'] = hub_dir['Hub_id'].map(str)
hub_dir['Hub_id'] = '0' + hub_dir['Hub_id']

hub_dir['Brand_id'] =  np.where(hub_dir['Brand_id'] <=9 , '0'+ hub_dir['Brand_id'].map(str),hub_dir['Brand_id'].map(str))
hub_dir['File_flag'] = 'Report not received'
hub_dir['Received_rows'] = ''
hub_dir['Loaded_rows'] = ''
hub_dir['RC_validation_flag'] = 'NA'
p = pd.DataFrame(os.listdir('F:\Projects\RIPPLR\SALES'), columns=['filename'])
p['Hub_id'] = p['filename'].str.slice(0,2)
p['Brand_id'] =  p['filename'].str.slice(3,5)


def map_hubs(value):
    if value == '01':
        return 'ITI'
    if value == '02':
        return 'NCK'
    if value == '03':
        return 'RP'
    if value == '04':
        return 'BM'
    if value == '05':
        return 'GGN'

def map_brands(value):
     if value == '01':
        return 'KIA'
     if value == '02':
        return 'Hyundai'
     if value == '03':
        return 'Too Yum'
     if value == '04':
        return 'OneEight'
     if value == '05':
        return 'Puma'
     if value == '06':
        return 'Telcom'
     if value == '07':
        return 'WD'
     if value == '08':
        return 'MCD'
     if value == '09':
        return 'KFC'
     if value == '10':
        return 'Britannia'
     if value == '11':
        return 'Airtel'
     if value == '12':
        return 'Docomo'
    
print('Mapping Hubs and Brands\n')    
p['Hub'] = p['Hub_id'].map(map_hubs)
p['Brand'] = p['Brand_id'].map(map_brands)

os.chdir('F:\Projects\RIPPLR\SALES')
filecount=0
for index, row1 in p.iterrows():
    file = row1["filename"]
    brand_id = row1["Brand_id"]
    hub_id = row1["Hub_id"]
    brand = row1["Brand"]
    hub = row1["Hub"]
    filecount+=1
##Hyundai ITI
    if brand_id =='02' and hub_id == '01':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')                 
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill Number']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
        # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
        master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
        # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
        #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Sales Value']].copy()
        # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
        # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
        # master_tamplate[['dp']] = copy_from[['Selling Rate']].copy()
        master_tamplate[['salesman_code']] = copy_from[['Salesman Code']].copy()
        master_tamplate[['salesman_name']] = copy_from[['Salesman Name']].copy()
        master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Amount']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,sales_quantity 
                                        ,gross_amount 
                                        ,salesman_code 
                                        ,salesman_name
                                        ,total_discount 
                                        ,total_tax_amount 
                                        ,net_amount
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.sales_quantity
                                ,row.gross_amount
                                ,row.salesman_code
                                ,row.salesman_name
                                ,row.total_discount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.brand
                                ,row.warehouse                  
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))
    
#KIA ITI
    if brand_id =='01' and hub_id == '01': 
            conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LAPTOP-TCEH1402;'
                                  'Database=IPL_V1;'
                                  'Trusted_Connection=yes;')
            cursor = conn.cursor()
            print("\n\nLoading file of {} from {}".format(brand,hub))
            hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] ==brand_id) , 'Report Received',inplace = True)
            
            copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
            hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] ==brand_id) ,copy_from.shape[0] ,inplace = True)
            
            master_tamplate = pd.read_excel('master template.xlsx')
            master_tamplate[['dms_invoice_number']] = copy_from[['Bill Number']].copy()
            master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
            master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
            master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
            # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
            # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
            master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
            master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
            # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
            # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
            master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
            # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
            #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
            master_tamplate[['gross_amount']] = copy_from[['Sales Value']].copy()
            # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
            # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
            # master_tamplate[['dp']] = copy_from[['Selling Rate']].copy()
            master_tamplate[['salesman_code']] = copy_from[['Salesman Code']].copy()
            master_tamplate[['salesman_name']] = copy_from[['Salesman Name']].copy()
            master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
            master_tamplate[['total_tax_amount']] = copy_from[['Total Tax']].copy()
            master_tamplate[['net_amount']] = copy_from[['Net Amount']].copy()
            master_tamplate[['brand']] = brand_id
            master_tamplate[['warehouse']] = hub_id
            
            df = pd.DataFrame(master_tamplate)
            print('\n=> Transformations completed')            
            print('\n=> Fetching required columns')           
            counter = 0
            for row in df.itertuples():
                counter+=1
                cursor.execute('''
                                INSERT INTO dbo.sales_master 
                                        (    dms_invoice_number
                                            ,dms_invoice_date
                                            ,retailer_code 
                                            ,retailer_name
                                            ,product_code 
                                            ,product_description 
                                            ,sales_quantity 
                                            ,gross_amount 
                                            ,salesman_code 
                                            ,salesman_name
                                            ,total_discount 
                                            ,total_tax_amount 
                                            ,net_amount
                                            ,brand
                                            ,warehouse
                                        )
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ''',
                                     row.dms_invoice_number
                                    ,row.dms_invoice_date
                                    ,row.retailer_code
                                    ,row.retailer_name
                                    ,row.product_code
                                    ,row.product_description
                                    ,row.sales_quantity
                                    ,row.gross_amount
                                    ,row.salesman_code
                                    ,row.salesman_name
                                    ,row.total_discount
                                    ,row.total_tax_amount
                                    ,row.net_amount
                                    ,row.brand
                                    ,row.warehouse
                              )
            hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] ==brand_id) ,counter ,inplace = True)
            conn.commit()
            print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))
            
##OneEight  NCK        
    if brand_id =='04' and hub_id == '02':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, skiprows =13)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] ==brand_id) ,copy_from.shape[0] ,inplace = True)      
        
        master_tamplate = pd.read_excel('master template.xlsx')        
        master_tamplate[['dms_invoice_date']] = copy_from[['Generation date']].copy()
        master_tamplate[['dms_invoice_number']] = copy_from[['Document Number']].copy()
        master_tamplate[['dms_invoice_status']] = copy_from[['Status']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Customer code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Customer name']].copy()
        #master_tamplate[['retailer_gstin']] = copy_from[['GSTIN']].copy()
        # master_tamplate[['route_plan']] = copy_from[['Route plan']].copy()
        # master_tamplate[['route_name']] = copy_from[['Route name']].copy()
        #master_tamplate[['dms_invoice_status']] = copy_from[['Status']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN']].copy()
        master_tamplate[['product_code']] = copy_from[['Product code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product description']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amount']].copy()
        # master_tamplate[['promotion_code']] = copy_from[['Promotion code']].copy()
        # master_tamplate[['promotion_description']] = copy_from[['Promotion description']].copy()
        # master_tamplate[['claimable_indicator']] = copy_from[['Claimable indicator']].copy()
        #master_tamplate[['sgst_amount']] =  copy_from[['SGST Amount']].copy()
        # master_tamplate[['cess_percent']] = copy_from[['CESS %']].copy()
        #master_tamplate[['cess_amount']] =  copy_from[['CESS Amount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax amount']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Amount']].copy()
        #master_tamplate[['utgst_amount']] =  copy_from[['UTGST Amount']].copy()
        # master_tamplate[['unit_price']] = copy_from[['Selling Rate']].copy()
        master_tamplate[['gstr1']] = copy_from[['Document Type']].copy()
        master_tamplate[['salesman_code']] = copy_from[['Route name']].copy()
        master_tamplate[['salesman_name']] = copy_from[['Route name']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,dms_invoice_status
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,gross_amount 
                                        ,total_tax_amount
                                        ,net_amount
                                        ,gstr1
                                        ,salesman_code
                                        ,salesman_name
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.dms_invoice_status
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.gross_amount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.gstr1
                                ,row.salesman_code
                                ,row.salesman_name
                                ,row.brand
                                ,row.warehouse
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] ==brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

##Puma NCK
    if brand_id =='05' and hub_id == '02':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, skiprows =13)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
                                                                 
        master_tamplate = pd.read_excel('master template.xlsx') 
        master_tamplate[['dms_invoice_date']] = copy_from[['Generation date']].copy()
        master_tamplate[['dms_invoice_number']] = copy_from[['Document Number']].copy()
        master_tamplate[['dms_invoice_status']] = copy_from[['Status']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Customer code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Customer name']].copy()
        #master_tamplate[['retailer_gstin']] = copy_from[['GSTIN']].copy()
        # master_tamplate[['route_plan']] = copy_from[['Route plan']].copy()
        # master_tamplate[['route_name']] = copy_from[['Route name']].copy()
        # master_tamplate[['dms_invoice_status']] = copy_from[['Status']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN']].copy()
        master_tamplate[['product_code']] = copy_from[['Product code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product description']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amount']].copy()
        # master_tamplate[['promotion_code']] = copy_from[['Promotion code']].copy()
        # master_tamplate[['promotion_description']] = copy_from[['Promotion description']].copy()
        # master_tamplate[['claimable_indicator']] = copy_from[['Claimable indicator']].copy()
        # master_tamplate[['cgst_percentage']] = copy_from[['CGST %']].copy()
        # master_tamplate[['igst_amount']] =  copy_from[['IGST Amount']].copy()
        # master_tamplate[['cess_percent']] = copy_from[['CESS %']].copy()
        # master_tamplate[['cess_amount']] =  copy_from[['CESS Amount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax amount']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Amount']].copy()
        # master_tamplate[['unit_price']] = copy_from[['Selling Rate']].copy()
        master_tamplate[['gstr1']] = copy_from[['Document Type']].copy()
        master_tamplate[['salesman_code']] = copy_from[['Route name']].copy()
        master_tamplate[['salesman_name']] = copy_from[['Route name']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,dms_invoice_status
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,gross_amount 
                                        ,total_tax_amount
                                        ,net_amount
                                        ,gstr1
                                        ,salesman_code
                                        ,salesman_name
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.dms_invoice_status
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.gross_amount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.gstr1
                                ,row.salesman_code
                                ,row.salesman_name
                                ,row.brand
                                ,row.warehouse
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

##Kimberly Clarke RP
    if brand_id == '09' and hub_id == '03':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, skiprows=18)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')        
        master_tamplate['dms_invoice_date'] = copy_from['Transaction Date'].copy()
        master_tamplate[['dms_invoice_number']] = copy_from[['Transaction No.']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Customer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Customer Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Description ']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amount (₹)']].copy()
        # master_tamplate[['promotion_discount_amount']] = copy_from[['Discount Amt (₹)']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Tax Amount (₹)']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Price(₹)']].copy()
        master_tamplate[['gstr1']] = copy_from[['Transaction Type']].copy()
        master_tamplate[['salesman_code']] = copy_from[['Salesman Code']].copy()
        master_tamplate[['salesman_name']] = copy_from[['Salesman Name ']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = brand_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,gross_amount 
                                        ,total_tax_amount
                                        ,net_amount
                                        ,gstr1
                                        ,salesman_code
                                        ,salesman_name
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.gross_amount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.gstr1
                                ,row.salesman_code
                                ,row.salesman_name
                                ,row.brand
                                ,row.warehouse
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))
        

        
##Telcom RP
    if brand_id == '06' and hub_id == '03':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')         
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill No']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
        # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
        master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
        # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
        #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amt']].copy()
        # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
        # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
        master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax Amount']].copy()
        master_tamplate[['net_amount']] = copy_from[['NetAmount']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,sales_quantity 
                                        ,gross_amount 
                                        ,total_discount 
                                        ,total_tax_amount 
                                        ,net_amount
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.sales_quantity
                                ,row.gross_amount
                                ,row.total_discount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.brand
                                ,row.warehouse                  
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))
        
##WD RP
    if brand_id =='07' and hub_id == '03':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')
        print('\n=> Files loaded to python')
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill No']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
        # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
        master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
        # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
        #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amt']].copy()
        # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
        # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
        master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax Amount']].copy()
        master_tamplate[['net_amount']] = copy_from[['NetAmount']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,sales_quantity 
                                        ,gross_amount 
                                        ,total_discount 
                                        ,total_tax_amount 
                                        ,net_amount
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.sales_quantity
                                ,row.gross_amount
                                ,row.total_discount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.brand
                                ,row.warehouse                  
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

#MCD RP    
    if brand_id =='08' and hub_id == '03':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 

        # master_tamplate[['sno']] = copy_from[['Sr No']].copy()
        # master_tamplate[['beat']] = copy_from[['BEAT_NAME']].copy()
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill Number']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        # master_tamplate[['dms_invoice_status']] = copy_from[['BILL_STATUS']].copy()
        # master_tamplate[['free_qty']] = copy_from[['SALES_FREE_QTY']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Sales']].copy()
        # master_tamplate[['invoice_quantity']] = copy_from[['TOTAL_SALES_QTY']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        master_tamplate[['net_sales_quantity']] = copy_from[['Units']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Sales']].copy()
        # master_tamplate[['product_category']] = copy_from[['PRODUCT_CATEGORY']].copy()
        # master_tamplate[['product_sub_category']] = copy_from[['PRODUCT_SUB_CATEGORY']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Description']].copy()
        # master_tamplate[['sales_quantity']] = copy_from[['SALES_RET_QTY']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Party Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Party']].copy()
        # master_tamplate[['sales_return_qty']] = copy_from[['TOTAL_SALES_QTY']].copy()
        # master_tamplate[['sales_return_free_qty']] = copy_from[['SALES_RET_FREE_QTY']].copy()
        # master_tamplate[['sales_return_value']] = copy_from[['SALES_RET_VALUE']].copy()
        # master_tamplate[['retailer_gstin']] = copy_from[['Party GSTIN Number']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax']].copy()
        # master_tamplate[['tax_percentage']] = copy_from[['TAX_PERCENTAGE']].copy()
        # master_tamplate[['vehicle_name']] = copy_from[['VEHICLE']].copy()
        # master_tamplate[['basic_rate']] = copy_from[['BASIC_RATE']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,gross_amount 
                                        ,net_sales_quantity
                                        ,net_amount 
                                        ,product_description 
                                        ,retailer_code
                                        ,retailer_name
                                        ,total_tax_amount 
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                   row.dms_invoice_number
                                  ,row.dms_invoice_date
                                  ,row.gross_amount 
                                  ,row.net_sales_quantity
                                  ,row.net_amount 
                                  ,row.product_description 
                                  ,row.retailer_code
                                  ,row.retailer_name
                                  ,row.total_tax_amount 
                                  ,row.brand
                                  ,row.warehouse               
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

##Docomo BM
    if brand_id =='12' and hub_id == '04':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill No']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
        # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
        master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
        # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
        #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Amt']].copy()
        # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
        # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
        master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax Amount']].copy()
        master_tamplate[['net_amount']] = copy_from[['NetAmount']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,sales_quantity 
                                        ,gross_amount 
                                        ,total_discount 
                                        ,total_tax_amount 
                                        ,net_amount
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.sales_quantity
                                ,row.gross_amount
                                ,row.total_discount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.brand
                                ,row.warehouse                  
                          )
        conn.commit()

		
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

#MCD BM    
    if brand_id =='08' and hub_id == '04':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 

        # master_tamplate[['sno']] = copy_from[['Sr No']].copy()
        # master_tamplate[['beat']] = copy_from[['BEAT_NAME']].copy()
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill Number']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        # master_tamplate[['dms_invoice_status']] = copy_from[['BILL_STATUS']].copy()
        # master_tamplate[['free_qty']] = copy_from[['SALES_FREE_QTY']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Gross Sales']].copy()
        # master_tamplate[['invoice_quantity']] = copy_from[['TOTAL_SALES_QTY']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        master_tamplate[['net_sales_quantity']] = copy_from[['Units']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Sales']].copy()
        # master_tamplate[['product_category']] = copy_from[['PRODUCT_CATEGORY']].copy()
        # master_tamplate[['product_sub_category']] = copy_from[['PRODUCT_SUB_CATEGORY']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Description']].copy()
        # master_tamplate[['sales_quantity']] = copy_from[['SALES_RET_QTY']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Party Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Party']].copy()
        # master_tamplate[['sales_return_qty']] = copy_from[['TOTAL_SALES_QTY']].copy()
        # master_tamplate[['sales_return_free_qty']] = copy_from[['SALES_RET_FREE_QTY']].copy()
        # master_tamplate[['sales_return_value']] = copy_from[['SALES_RET_VALUE']].copy()
        # master_tamplate[['retailer_gstin']] = copy_from[['Party GSTIN Number']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax']].copy()
        # master_tamplate[['tax_percentage']] = copy_from[['TAX_PERCENTAGE']].copy()
        # master_tamplate[['vehicle_name']] = copy_from[['VEHICLE']].copy()
        # master_tamplate[['basic_rate']] = copy_from[['BASIC_RATE']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')        
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,gross_amount 
                                        ,net_sales_quantity
                                        ,net_amount 
                                        ,product_description 
                                        ,retailer_code
                                        ,retailer_name
                                        ,total_tax_amount 
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                   row.dms_invoice_number
                                  ,row.dms_invoice_date
                                  ,row.gross_amount 
                                  ,row.net_sales_quantity
                                  ,row.net_amount 
                                  ,row.product_description 
                                  ,row.retailer_code
                                  ,row.retailer_name
                                  ,row.total_tax_amount 
                                  ,row.brand
                                  ,row.warehouse               
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))

##KIA GGN
    if brand_id =='01' and hub_id == '05':
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-TCEH1402;'
                              'Database=IPL_V1;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        print("\n\nLoading file of {} from {}".format(brand,hub))
        hub_dir['File_flag'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) , 'Report Received',inplace = True)
        
        copy_from = pd.read_excel(file, 'Sheet1', skiprows=2)
        hub_dir['Received_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,copy_from.shape[0] ,inplace = True) 
        
        master_tamplate = pd.read_excel('master template.xlsx')
        master_tamplate[['dms_invoice_number']] = copy_from[['Bill Number']].copy()
        master_tamplate[['dms_invoice_date']] = copy_from[['Bill Date']].copy()
        master_tamplate[['retailer_code']] = copy_from[['Retailer Code']].copy()
        master_tamplate[['retailer_name']] = copy_from[['Retailer Name']].copy()
        # master_tamplate[['hsn_code']] = copy_from[['HSN Code']].copy()
        # master_tamplate[['hsn_name']] = copy_from[['HSN Name']].copy()
        master_tamplate[['product_code']] = copy_from[['Product Code']].copy()
        master_tamplate[['product_description']] = copy_from[['Product Name']].copy()
        # master_tamplate[['mrp']] = copy_from[['MRP']].copy()
        # master_tamplate[['batch_code']] = copy_from[['Batch Code']].copy()
        master_tamplate[['sales_quantity']] = copy_from[['Sales Qty']].copy()
        # master_tamplate[['free_qty']] = copy_from[['Free Qty']].copy()
        #master_tamplate[['sales_quantity']] = copy_from[['Total Qty']].copy()
        master_tamplate[['gross_amount']] = copy_from[['Sales Value']].copy()
        # master_tamplate[['scheme_discount']] = copy_from[['Scheme Amount']].copy()
        # master_tamplate[['spl_discount']] = copy_from[['Spl.Discount']].copy()
        # master_tamplate[['dp']] = copy_from[['Selling Rate']].copy()
        master_tamplate[['salesman_code']] = copy_from[['Salesman Code']].copy()
        master_tamplate[['salesman_name']] = copy_from[['Salesman Name']].copy()
        master_tamplate[['total_discount']] = copy_from[['Total Discount']].copy()
        master_tamplate[['total_tax_amount']] = copy_from[['Total Tax']].copy()
        master_tamplate[['net_amount']] = copy_from[['Net Amount']].copy()
        master_tamplate[['brand']] = brand_id
        master_tamplate[['warehouse']] = hub_id
        df = pd.DataFrame(master_tamplate)
        print('\n=> Transformations completed')       
        print('\n=> Fetching required columns')
        
        counter = 0
        for row in df.itertuples():
            counter+=1
            cursor.execute('''
                            INSERT INTO dbo.sales_master 
                                    (    dms_invoice_number
                                        ,dms_invoice_date
                                        ,retailer_code 
                                        ,retailer_name
                                        ,product_code 
                                        ,product_description 
                                        ,sales_quantity 
                                        ,gross_amount 
                                        ,salesman_code 
                                        ,salesman_name
                                        ,total_discount 
                                        ,total_tax_amount 
                                        ,net_amount
                                        ,brand
                                        ,warehouse
                                    )
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                                 row.dms_invoice_number
                                ,row.dms_invoice_date
                                ,row.retailer_code
                                ,row.retailer_name
                                ,row.product_code
                                ,row.product_description
                                ,row.sales_quantity
                                ,row.gross_amount
                                ,row.salesman_code
                                ,row.salesman_name
                                ,row.total_discount
                                ,row.total_tax_amount
                                ,row.net_amount
                                ,row.brand
                                ,row.warehouse                  
                          )
        conn.commit()
        hub_dir['Loaded_rows'].mask((hub_dir['Hub_id'] == hub_id) & (hub_dir['Brand_id'] == brand_id) ,counter ,inplace = True)
        print('\n{} rows inserted for brand {} and hub {}'.format(counter,brand,hub))



hub_dir['RC_validation_flag'] = np.where((hub_dir['Received_rows'] == hub_dir['Loaded_rows']) & (hub_dir['File_flag'] == 'Report Received'), 'Success',hub_dir['RC_validation_flag']) 

print('\nGenerating data staging report...')
print('\n files received = {} \n files loaded = {}'.format(p.shape[0],filecounter))
upload_path = r"F:\Projects\RIPPLR\Upload_Reports\\"
hub_dir.to_excel(upload_path + 'Upload_report_' + str(datetime.fromtimestamp(time.time())).replace(':','_') + ".xlsx",index=False)
print('\nReport Generated successfully. \nsaved at :' + upload_path)
                                                                 
