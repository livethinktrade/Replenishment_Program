# STEP 8 FORMAT SALES SHEET FOR KROGER
import openpyxl
from openpyxl.styles import NamedStyle, Font, Border, Side, colors,Color, Alignment
# from openpyxl.styles import colors
# from openpyxl.styles import Font, Color
from openpyxl.styles.fills import PatternFill

def weekly_sales_report_format(filename, replenishment_len, sales_report_len):

    wb = openpyxl.load_workbook(filename)
    ws = wb['Sales Report']

    #Making whole page white
    pattern_fill_white = PatternFill(patternType='solid', fgColor= 'FFFFFF')


    for row in ws.iter_rows(min_row=0, max_row=500, min_col=0, max_col=52):
        for cell in row:
            cell.fill = pattern_fill_white

    #Merge cells and assigns a value and color

    ws.merge_cells('M5:M6')
    ws.merge_cells('N5:N6')
    ws.merge_cells('O5:O6')

    ws.merge_cells('M10:M11')
    ws.merge_cells('N10:N11')
    ws.merge_cells('O10:O11')

    ws.merge_cells('M14:M15')
    ws.merge_cells('N14:N15')
    ws.merge_cells('O14:O15')



    ws['M5'].value = 'Division Sales current YTD($)'
    ws['N5'].value = 'Division Sales previous YTD($)'
    ws['O5'].value = '% +/- YOY Change'

    # ws['M10'].value = 'Division Sales current YTD($) +Mask'
    # ws['N10'].value = 'Division Sales previous YTD($) +Mask'
    # ws['O10'].value = '% +/- YOY Change'

    ws['M14'].value = 'Rank'
    ws['N14'].value = 'Item'
    ws['O14'].value = 'Sales ($)'


    ws['M16'].value = '1'
    ws['M17'].value = '2'
    ws['M18'].value = '3'
    ws['M19'].value = '4'
    ws['M20'].value = '5'
    ws['M21'].value = '6'
    ws['M22'].value = '7'
    ws['M23'].value = '8'
    ws['M24'].value = '9'
    ws['M25'].value = '10'


    #formating cell format % $

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=4, max_col=4):
        for cell in row:
            cell.style = 'Percent'

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=7, max_col=7):
        for cell in row:
            cell.style = 'Percent'

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=10, max_col=10):
        for cell in row:
            cell.style = 'Percent'

    o7 = ws['O7']
    o7.style = 'Percent'

    o12 = ws['O12']
    o12.style = 'Percent'



    #color set
    pattern_fill_blue = PatternFill(patternType='solid', fgColor= 'DDEBF7')
    pattern_fill_gray = PatternFill(patternType='solid', fgColor= 'D0CECE')
    pattern_fill_green = PatternFill(patternType='solid', fgColor= 'E2EFDA')
    pattern_fill_darkblue = PatternFill(patternType='solid', fgColor= '002060')
    pattern_fill_salmon = PatternFill(patternType='solid', fgColor= 'FCE4D6')



    #changes cell colors
    a1 = ws['A1']
    a1.fill = pattern_fill_gray

    for row in ws.iter_rows(min_row=1, max_row=1, min_col=2, max_col=4):
        for cell in row:
            cell.fill = pattern_fill_green


    #change background color to blue for ytd sales (-mask) and store details 
    for row in ws.iter_rows(min_row=1, max_row=1, min_col=5, max_col=7):
        for cell in row:
            cell.fill = pattern_fill_blue


    for row in ws.iter_rows(min_row=5, max_row=5, min_col=13, max_col=15):
        for cell in row:
            cell.fill = pattern_fill_blue


    #change background color to blue for ytd sales (+mask)
    # for row in ws.iter_rows(min_row=10, max_row=10, min_col=13, max_col=15):
    #     for cell in row:
    #         cell.fill = pattern_fill_blue

    #change background color to blue for item rank
    for row in ws.iter_rows(min_row=1, max_row=1, min_col=13, max_col=15):
        for cell in row:
            cell.fill = pattern_fill_blue



    # changes cells to be bold
    for row in ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=15):
        for cell in row:
            cell.font = Font(bold=True)

        #bold for store ytd column
    for row in ws.iter_rows(min_row=5, max_row=6, min_col=13, max_col=15):
        for cell in row:
            cell.font = Font(bold=True)

        # Bold ytd + mas 
    for row in ws.iter_rows(min_row=10, max_row=11, min_col=13, max_col=15):
        for cell in row:
            cell.font = Font(bold=True)

        # Bold item sales rank 
    for row in ws.iter_rows(min_row=14, max_row=15, min_col=13, max_col=15):
        for cell in row:
            cell.font = Font(bold=True)


    #outlines using thin borders 
    bd = Side(style='thick', color="000000")
    bd_thin = Side(style='thin', color="000000")

    border = Border(left=bd_thin, top=bd_thin, right=bd_thin, bottom=bd_thin)
        
        #outlines main sales table
    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=1, max_col=10):
        for cell in row:
            cell.border = border

        #outlines store sumary detail
    for row in ws.iter_rows(min_row=1, max_row=2, min_col=13, max_col=15):
        for cell in row:
            cell.border = border
            
        #outlines Store YTD Collumns
    for row in ws.iter_rows(min_row=5, max_row=7, min_col=13, max_col=15):
        for cell in row:
            cell.border = border

        #outlines Store YTD Collumns
    # for row in ws.iter_rows(min_row=10, max_row=12, min_col=13, max_col=15):
    #     for cell in row:
    #         cell.border = border

    for row in ws.iter_rows(min_row=14, max_row=25, min_col=13, max_col=15):
        for cell in row:
            cell.border = border

    #outlines using thick borders for all cells on far left side of sales data table

    border = Border(left=bd, top=bd_thin, right=bd_thin, bottom=bd_thin)

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=1, max_col=1):
        for cell in row:
            cell.border = border



    #outlines using thick borders for all cells on far right side of sales data table

    border = Border(left=bd_thin, top=bd_thin, right=bd, bottom=bd_thin)

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=10, max_col=10):
        for cell in row:
            cell.border = border


    #outlines using thick borders for all cells on far top side of sales data table

    border = Border(left=bd_thin, top=bd, right=bd_thin, bottom=bd_thin)

    for row in ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=10):
        for cell in row:
            cell.border = border

    #outlines using thick borders for all cells on far bottom side of sales data table

    border = Border(left=bd_thin, top=bd_thin, right=bd_thin, bottom=bd)

    for row in ws.iter_rows(min_row=sales_report_len, max_row=sales_report_len, min_col=1, max_col=10):
        for cell in row:
            cell.border = border


    a1 = ws['A1']
    a1.border = Border(left=bd, top=bd, right=bd_thin, bottom=bd_thin)

    j1 = ws['J1']
    j1.border = Border(left=bd_thin, top=bd, right=bd, bottom=bd_thin)

    a_bottom = ws[f'A{sales_report_len}']
    a_bottom.border = Border(left=bd, top=bd_thin, right=bd_thin, bottom=bd)

    j_bottom = ws[f'J{sales_report_len}']
    j_bottom.border = Border(left=bd_thin, top=bd_thin, right=bd, bottom=bd)


    # for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=4, max_col=4):
    #     for cell in row:
    #         cell.style = 'Percent'


    #change width and height of collumns
    ws.column_dimensions['A'].width = 10
    ws.column_dimensions['M'].width = 20
    ws.column_dimensions['N'].width = 25
    ws.column_dimensions['O'].width = 20

    ws.row_dimensions[1].height = 60

    #change text to wrap text and center aligns 

    for row in ws.iter_rows(min_row=1, max_row=sales_report_len, min_col=1, max_col=20):
        for cell in row:
            cell.alignment = Alignment(horizontal='center', vertical='center')


    for row in ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=20):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

    for row in ws.iter_rows(min_row=5, max_row=5, min_col=13, max_col=15):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

    # for row in ws.iter_rows(min_row=10, max_row=10, min_col=13, max_col=15):
    #     for cell in row:
    #         cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')


    #####FROM THIS POINT ON CODE IS FORMATTING REPLENISHMENT PAGE #######

    ws = wb['Replenishment']

    #outlining border 

    border = Border(left=bd_thin, top=bd_thin, right=bd_thin, bottom=bd_thin)

    for row in ws.iter_rows(min_row=1, max_row=replenishment_len, min_col=1, max_col=3):
        for cell in row:
            cell.border = border


    # setting columns to certain colors
    for row in ws.iter_rows(min_row=1, max_row=replenishment_len, min_col=1, max_col=1):
        for cell in row:
            cell.fill = pattern_fill_gray

    for row in ws.iter_rows(min_row=1, max_row=replenishment_len, min_col=2, max_col=2):
        for cell in row:
            cell.fill = pattern_fill_green

    for row in ws.iter_rows(min_row=1, max_row=replenishment_len, min_col=3, max_col=3):
        for cell in row:
            cell.fill = pattern_fill_salmon

    for row in ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=3):
        for cell in row:
            cell.fill = pattern_fill_darkblue


    #setting cells to bold

        #sets first row to bold and change color to white
    for row in ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=13):
        for cell in row:
            cell.font = Font(bold=True, color='FFFFFF')

        #sets store number to bold 
    for row in ws.iter_rows(min_row=2, max_row=replenishment_len, min_col=1, max_col=1):
        for cell in row:
            cell.font = Font(bold=True)

    # center aligns data
    for row in ws.iter_rows(min_row=1, max_row=replenishment_len, min_col=1, max_col=3):
        for cell in row:
            cell.alignment = Alignment(wrap_text=False, horizontal='center', vertical='center')

    ws.column_dimensions['B'].width = 25

    ws['A1'].value = 'Store'
    ws['B1'].value = 'ITEM'
    ws['C1'].value = 'CASE'


    #####FROM THIS POINT ON CODE IS FORMATTING on hand PAGE #######

    ws = wb['On Hand']

    ws['A1'].value = 'Store'
    ws['B1'].value = 'Item'
    ws['C1'].value = 'Display Size'
    ws['D1'].value = 'Season'
    ws['E1'].value = 'Case Size'
    ws['F1'].value = 'Delivery'
    ws['G1'].value = 'Credit'
    ws['H1'].value = 'Sales'
    ws['I1'].value = 'On Hand'
    ws['J1'].value = 'Case Qty'

    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 30
    ws.column_dimensions['C'].width = 15
    ws.column_dimensions['D'].width = 15
    ws.column_dimensions['E'].width = 15
    ws.column_dimensions['F'].width = 15
    ws.column_dimensions['G'].width = 15
    ws.column_dimensions['H'].width = 15
    ws.column_dimensions['I'].width = 15
    ws.column_dimensions['J'].width = 15


    #####FROM THIS POINT ON CODE IS FORMATTING No scans PAGE #######

    ws = wb['No Scan']
    ws.column_dimensions['B'].width = 30

    wb.save(filename)
