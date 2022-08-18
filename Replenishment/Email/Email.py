import win32com.client as win32
from datetime import datetime
import os
from PIL import ImageGrab


# all of the inputs
store_type_input = 'Atlanta'
no_scans_current_week = 13
no_scans_previous_week = 20
ytd_sales = 28081
dollar_per_store = 390
active_stores = 48

team_name = ['Kimberly', 'Laura', 'Susan']
team_name = ', '.join(str(e) for e in team_name)
leader_email = 'kimberly.cortez@kroger.com'
div_cc1 = 'laura.a.green@kroger.com'
div_cc2 = 'susanatlanta.jones@kroger.com'
winwin_cc1 = 'kevin@winwinproducts.com'
winwin_cc2 = 'michael@winwinproducts.com'

to_email = 'michael@winwinproducts.com'
cc_email = 'michael@winwinproducts.com'

# find path of external sheet
workbook_path = r'C:\Users\User1\OneDrive - winwinproducts.com\Grocery Sales Reports\8. August\08-15-2022\atlanta\kroger_atlanta_external_sales_report_Aug-15-2022.xlsx'

# creates excel object, opens workbook and selects the sheet needed to create png
excel = win32.Dispatch('Excel.Application')
wb = excel.Workbooks.Open(workbook_path)
sheet = wb.Sheets['Sales Report']

# creating png file of the sales table
# Also need to make the next line dynamic. size of the sales table will vary.
sales_table_png = sheet.Range('A1:J49')
sales_table_png.CopyPicture(Appearance=1, Format=2)
ImageGrab.grabclipboard().save(f'{store_type_input}_sales_table.png')

# creating png file for YTD data

ytd_png = sheet.Range('M5:M7')
ytd_png.CopyPicture(Appearance=1, Format=2)
ImageGrab.grabclipboard().save(f'{store_type_input}_ytd.png')

# creating png file for the no scans

sheet = wb.Sheets['No Scan']
ytd_png = sheet.Range('A1:B14')
ytd_png.CopyPicture(Appearance=1, Format=2)
ImageGrab.grabclipboard().save(f'{store_type_input}_no_scan.png')


excel.Quit()

if no_scans_previous_week > no_scans_current_week:

    no_scan_message = f"""
    {no_scans_previous_week-no_scans_current_week} stores have come off the No Scans List. <br>  
    There are now only {no_scans_current_week} remaining, list is at the bottom for you. """

elif no_scans_previous_week < no_scans_current_week:
    raise Exception('No Scans Increased compared to the previous week')

else:

    no_scan_message = f"""
       There is still {no_scans_current_week} stores remaining on the No Scans List. The list is at the bottom for you.
    """


outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)

mail.Subject = f'{store_type_input} Division  Sales and Replenishment Report ' + datetime.now().strftime('%#d %b %Y')
mail.To = "michael@winwinproducts.com"

sales_table_attachment = mail.Attachments.Add(os.getcwd() + f"\\{store_type_input}_sales_table.png")
sales_table_attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", f"{store_type_input}_sales_table_img")

ytd_attachment = mail.Attachments.Add(os.getcwd() + f"\\{store_type_input}_ytd.png")
ytd_attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", f"{store_type_input}_ytd_img")

no_scan_attachment = mail.Attachments.Add(os.getcwd() + f"\\{store_type_input}_no_scan.png")
no_scan_attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", f"{store_type_input}_no_scan_img")


#
mail.HTMLBody = fr"""
TO:   {leader_email}<br>
CC:   {div_cc1}; {div_cc2}; {winwin_cc1}; {winwin_cc2}<br><br><br>

    <p style="font-size:17.5px; font-family:Century Gothic;">

        Hello {store_type_input} Division,<br><br>
        Now showing YTD ${round(ytd_sales/1000)}K in total sales volume— ${round(dollar_per_store)}/store for {active_stores} active stores.<br><br>
        {no_scan_message}<br><br><br>
        Full report is attached, have a wonderful week. Thanks!<br><br>
        Greg Greer<br>
        WinWin Products Inc.<br><br><br>
        
        Kroger {store_type_input} Division Team: {team_name}<br><br>
        
        <img src="cid:{store_type_input}_ytd_img"></img><br><br>
        <img src="cid:{store_type_input}_sales_table_img"></img><br><br>
        Only {no_scans_current_week} stores remain on No Scan List<br><br>
        <img src="cid:{store_type_input}_no_scan_img"></img><br><br>

    </p>

"""

mail.Display()

if os.path.exists(os.getcwd() + f"\\{store_type_input}_sales_table.png"):
    os.remove(os.getcwd() + f"\\{store_type_input}_sales_table.png")
if os.path.exists(os.getcwd() + f"\\{store_type_input}_ytd.png"):
    os.remove(os.getcwd() + f"\\{store_type_input}_ytd.png")
if os.path.exists(os.getcwd() + f"\\{store_type_input}_no_scan.png"):
    os.remove(os.getcwd() + f"\\{store_type_input}_no_scan.png")
