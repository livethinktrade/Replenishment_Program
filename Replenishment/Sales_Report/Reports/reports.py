# from datetime import timedelta, date, datetime
import datetime
from Sales_Report.Replenishment.initial_order import *
from Sales_Report.Report_Format.ReportsFormat import SalesReportFormat, KrogerCorporateFormat
import numpy as np
from store_list import kroger_stores
import DbConfig
from IPython import display


class Reports:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

        # reports for both internal and external
        self.reports = ReportsData(self.store_type_input, self.store_setting)

        restock = Restock(self.store_type_input, self.store_setting)

        replen = restock.replenishment()

        self.replenishment_report = replen['replenishment']
        self.on_hands_after_replen = replen['on_hands_after_replenishment']
        self.replenishment_reasons = replen['replenishment_reasons']
        self.on_hands_store = replen['on_hands_store_case_total']
        self.on_hands_display_size_season = replen['on_hands_display_size_season']

        self.replenishment_len = len(self.replenishment_report) + 1
        #
        self.on_hand = self.reports.on_hands()
        self.no_scan = self.reports.no_scan(self.on_hand)

        self.sales_report = self.reports.sales_table()
        self.sales_report_len = self.reports.sales_report_len(self.sales_report)

        #internal report variables
        self.initial_orders = self.store_setting.loc['initial_orders', 'values']
        self.size_build_up = self.store_setting.loc['size_build_up', 'values']
        self.high_return = self.store_setting.loc['high_return', 'values']
        self.store_sales_rank = self.store_setting.loc['store_sales_rank', 'values']
        self.item_approval = self.store_setting.loc['item_approval', 'values']
        self.store_program = self.store_setting.loc['store_program', 'values']

        # external report variables
        self.ytd_mask_sales_table = self.store_setting.loc['ytd_mask_sales_table', 'values']
        self.ytd_womask_sales_table = self.store_setting.loc['ytd_womask_sales_table', 'values']
        self.item_sales_rank = self.store_setting.loc['item_sales_rank', 'values']
        self.top20 = self.store_setting.loc['top20', 'values']

    def internal_report(self, filename):

        # self.external_report(filename)
        #
        # ExcelWorkbook = load_workbook(filename)
        # writer = pd.ExcelWriter(filename, engine='openpyxl')
        # writer.book = ExcelWorkbook

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:

            # writer.book = ExcelWorkbook

            self.replenishment_report.to_excel(writer, sheet_name="Replenishment",
                                               index=False,
                                               columns=('store_name', 'item', 'case',
                                                        'notes', 'case_qty', 'display_size'))

            try:

                replen_pivot = pd.pivot_table(self.replenishment_report,
                                              values='case',
                                              index=['initial', 'store'],
                                              columns='item',
                                              aggfunc=np.sum,
                                              fill_value= '',
                                              margins= True)
                replen_pivot = replen_pivot.reset_index()

                replen_pivot.to_excel(writer, sheet_name='Front Desk', index=False)

            except Exception as e:
                print(f'Error: {e}')
                pass

            self.on_hand.to_excel(writer, sheet_name="On Hand", index=False)

            # calls for replenishment df and inserts to excel then formats it
            self.replenishment_reasons.to_excel(writer, sheet_name="Replenishment Reasons", index=False)

            if self.initial_orders == 1:

                # will need to replace the code for this
                self.on_hands_store.to_excel(writer, sheet_name="Potential Initial Orders",
                                                    index=False,
                                                    startcol=6,
                                                    startrow=1)

                self.on_hands_display_size_season.to_excel(writer, sheet_name="Potential Initial Orders",
                                                    index=False,
                                                    startcol=12,
                                                    startrow=1)

            if self.size_build_up == 1:
                pass

            if self.high_return == 1:

                on_hands = self.on_hand

                on_hands['return_ratio'] = (on_hands['credit'] * -1) / on_hands['deliveries']

                return_percentage = self.store_setting.loc['Return_Pecentage', 'values']

                on_hands = on_hands[(on_hands['return_ratio'] >= return_percentage)]

                on_hands.to_excel(writer, sheet_name="High Return", index=False)

            if self.store_sales_rank == 1:

                store_sales_rank = self.reports.store_sales_rank()

                store_sales_rank_len = len(store_sales_rank)

                store_sales_rank.to_excel(writer, sheet_name="Store Ranks", index=False, startcol=1)

            if self.item_approval == 1:

                item_approval = self.reports.item_approval()

                item_approval.to_excel(writer, sheet_name="Items Approved", index=False)

            if self.store_program == 1:

                store_program = self.reports.store_program()

                store_program.to_excel(writer, sheet_name="Store Program", index=False)

            writer.save()

        format = SalesReportFormat(filename, self.replenishment_len, 1, store_sales_rank_len, self.store_setting)

        format.internal_report()

    def external_report(self, filename):

        print(filename)

        with pd.ExcelWriter(filename) as writer:

            # replenishment tab
            self.replenishment_report.to_excel(writer,
                                               sheet_name="Replenishment",
                                               index=False,
                                               columns=('store_name', 'item', 'case'))

            # sales report tab

            if self.ytd_mask_sales_table == 1:

                sales_table_ytd_mask = self.reports.ytd_table_mask()

                sales_table_ytd_mask.to_excel(writer,
                                              sheet_name="Sales Report",
                                              index=False,
                                              columns=('ytd_current', 'ytd_previous', 'yoy_change'),
                                              header=('Division Sales current YTD($) +Mask',
                                                      'Division Sales previous YTD($) +Mask', '% +/- YOY Change'),
                                              startrow=10,
                                              startcol=12)

            if self.ytd_womask_sales_table == 1:

                sales_table_ytd_wo_mask = self.reports.ytd_table_no_mask()

                sales_table_ytd_wo_mask.to_excel(writer, sheet_name="Sales Report",
                                                 index=False,
                                                 columns=('ytd_current', 'ytd_previous', 'yoy_change'),
                                                 header=('Division Sales current YTD($) -Mask',
                                                         'Division Sales previous YTD($) -Mask', '% +/- YOY Change'),
                                                 startrow=5,
                                                 startcol=12)

            # table that shows stores supported weekly and month sales
            sales_table_ytd_wo_mask.to_excel(writer, sheet_name="Sales Report",
                                             index=False,
                                             columns=('store_supported', 'avg_wk_store', 'avg_month_store'),
                                             header=('(#) Stores Supported', 'Avg Sales Per Wk/Store ($)',
                                                     'Avg Sales Per Mo/Store ($)'),
                                             startrow=0,
                                             startcol=12)

            if self.top20 == 1:
                '''
                
                stores that have ytd data will usually use top 20 stores that 
                get weekly data will get normal sales table
                
                grabs the sales table that was generated and gets the top 20 stores with the highest ytd
                
                '''

                sales_report = self.sales_report.drop(columns=['current_week',
                                                               'previous_week',
                                                               'wow_sales_percentage',
                                                               'current_week',
                                                               'previous_year_week',
                                                               'yoy_sales_percentage',
                                                               'ytd_2021',
                                                               'yoy_sales_percentage'])

                sales_report = sales_report.dropna()

                sales_report = sales_report.head(20)

                sales_report.to_excel(writer,
                                      sheet_name="Sales Report",
                                      index=False,
                                      header=('Store (#)', 'Sales ($) 2022 YTD'),
                                      startrow=1,
                                      startcol=0)
            else:

                self.sales_report.to_excel(writer,
                                           sheet_name="Sales Report",
                                           index=False,
                                           header=(
                                                   'Store (#)', 'Current Week Sales', 'Previous Week Sales',
                                                   '% +/- Change WOW','Sales ($) 2022 Current Week',
                                                   'Sales ($) 2021 Current Week', '% +/- Change YOY',
                                                   'Sales ($) 2022 YTD', 'Sales ($) 2021 YTD', '% +/- Change (YOY)')
                                           )

            if self.item_sales_rank == 1:

                item_sales_rank = self.reports.item_sales_rank()

                item_sales_rank.to_excel(writer, sheet_name="Sales Report",
                                         index=True,
                                         columns=('item_group_desc', 'sales', 'sales per active store', 'active stores','percent_of_total_sales'),
                                         header=('item', 'sales ($)', 'Sales Per Active Store', 'Active Stores', '% of Annual Sales'),
                                         startrow=14,
                                         startcol=12)

            # Qty Sales Report for cincinatti only
            if self.store_type_input == 'kroger_cincinatti':

                sales_table_qty_report = self.reports.sales_table_qty()
                sales_rank_table = self.reports.item_sales_rank_qty()

                sales_table_qty_report.to_excel(writer,
                                                sheet_name="Qty Report",
                                                index=False,
                                                header=(
                                                    'Store (#)', 'Current Week Qty Sold', 'Previous Week Qty Sold',
                                                    '% +/- Change WOW', 'Qty Sold 2022 Current Week',
                                                    'Qty Sold 2021 Current Week', '% +/- Change YOY',
                                                    'Qty Sold 2022 YTD', 'Qty Sold 2021 YTD', '% +/- Change (YOY)')
                                                )

                sales_rank_table.to_excel(writer, sheet_name="Qty Report",
                                          index=True,
                                          columns=('item_group_desc', 'units_sold', 'units sold per active store', 'active stores',
                                                   'percent_of_total_sales'),
                                          header=('item', 'Units Sold', 'Units Sold Per Active Store', 'Active Stores',
                                                  '% of Annual Sales'),
                                          startrow=14,
                                          startcol=12)

            # no scans tab
            in_season_setting = self.store_setting.loc['In_Season', 'values']

            if in_season_setting == 1:

                header = ('store', 'item_group_desc', 'last shipped', 'weeks age')

            else:

                header = ('store', 'item')

            self.no_scan.to_excel(writer, sheet_name="No Scan", index=False, header=header)

            # on hands tab
            self.on_hand.to_excel(writer, sheet_name="On Hand", index=False)

        '''
        
        weekly_sales_report_format(filename, self.replenishment_len, self.sales_report_len)
        ReportsFormat class instantiated for use later
        
        '''
        format = SalesReportFormat(filename, self.replenishment_len, self.sales_report_len, 1, self.store_setting)

        format.external_report()

    def kroger_corporate_report(self):

        today_date = datetime.date.today()
        today_date = today_date.strftime("%b-%d-%Y")

        file_name = f'Kroger Corporate {today_date}.xlsx'

        corporate_period_table_lengths_for_outlining = {}
        sales_table_lengths_for_outlining = {}

        with pd.ExcelWriter(file_name) as writer:

            report_data = ReportsData(self.store_type_input, self.store_setting)

            period_table = report_data.kroger_sales_by_period()

            # puts summary data by period for all divisions in Excel

            kroger_sales_overview = report_data.kroger_corporate_sales_overview()

            kroger_sales_overview.to_excel(writer,
                                           sheet_name=f"Corporate Overview",
                                           index=True,
                                           startrow=4,
                                           startcol=3)

            period_table.to_excel(writer,
                                  sheet_name=f"Corporate Overview",
                                  index=True,
                                  startrow=20,
                                  startcol=3
                                  )

            corporate_period_table_lengths_for_outlining['Overall Period Summary'] = {'start_row': 21,
                                                                                      'table_length': len(period_table)}

            # Puts summary data of sales ($ and qty) by division by period into the report

            # start row is set to 33 because other data is at this location.

            start_row = 33

            for store_type_input in kroger_stores:

                store_setting = pd.read_excel(
                    rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{store_type_input}\{store_type_input}_store_setting.xlsm',
                    sheet_name='Sheet2',
                    header=None,
                    index_col=0,
                    names=('setting', 'values'))

                report_data = ReportsData(store_type_input, store_setting)

                sales_table = report_data.kroger_division_sales()

                # takes the store_input variable and takes the 'kroger_' out from the str and
                # then capitalizes the 1 character
                name = store_type_input.split('kroger_')[1].capitalize()

                # find length of table for formatting purposes later
                sales_table_lengths_for_outlining[f'{name}'] = len(sales_table) + 1

                sales_table.to_excel(writer,
                                     sheet_name=f"{name}",
                                     index=False)

                # Gets the summary data for sales $ for each period for a given kroger division
                division_period_table = report_data.kroger_division_sales_by_period()

                division_period_table.to_excel(writer,
                                               sheet_name='Corporate Overview',
                                               startcol=3,
                                               startrow=start_row,
                                               index=True)

                # find length of table for formatting purposes added plus 1 because openpyxl is not 0 indexed based
                corporate_period_table_lengths_for_outlining[f'{name}'] = {'start_row': start_row+1,
                                                                           'table_length': len(division_period_table)}

                start_row += len(division_period_table) + 4

            corporate_period_table_lengths_for_outlining['max row'] = start_row

        kroger_format = KrogerCorporateFormat(file_name,
                                              corporate_period_table_lengths_for_outlining,
                                              sales_table_lengths_for_outlining)

        kroger_format.kroger_corporate_format()









