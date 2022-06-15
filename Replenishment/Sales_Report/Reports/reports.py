# from datetime import timedelta, date, datetime
import datetime
from Sales_Report.Replenishment.replenishment import *
from Sales_Report.Replenishment.initial_order import *
from Sales_Report.Report_Format.weekly_sales_report_format import ReportFormat
from openpyxl import load_workbook

class Reports:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

        # reports for both internal and external
        self.reports = ReportsData(self.store_type_input, self.store_setting)

        self.replenishment_report, self.on_hands_after_replen, self.replenishment_reasons = replenishment(self.store_type_input, self.store_setting)

        self.replenishment_len = len(self.replenishment_report) + 1
        #
        self.on_hand = self.reports.on_hands()
        self.no_scan = self.reports.no_scan(self.on_hand)

        self.sales_report = self.reports.sales_table()
        self.sales_report_len = self.reports.sales_report_len(self.sales_report)

        #reports for internal only
        self.on_hands_store, self.on_hands_display_size = initial_order(self.store_type_input, self.store_setting)


        #internal report variables
        self.initial_orders = self.store_setting.loc['initial_orders','values']
        self.size_build_up = self.store_setting.loc['size_build_up','values']
        self.high_return = self.store_setting.loc['high_return','values']
        self.store_sales_rank = self.store_setting.loc['store_sales_rank','values']
        self.item_approval = self.store_setting.loc['item_approval','values']
        self.store_program = self.store_setting.loc['store_program','values']

        # external report variables
        self.ytd_mask_sales_table = self.store_setting.loc['ytd_mask_sales_table','values']
        self.ytd_womask_sales_table = self.store_setting.loc['ytd_womask_sales_table','values']
        self.item_sales_rank = self.store_setting.loc['item_sales_rank','values']
        self.top20 = self.store_setting.loc['top20','values']

    def internal_report(self,filename):

        self.external_report(filename)

        ExcelWorkbook = load_workbook(filename)
        writer = pd.ExcelWriter(filename, engine='openpyxl')
        writer.book = ExcelWorkbook


        with pd.ExcelWriter(filename, engine='openpyxl') as writer:

            writer.book = ExcelWorkbook


            #calls for replenishment df and inserts to excel then formats it
            self.replenishment_reasons.to_excel(writer, sheet_name="Replenishment Reasons", index=False)

            if self.initial_orders == 1:

                #will need to replace the code for this
                self.on_hands_store.to_excel(writer, sheet_name="Potential Initial Orders",
                                                    index=False,
                                                    startcol=6,
                                                    startrow=1)

                self.on_hands_display_size.to_excel(writer, sheet_name="Potential Initial Orders",
                                                    index=False,
                                                    startcol=12,
                                                    startrow=1)

            if self.size_build_up == 1:
                pass

            if self.high_return ==1:

                on_hands = self.on_hand

                on_hands['return_ratio'] = (on_hands['credit'] * -1) / on_hands['deliveries']

                return_percentage = self.store_setting.loc['Return_Pecentage', 'values']

                on_hands = on_hands[(on_hands['return_ratio'] >= return_percentage)]

                on_hands.to_excel(writer, sheet_name="High Return", index=False)

            if self.store_sales_rank ==1:

                store_sales_rank = self.reports.store_sales_rank()

                store_sales_rank_len = len(store_sales_rank)

                store_sales_rank.to_excel(writer, sheet_name="Store Ranks", index=False, startcol=1)

            if self.item_approval ==1:

                item_approval = self.reports.item_approval()

                item_approval.to_excel(writer, sheet_name="Items Approved", index=False)

            if self.store_program ==1:

                store_program = self.reports.store_program()

                store_program.to_excel(writer, sheet_name="Store Program", index=False)

            writer.save()

        format = ReportFormat(filename, 1,1,store_sales_rank_len, self.store_setting)

        format.internal_report()



    def external_report(self, filename):

        print(filename)

        with pd.ExcelWriter(filename) as writer:



            # replenishment tab
            self.replenishment_report.to_excel(writer, sheet_name="Replenishment",
                                               index=False,
                                               columns=(
                                               'store_name', 'item', 'case', 'notes', 'case_qty', 'display_size'))

            #sales report tab

            if self.ytd_mask_sales_table == 1:

                sales_table_ytd_mask = self.reports.ytd_table_mask()

                sales_table_ytd_mask.to_excel(writer, sheet_name="Sales Report",
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

            if self.top20 == 1:   #stores that have ytd data will usually use top 20 stores that get weekly data will get normal sales table

                #grabs the sales table that was generated and gets the top 20 stores with the highest ytd
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
                                      'Store (#)', 'Current Week Sales', 'Previous Week Sales', '% +/- Change WOW',
                                      'Sales ($) 2022 Current Week', 'Sales ($) 2021 Current Week', '% +/- Change YOY',
                                      'Sales ($) 2022 YTD', 'Sales ($) 2021 YTD', '% +/- Change (YOY)')
                                      )


            if self.item_sales_rank == 1:

                item_sales_rank = self.reports.item_sales_rank()

                item_sales_rank.to_excel(writer, sheet_name="Sales Report",
                                         index=True,
                                         columns=('item_group_desc', 'sales'),
                                         header=('item', 'sales ($)'),
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



        # weekly_sales_report_format(filename, self.replenishment_len, self.sales_report_len)
        # ReportsFormat class instantiated for use later
        format = ReportFormat(filename, self.replenishment_len, self.sales_report_len, 1, self.store_setting)

        format.external_report()

