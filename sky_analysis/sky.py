import openpyxl
from openpyxl import load_workbook
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

class Analysiser:
    def __init__(self):
        years = [range(2006,2017)]
        df = pd.read_excel('data_o.xlsx', sheetname=0, header=0,  parse_cols=[9, 10, 23, 32, 45, 60])
        # df = df[(df['PY'] not in years)]
        self.df = df


    # def parse(self):
    #     .wb = load_workbook('data_min.xlsx')
    #     sheet = wb.get_sheet_by_name('all')
    #     new_wb = openpyxl.Workbook()
    #     new_sheet = new_wb.create_sheet('simple')
    #     new_sheet.append(['TI', 'SO', 'C1', 'TC', 'PY', 'UT'])
    #
    #
    #     for row in list(sheet.rows)[2:100]:
    #         r = [c.value for c in row]
    #         r_min = [r[9],r[10],r[23],r[32],r[45],r[60]]
    #         print(r_min)
    #         new_sheet.append(r_min)
    #     new_wb.save('export.xlsx')

    def parse2(self):

        self.df.ExcelWriter('output.xls')





    def fun1(self):
        df = self.df
        print(df.head())
        plt.figure(figsize=(9, 6))
        plt.scatter(df['PY'], df['TC'], s=25, alpha=0.4, marker='o')
        # T:散点的颜色
        # s：散点的大小
        # alpha:是透明程度
        plt.show()



if __name__ == '__main__':
    a = Analysiser()

    a.fun1()