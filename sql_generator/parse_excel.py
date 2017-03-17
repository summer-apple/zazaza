from openpyxl import load_workbook


class ExcelParser:
    def __init__(self,filename):
        if not filename.endswith('.xls'):
            if not filename.endswith('.xlsx'):
                raise Exception('ONLY EXCEL FILE ACCEPT!')
        self.wb = load_workbook(filename=filename, read_only=True)

        for ws in [self.wb[sn] for sn in self.wb.sheetnames]:
            for row in ws.rows:
                tmp = ''
                for cell in row:

                    tmp = tmp + str(cell.value)+'\t' if cell.value is not None else tmp+'None\t'
                print(tmp)



    def create_db(self):
        head = self.wb['HEAD']
        print(head.rows)
        db_name,db_desp,charset = head.rows[1]
        print(db_name,db_desp,charset)


if __name__ == '__main__':
    ep = ExcelParser('sql.xlsx')
    ep.create_db()