import csv
import re

# 去除unicode，只有utf-8
def open_file(url):
    s = ""
    with open(url, encoding="utf-8") as csv_file:
        for row in csv_file:
            row = row.encode(encoding='UTF-8',errors='strict')
            row = re.sub(r'\\u.{4}', '', row.__repr__())
            row = eval(row)
            s += row.decode(encoding='UTF-8',errors='strict')
            print(row.decode(encoding='UTF-8',errors='strict'))
        print("Over!")

    with open(url, "w", encoding="utf-8") as f:
        f.write(s)


if __name__ == "__main__":
    URL = "../DataSet/result.csv"
    open_file(URL)