import csv
import re

# 获得所有的tag
def open_file(url):
    s = ""
    with open(url, encoding="utf-8") as csv_file:
        last_title = ""
        for row in csv_file:
            # print(row)
            if len(row.split(sep=",")) == 0:
                continue
            title = row.split(sep=",")[0]

            if last_title != "" and title == last_title:
                continue
            pattern = "#.+"
            tags = re.findall(pattern, title)
            tag_arr = []
            if len(tags) >= 1:
                tag_arr = tags[0].split(" ")
            for index in range(0, len(tag_arr)):
                s += tag_arr[index]
                s += '\n'
            last_title = title
        print("Over!")

    print(s)
    tag_file = "../DataSet/hot_tag.txt"
    with open(tag_file, "w", encoding="utf-8") as f:
        f.write(s)

    s = ''
    with open(tag_file, encoding="utf-8") as f:
        for row in f:
            pattern = "#.+"
            ret = re.findall(pattern, row)
            if len(ret) >= 1:
                s += ret[0] + '\n'

    with open(tag_file, "w", encoding="utf-8") as f:
        f.write(s)


if __name__ == "__main__":
    URL = "../DataSet/热门短视频数据2w.csv"
    open_file(URL)