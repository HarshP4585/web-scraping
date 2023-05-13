import requests
import csv
from bs4 import BeautifulSoup
import threading
import os

URL_FORMAT = "https://gseb.org/522lacigam/sci/B{0}/{1}/B{0}{1}{2}.html"

mapping = {
    "054": "Physics",
    "055": "Physics Practical",
    "052": "Chemistry",
    "053": "Chemistry Practical",
    "056": "Biology",
    "057": "Biology Practical",
    "050": "Mathematics",
    "006": "English FL",
    "013": "English SL",
    "001": "Gujarati FL",
    "002": "Hindi FL",
    "003": "Marathi FL",
    "004": "Urdu FL",
    "005": "Sindhi FL",
    "007": "Tamil FL",
    "008": "Gujarati SL",
    "009": "Hindi SL",
    "129": "Sanskrit SL",
    "130": "Persian SL",
    "131": "Arabic SL",
    "132": "Pakrit SL",
    "331": "Computer FL",
    "332": "Computer Practical FL",
    "Total": "Total Marks"
}

fieldnames = [
    "id", "name", "school index", "sid", "group", "overall %", "science %", "theory %", "result",
    "Gujarati FL",
    "Gujarati FL Grade",
    "Hindi FL",
    "Hindi FL Grade",
    "Marathi FL",
    "Marathi FL Grade",
    "Urdu FL",
    "Urdu FL Grade",
    "Sindhi FL",
    "Sindhi FL Grade",
    "English FL",
    "English FL Grade",
    "Tamil FL",
    "Tamil FL Grade",
    "Gujarati SL",
    "Gujarati SL Grade",
    "Hindi SL",
    "Hindi SL Grade",
    "English SL",
    "English SL Grade",
    "Mathematics",
    "Mathematics Grade",
    "Chemistry",
    "Chemistry Grade",
    "Chemistry Practical",
    "Chemistry Practical Grade",
    "Physics",
    "Physics Grade",
    "Physics Practical",
    "Physics Practical Grade",
    "Biology",
    "Biology Grade",
    "Biology Practical",
    "Biology Practical Grade",
    "Sanskrit SL",
    "Sanskrit SL Grade",
    "Persian SL",
    "Persian SL Grade",
    "Arabic SL",
    "Arabic SL Grade",
    "Pakrit SL",
    "Pakrit SL Grade",
    "Computer FL",
    "Computer FL Grade",
    "Computer Practical FL",
    "Computer Practical FL Grade",
    "Total Marks",
    "Total Grade"
]

# for b in range(20, 33):
#     for group in range(0, 100):

#         group_str = str(group)
#         if group < 10:
#             group_str = group_str.zfill(2)

def execute(str_a, csv_writer):
    b, group_str = list(map(''.join, zip(*[iter(str_a)]*2)))   
    for number in range(0, 100):
        data = []
        to_cache = True
                
        number_str = str(number)
        if number < 10:
            number_str = number_str.zfill(2)
        
        content = None
        if os.path.exists("./cache/{}.html".format(str_a + number_str)):
            content = open("./cache/{}.html".format(str_a + number_str), "r").read().strip()
            to_cache = False
        else:
            r = requests.get(URL_FORMAT.format(b, group_str, number_str), verify=False)
            content = r.text
        soup = BeautifulSoup(content, 'html.parser')
        counter = 0
        for i in soup.find_all("tr"):
            if "Sub Name" in i.get_text():
                continue
            span = i.find_all("span")
            len_span = len(span)
            if len_span in [1, 2]:
                for j in span:
                    counter += 1
                    data.append(j.get_text().split(":")[-1].strip())
            elif len_span > 0:
                for field in fieldnames[counter::2]:
                    sub = mapping.get(span[0].get_text().split(" ")[0].strip())
                    if sub == field:
                        data.append(span[-2].get_text().strip())
                        data.append(span[-1].get_text().strip())
                        counter += 2
                        break
                    else:
                        data.append("NA")
                        data.append("NA")
                    counter += 2

        if to_cache:
            with open("./cache/{}.html".format(str_a + number_str), "a") as f:
                f.write(r.text)

        csv_writer.writerow(data)

threads_list = []

# 2000, 3299
for i in range(2000, 3299, 100):
    del threads_list
    threads_list = []

    for j in range(0, 100):
        start = i + j
        str_start =str(start)
        
        f = open("./data/data_{}(0-99).csv".format(start), "a")
        file_writer = csv.writer(f)
        file_writer.writerow(fieldnames)
        
        t = threading.Thread(target=execute, args=[str_start, file_writer])
        threads_list.append(t)
        t.start()
        
    for thread in threads_list:
        thread.join()
