# 从xml提取titles
import xml.sax
import csv
path="titles.txt"
fp=open(path,'w',encoding="utf-8",newline="")
writer = csv.writer(fp)

paperTag = ('article', 'inproceedings', 'proceedings', 'book',
            'incollection', 'phdthesis', 'mastersthesis', 'www')


class Handler(xml.sax.ContentHandler):
    def __init__(self):
        self.cnt=0
        self.CurrentData = ""
        self.article=""
        self.flag=0

    def startElement(self, tag, attributes):
        self.CurrentData=tag
        if tag=="title":
            self.cnt+=1
            self.flag=1
        # if self.cnt>10:
        #     raise Exception

    def endElement(self, tag):
        self.CurrentData=""

    def characters(self, content):
        if self.flag==1:
            # print(content)
            writer.writerow([content])
            self.flag=0

if __name__ == '__main__':
    handler = Handler()
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    parser.setContentHandler(handler)
    parser.parse(r"dblp.xml")
    print(handler.cnt)
