import re
import pandas as pd

if __name__=="__main__":
    input_path=r"C:\Users\Mika\Desktop\毕业设计\数据集\data.txt"
    file = open(input_path, 'r', encoding='utf-8')
    text = file.read()
    urls=re.findall(r'<li><a href="(.*?)">',text)
    titles=re.findall(r'<li><a href=.*?>(.*?)</a>',text)
    authors=re.findall(r'<i>(.*?)</i></li>',text)
    print(len(urls),len(titles),len(authors))

    table=pd.DataFrame({'url': urls, 'title': titles,'author':authors})
    output_path=r"C:\Users\Mika\Desktop\毕业设计\数据集\data.csv"
    table.to_csv(output_path)

