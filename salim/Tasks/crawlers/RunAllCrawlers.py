
from SuperSapirCrawler import SuperSapirCrawler

from ZolVeBegadolCrawler import ZolVeBegadolCrawler
from YohannoffCrawler import YohananofCrawler

def run_all():
    SuperSapirCrawler("https://supersapir.binaprojects.com/Main.aspx", "SuperSapir").run()
    ZolVeBegadolCrawler("https://zolvebegadol.binaprojects.com/Main.aspx", "ZolVeBegadol").run()
    YohananofCrawler("https://url.publishedprices.co.il/login", "yohananof").run()

if __name__ == "__main__":
    run_all()
