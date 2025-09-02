import os
os.environ["HOME"] = os.environ.get("USERPROFILE", "C:\\Users\\adar2")

from .cerbrus_crawler import CerberusCrawler

def lambda_handler(event=None, context=None):
    crawlers = [
        #("yohannof", CerberusCrawler("yohananof")),
        #("tivtam", CerberusCrawler("TivTaam")),
        #("doralon", CerberusCrawler("doralon")),
        #("osherad", CerberusCrawler("osherad")),
        #("freshmarket", CerberusCrawler("freshmarket")),
        ("RamiLevi", CerberusCrawler("RamiLevi")),
   
    ]

    total_uploaded = 0
    all_results = {}
    

    for name, crawler in crawlers:
        print(f"\n===== Starting crawl for: {name} =====")
        print(f"Username: {crawler.user_name}")
        driver = crawler.get_driver()
        try:
            uploaded_count = crawler.crawl(driver, name)
            print(f"{uploaded_count} files found by {name}")
            total_uploaded += uploaded_count
            all_results[name] = {
                "found": uploaded_count,
                "uploaded": uploaded_count
            }
        except Exception as e:
            print(f"Error in crawler '{name}': {e}")
            import traceback
            print(f"Full traceback for {name}:")
            traceback.print_exc()
            all_results[name] = {
                "error": str(e)
            }
        finally:
            driver.quit()

    return {
        'statusCode': 200,
        'body': {
            "total_uploaded": total_uploaded,
            "details": all_results
        }
    }

if __name__ == "__main__":
    result = lambda_handler()
    print("\n=== Final Result ===")
    print(result)
