import os
os.environ["HOME"] = os.environ.get("USERPROFILE", "C:\\Users\\adar2")

from .cerbrus_crawler import CerberusCrawler

def lambda_handler(event=None, context=None):
    crawlers = [
        ("yohannof", CerberusCrawler("yohananof")),
        ("tivtam", CerberusCrawler("TivTaam")),
        ("doralon", CerberusCrawler("doralon")),
    ]

    total_uploaded = 0
    all_results = {}

    for name, crawler in crawlers:
        print(f"\n===== Starting crawl for: {name} =====")
        driver = crawler.get_driver()
        try:
            files = crawler.crawl(driver)
            print(f"{len(files)} files found by {name}")
            uploaded_count = 0
            for file in files:
                success = crawler.upload_file_to_s3(file, s3_key=name)
                if success:
                    uploaded_count += 1
            total_uploaded += uploaded_count
            all_results[name] = {
                "found": len(files),
                "uploaded": uploaded_count
            }
        except Exception as e:
            print(f"Error in crawler '{name}': {e}")
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
