#!/usr/bin/env python3
"""
Test script to verify crawler setup
"""

import sys
import boto3
from pathlib import Path
import gzip

def test_aws_credentials():
    """Test AWS credentials"""
    print("Testing AWS credentials...")
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f" AWS credentials valid!")
        print(f"   Account: {identity['Account']}")
        print(f"   User: {identity['Arn']}")
        return True
    except Exception as e:
        print(f" AWS credentials error: {e}")
        print("   Run: aws configure")
        return False

def test_s3_access(bucket_name):
    """Test S3 bucket access"""
    print(f"\nTesting S3 bucket access: {bucket_name}")
    try:
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket=bucket_name)
        print(f" S3 bucket accessible!")
        return True
    except Exception as e:
        print(f" S3 bucket error: {e}")
        return False

def test_file_compression():
    """Test file compression"""
    print("\nTesting file compression...")
    try:
        # Create test file
        test_file = Path("test.txt")
        test_file.write_text("Test content for compression")
        
        # Compress
        gz_file = Path("test.txt.gz")
        with open(test_file, 'rb') as f_in:
            with gzip.open(gz_file, 'wb') as f_out:
                f_out.writelines(f_in)
        
        # Verify
        with gzip.open(gz_file, 'rt') as f:
            content = f.read()
        
        # Cleanup
        test_file.unlink()
        gz_file.unlink()
        
        print(" File compression working!")
        return True
    except Exception as e:
        print(f" Compression error: {e}")
        return False

def test_selenium():
    """Test Selenium setup"""
    print("\nTesting Selenium/Chrome...")
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        
        driver = webdriver.Chrome(options=options)
        driver.get("https://www.google.com")
        title = driver.title
        driver.quit()
        
        print("✅ Selenium/Chrome working!")
        print(f"   Test page title: {title}")
        return True
    except Exception as e:
        print(f" Selenium error: {e}")
        print("   Make sure Chrome and ChromeDriver are installed")
        return False

def main():
    """Run all tests"""
    print("🔧 Israeli Supermarket Crawler - Setup Test\n")
   
    bucket_name = input("Enter your S3 bucket name: ").strip()
    

    tests = [
        test_aws_credentials(),
        test_s3_access(bucket_name) if bucket_name else False,
        test_file_compression(),
        test_selenium()
    ]
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY:")
    print(f"Passed: {sum(tests)}/{len(tests)}")
    
    if all(tests):
        print("\n✅ All tests passed! You're ready to run the crawler.")
        print(f"\nNext step: python crawler.py")
    else:
        print("\n❌ Some tests failed. Please fix the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
