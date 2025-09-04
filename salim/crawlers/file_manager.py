import os
import re
import shutil
import time
from datetime import datetime


class FileManager:
    def __init__(self, download_dir):
        self.download_dir = download_dir
    
    def extract_datetime_from_filename(self, filename):
        """Extract date and time from filename for S3 naming"""
        try:
            # Look for date-time pattern in filename (YYYYMMDDHHMMSS or similar)
            # Examples: Price7290492000005-001-524-20250901-230216.gz
            #          Promo7290873255550-523-202509012210.gz
            
            # Pattern 1: YYYY-MM-DD-HHMMSS
            pattern1 = r'(\d{8})-(\d{6})'
            match1 = re.search(pattern1, filename)
            if match1:
                date_part = match1.group(1)  # YYYYMMDD
                time_part = match1.group(2)  # HHMMSS
                return f"{date_part}_{time_part}"
            
            # Pattern 2: YYYYMMDDHHMMSS (concatenated)
            pattern2 = r'(\d{12})'
            match2 = re.search(pattern2, filename)
            if match2:
                datetime_str = match2.group(1)
                date_part = datetime_str[:8]   # YYYYMMDD
                time_part = datetime_str[8:12] + "00"  # HHMM -> HHMMSS
                return f"{date_part}_{time_part}"
            
            # Fallback: use current datetime
            now = datetime.now()
            return now.strftime("%Y%m%d_%H%M%S")
            
        except Exception as e:
            # Fallback: use current datetime
            now = datetime.now()
            return now.strftime("%Y%m%d_%H%M%S")
    
    def extract_file_info(self, filename):
        """Extract branch identifier and date from filename"""
        try:
            # New pattern: PriceFull7290492000005-001-560-20250902-001706.gz
            # Parts:        [name]-[3digits]-[3digits]-[date]-[time].gz
            #               branch_id = second 3-digit number (560)
            #               date = the YYYYMMDD part (20250902)
            
            parts = filename.split('-')
            print(f"Filename parts: {parts}")
            
            # Look for date part (starts with 2025)
            date_index = -1
            time_index = -1
            
            for i, part in enumerate(parts):
                if len(part) >= 8 and part[:4] == '2025':
                    # Remove .gz if present
                    clean_part = part.replace('.gz', '')
                    if len(clean_part) >= 8 and clean_part.isdigit():
                        date_index = i
                        # Check if next part is time (HHMM format)
                        if i + 1 < len(parts):
                            next_part = parts[i + 1].replace('.gz', '')
                            if len(next_part) >= 4 and next_part.isdigit():
                                time_index = i + 1
                        break
            
            if date_index == -1:
                print(f"Could not find date part in filename: {filename}")
                return None, None, None
            
            date_part = parts[date_index].replace('.gz', '')[:8]  # YYYYMMDD
            
            # Extract time part if exists
            if time_index != -1:
                time_part = parts[time_index].replace('.gz', '')[:4]  # HHMM
                full_timestamp = f"{date_part}{time_part}"  # YYYYMMDDHHMM
                print(f"Found separate date: {date_part}, time: {time_part}")
            else:
                # Check if date part contains time (longer than 8 digits)
                date_timestamp = parts[date_index].replace('.gz', '')
                if len(date_timestamp) > 8:
                    full_timestamp = date_timestamp  # Use as is
                    print(f"Found combined date+time: {full_timestamp}")
                else:
                    full_timestamp = date_part  # Only date available
                    print(f"Found only date: {date_part}")
            
            # Check patterns based on position relative to date
            # Pattern 1: xxxxxxxx-001-054-20250902-1102 (two 3-digit numbers before date)
            # Pattern 2: xxxxxxx-340-20250902-1102 (one 3-digit number before date)
            
            if date_index >= 3:  # Pattern 1: two 3-digit numbers before date
                first_three_digit = parts[date_index-2]
                second_three_digit = parts[date_index-1]
                
                print(f"Pattern 1 - First 3-digit: {first_three_digit}, Second 3-digit: {second_three_digit}, Date: {date_part}")
                
                # Validate both are 3-digit numbers
                if (len(first_three_digit) == 3 and first_three_digit.isdigit() and
                    len(second_three_digit) == 3 and second_three_digit.isdigit()):
                    
                    branch_id = second_three_digit  # Use second 3-digit as branch ID
                    print(f"Pattern 1 - Extracted branch_id: {branch_id}, date: {date_part}, full_timestamp: {full_timestamp}")
                    return branch_id, date_part, full_timestamp
                    
            elif date_index >= 2:  # Pattern 2: one 3-digit number before date
                single_three_digit = parts[date_index-1]
                
                print(f"Pattern 2 - Single 3-digit: {single_three_digit}, Date: {date_part}")
                
                # Validate it's a 3-digit number
                if len(single_three_digit) == 3 and single_three_digit.isdigit():
                    branch_id = single_three_digit  # Use single 3-digit as branch ID
                    print(f"Pattern 2 - Extracted branch_id: {branch_id}, date: {date_part}, full_timestamp: {full_timestamp}")
                    return branch_id, date_part, full_timestamp
            
            print(f"Could not extract info from filename: {filename}")
            return None, None, None
        except Exception as e:
            print(f"Error extracting file info from {filename}: {e}")
            return None, None, None
    
    def move_file_to_branch_folder(self, original_filename, supermarket_name, branch_folder, new_filename):
        """Move downloaded file to branch-specific folder with new name"""
        try:
            source_file = os.path.join(self.download_dir, original_filename)
            
            # Create branch folder structure
            branch_dir = os.path.join(self.download_dir, supermarket_name, branch_folder)
            os.makedirs(branch_dir, exist_ok=True)
            
            dest_file = os.path.join(branch_dir, new_filename)
            
            if os.path.exists(source_file):
                shutil.move(source_file, dest_file)
                print(f"Moved {original_filename} -> {branch_folder}/{new_filename}")
                return dest_file
            else:
                print(f"Source file not found: {source_file}")
                return None
                
        except Exception as e:
            print(f"Error moving file: {e}")
            return None
    
    def wait_for_download(self, file_name, timeout=10):
        """Wait for a specific file to download completely"""
        try:
            expected_file = os.path.join(self.download_dir, file_name)
            temp_file = expected_file + ".crdownload"  # Chrome temp download file
            
            # Wait for download to start or complete
            for i in range(timeout):
                # Check for exact filename
                if os.path.exists(expected_file):
                    return True
                
                # Check for files with similar names (Chrome adds " (1)" etc.)
                import glob
                pattern = os.path.join(self.download_dir, file_name.replace('.gz', '*'))
                matches = glob.glob(pattern)
                if matches:
                    return True
                
                # Check if download in progress
                if os.path.exists(temp_file):
                    pass  # Download in progress
                
                time.sleep(1)
            
            # Final check for any similar files
            import glob
            pattern = os.path.join(self.download_dir, file_name.replace('.gz', '*'))
            matches = glob.glob(pattern)
            if matches:
                return True
            
            print(f"Download timeout for: {file_name}")
            return False
            
        except Exception as e:
            print(f"Error waiting for download: {e}")
            return False