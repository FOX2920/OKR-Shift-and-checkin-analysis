
import os

target_file = 'goal_test.py'
append_file = 'goal_test_calculation.py'

try:
    with open(target_file, 'rb') as f:
        content = f.read()

    # Marker based on the last known valid line
    # "print("Không thể tạo báo cáo tổng hợp do thiếu dữ liệu")"
    # Using a shorter unique byte sequence from that line to avoid encoding mismatch issues
    # "bao cao tong hop do thieu du lieu" (approximate or just part of the variable name)
    # The line is: print("Không thể tạo báo cáo tổng hợp do thiếu dữ liệu")
    # Let's search for "if comprehensive_report:" which is safer ASCII
    marker = b'if comprehensive_report:'
    
    idx = content.rfind(marker)
    
    if idx != -1:
        # Find the end of the else block
        # It goes:
        # if comprehensive_report:
        #     print_report(comprehensive_report)
        # else:
        #     print("...")
        
        # Search for the print statement after the else
        print_marker = b'print("Kh' 
        print_idx = content.find(print_marker, idx)
        
        if print_idx != -1:
            # Find the newline after the print statement
            end_idx = content.find(b'\n', print_idx)
            if end_idx != -1:
                clean_content = content[:end_idx+1] # Include the newline
                
                with open(append_file, 'rb') as f2:
                    append_txt = f2.read()
                    
                with open(target_file, 'wb') as f_out:
                    f_out.write(clean_content + b'\n' + append_txt)
                
                print("Successfully repaired and patched goal_test.py")
            else:
                 print("Could not find end of print line")
        else:
            print("Could not find print statement in else block")
    else:
        print("Could not find 'if comprehensive_report:' marker")

except Exception as e:
    print(f"Error: {e}")
