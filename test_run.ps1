# Test script to run main.py with exit command
.\linkedin_env\Scripts\Activate.ps1

# Create input file with exit command
@"
0
"@ | Out-File -FilePath test_input.txt -Encoding UTF8

# Run with piped input
python main.py < test_input.txt 2>&1 | Select-Object -First 80

# Cleanup
Remove-Item test_input.txt -Force
