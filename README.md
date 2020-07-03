# Data-Cleaning-Tools
To fix reason for starting do the following:
1. Open the MOH cohort report and drill down to the number for unknown reasons
2. Copy the ARV numbers
3. Open the file 'site_unknown_reason.xls' located in this folder/path and save a copy of it with exact site name on site in the file name
4. paste the ARV numbers and under ARV number header. Make sure you paste only numbers and no headers.
5. Go through the manual registers and indicate the reason for every number as indicated in the register by
   only selecting the reasons that pop up on the field for reason
6: Save the file the file as CSV
7. Open the reason_for_starting_fix.rb file in this folder and edit on path just to point to your CSV file
8. Go to the root of BHT-EMR_API and run: rails r bin/Data-Cleaning-Tools/reason_for_starting_fix.rb


