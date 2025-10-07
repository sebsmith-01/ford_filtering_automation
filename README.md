# Ford Filtering Automation 
This series of scripts automates the weekly filtering of posts from social media, Facebook groups, and forums relating to a set of vehicles, and creates a Google Sheet of posts to review for each vehicle, ready for human validation. 
Each script is run in order (using the run_all.py script), starting with...
## move_files.py
Moves the most recently downloaded weekly data files, 'full_data-{date}.xlsx' and 'dataset_facebook-groups-scraper_{datetime}.xlsx' into a working folder 'weekly_data/{monday date of current week}.
## get_ownership_database.py
Iterates through each vehicle_id (stored in vehicle_ids.csv) and fetches all recorded owners, pre-owners, and 'showing interest' authors for the id's corresponding vehicle. 
The returned dataset is stored in ownership_databases/
Credentials for the Ford database are stored in a .env file.
## add_facebook_names.py
In the full_data-{date}.xlsx file, author_names for posts from Facebook groups are missing. This script matches author_name values of posts using the 'url' column and 'url' (or 'facebookUrl') in the 'dataset_facebook-groups-scraper_{datetime}.xlsx' spreadsheet.
For author_names starting with 'anonymous', author_name is set to 'author {mention_id}' to ensure that the Ford Lens platform doesn't recognise posts from two anonymous writers as being from a unique author.
feedback_subcategory values need to be frequently changed. This script also 'corrects' feedback_subcategory names by using a dictionary (e.g. Range -> Range/Consumption).
The cleaned dataset is stored as weekly_data/{date}/added_facebook_names{date}.xlsx
## google_sheet_editing.py
This script performs the following operations: 
1. Fetches filtering instructions for each vehicle from this Google Sheet - https://docs.google.com/spreadsheets/d/17kK-tOIpwBsYT_I98Me8SwGqq-5MBJZAXWxB7AlVsvQ/edit?usp=sharing
2. Creates or reuses a credentials.json file which allows the script to edit Google Sheets
3. Creates a new Google Sheet 'For_report_{monday date}', copying the structure from this template file - https://docs.google.com/spreadsheets/d/1WridMYMZ4uuJWrNLmkG6JSTjLk7oEUi_ZSaDKvA2AfQ/edit?usp=sharing
4. For each vehicle:
    - Creates a set of all owners, pre-owners, and 'showing interest' authors, and a dictionary linking them to their ownership status (owner takes precedence over pre-ownership, which takes precedence over showing interest)
    - Using the filtering instructions, sets country_codes to either EU5 or EU7
    - Creates filtering masks for posts from owners, posts from domains listed in all_domain_posts, posts from domains in location_domain_posts AND with valid country_codes, posts from threads in thread_titles AND with valid country codes, and posts with 'brand' in brand column, 'model' in model_searches column, AND with valid country_codes.
    - Combines the masks with a logical OR, filters weekly_data with this mask, and adds 'vehicle_ownership_status' and 'ownership_count' columns (ownership_count declares how many vehicles in our set the author owns)
    - Pastes this data into the vehicle's corresponding Google Sheets tab, and hides the columns not included in VISIBLE_COLS

The following scripts are also included: 
## excel_upload.py
Contructs a properly formatted excel spreadsheet of all 'hits' for a set of vehicles in vehicles_to_upload, pulling from the week's tagging file, ready to upload to our database.
Value checking is also performed on the sentiment, ownership, country_code, id+id_2 and feedback_subcategory columns.
## slack_listener.py (WIP)
Will automatically download the weekly data files (sent by Danny Quinn on the #ford slack channel) using a Slack bot, and run the run_all.py script


