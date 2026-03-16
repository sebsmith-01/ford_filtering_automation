import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from helper_functions import get_monday_str
from dotenv import load_dotenv
import os
from collections import defaultdict
from DataProcessor import DataProcessor as MentionDataProcessor, VEHICLE_MODELS

load_dotenv()

EU7_COUNTRY_NAMES = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'UK': 'the UK',
    'NL': 'the Netherlands',
    'NO': 'Norway'
}

# START_DATE = date.today().replace(day=1)
START_DATE = date(2025, 12, 16)
# END_DATE = date.today()
END_DATE = date(2025, 12, 16)

vehicle_ids = pd.read_csv(PROJECT_ROOT / 'vehicle_ids.csv')
id_to_name = dict(zip(vehicle_ids["desired_vehicle_id"], vehicle_ids["vehicle_name"]))

output_name = "explorer-fix.xlsx"

# Choose a competitor set here. Must be spelt correctly
competitor_set = "Electric Explorer"
include_showing_interest = False
ownerships = "('Owner', 'Pre-Ownership', 'Showing Interest')" if include_showing_interest else "('Owner', 'Pre-Ownership')" 

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")

ENGINE = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

COMPETITOR_SETS = {
    "Mustang Mach-E": [16, 2, 17],
    "Puma Gen-E": [25, 26, 27, 28],
    "Electric Explorer": [1, 2, 3, 4, 5, 6],
    "Ford Capri": [20, 21, 22, 23, 24, 3],
    "Kuga MCA": [7],
    "Puma MCA": [11],
    "Transit Custom": [36, 37, 38, 39, 40, 41], 
    "All": [i for i in range(1, 42)]
}

comp_set_string = (str(COMPETITOR_SETS.get(competitor_set))).strip('[]') 

query = f"""
SELECT 
    dm.*, -- All columns from data_mention
    da.name AS author_name, -- Author's name
    dt.title AS thread_title, -- Thread's title
    dv.*, -- All columns from data_vehiclemention
    df.*, -- All columns from data_feedback
    dm.id as mentionid,
    dv.id as id2,
    df.id as id3
FROM 
    data_mention dm
JOIN 
    data_author da ON dm.author_id = da.id -- data_mention links to data_author
JOIN 
    data_thread dt ON dm.thread_id = dt.id -- data_mention links to data_thread
JOIN 
    data_vehiclemention dv ON dv.mention_id = dm.id -- data_vehiclemention links to data_mention
JOIN 
    data_feedback df ON df.vehicle_mention_id = dv.id -- data_feedback links to data_vehiclemention
WHERE 
    dm.post_date BETWEEN ('{START_DATE}') AND ('{END_DATE}')
    AND df.hit = TRUE
    AND dv.desired_vehicle_id IN ({comp_set_string})
    AND dv.ownership_status IN {ownerships};
    """

df = pd.read_sql_query(query, con=ENGINE)
# Editing 'model' so easier to create pivot tables 
topics = ["Driving Technologies"]
df["model"] = df["desired_vehicle_id"].map(id_to_name)
df = df.loc[df['feedback_subcategory'].isin(topics)]

data_outputs = PROJECT_ROOT / "data_outputs"
if not data_outputs.exists():
    data_outputs.mkdir()
df.to_excel(data_outputs / output_name, index=False)


# Single model code first...
# Example outputs
'''
<div class="highlights-section">
<ul>
	<li>Across EU5, 53 mentions have been posted by 37 Kuga MCA customers.</li>
	<li>In terms of sentiment, 12 posts were positive, 25 neutral, and 16 negative.</li>
	<li>In terms of markets, 34 mentions came from the UK, 5 from Germany, 4 from Spain, and 2 from Italy.</li>
</ul>
</div>

<div class="highlights-section">
<p>Across EU7, 104 mentions were posted by Mustang Mach-E customers, 183 by Polestar 2 customers, 156 by Tesla Model 3 customers, 148 by Volkswagen ID.4 customers, and 138 Tesla Model Y customers.</p>

<p>In terms of sentiment, Model 3 had the highest share of positive sentiment feedback (26% - 41 mentions) whilst Polestar 2 had the highest share of negative sentiment feedback (56% - 102 mentions).</p>

<p>In terms of markets, most mentions came from Germany for Mach-E (78%) and ID.4 (63%), from France for Model Y (69%) and Model 3 (44%), and from the UK for Polestar 2 (69%).</p>
</div>



'''
       
def generate_market_text(market_data: dict):
    
    if not market_data: 
        return ""
    
    grouped_by_count = defaultdict(list)
    for conutry_code, count in market_data.items():
        grouped_by_count[count].append(EU7_COUNTRY_NAMES.get(conutry_code, conutry_code))
    sorted_counts = sorted(grouped_by_count.items(), reverse=True)
    
    parts = []
    counter = 0 # There is probably a better way to do this...
    for count, countries in sorted_counts: 
        counter += 1
        country_list = sorted(countries)

        # Must begin with x MENTIONS (EACH) CAME FROM y... 
        if counter == 1: 
            if len(country_list) == 1:
                parts.append(f"{count} mentions came from {country_list[0]}")
            else: 
                joined = (", ".join(country_list[:-1]) + f", and {country_list[-1]}" if len(country_list) > 2 else
                    f"{country_list[0]} and {country_list[1]}")
                parts.append(f"{count} mentions each came from {joined}")
        else: 
            if len(country_list) == 1:
                parts.append(f"{count} from {country_list[0]}")
            else: 
                joined = (", ".join(country_list[:-1]) + f", and {country_list[-1]}" if len(country_list) > 2 else
                    f"{country_list[0]} and {country_list[1]}")
                parts.append(f"{count} each from {joined}")
            
    # Final group must start with ", and x each for ..."
    if len(parts) == 1:
        final_sentence = parts[0] + "."
    else:
        final_sentence = ", ".join(parts[:-1]) + ", and " + parts[-1] + "."
        
    return final_sentence

def model_intro(competitor_set: str, month_dataframe: pd.DataFrame): 
    df = month_dataframe.drop_duplicates(['mentionid'])
    competitor_ids = COMPETITOR_SETS.get(competitor_set, [])

    def format_list(items: list[str]) -> str:
        if not items:
            return ""
        if len(items) == 1:
            return items[0]
        if len(items) == 2:
            return f"{items[0]} and {items[1]}"
        return ", ".join(items[:-1]) + f", and {items[-1]}"
    
    if len(competitor_ids) == 1: # single model intro
        
        total_mentions = len(df.index)
        total_authors = df['author_name'].nunique()
        
        positive_count = len(df[df["overall_sentiment"] == "Positive"])
        neutral_conunt = len(df[df["overall_sentiment"] == "Neutral"])
        negative_count = len(df[df["overall_sentiment"] == "Negative"])
        
        country_counts = df["country_code"].value_counts().to_dict()
        market_text = generate_market_text(country_counts)
        
        return f"""
        <div class="highlights-section">
        <ul>
	        <li>Across EU5, {total_mentions} mentions have been posted by {total_authors} {competitor_set} customers.</li>
	        <li>In terms of sentiment, {positive_count} posts were positive, {neutral_conunt} neutral, and {negative_count} negative.</li>
	        <li>In terms of markets, {market_text}</li>
        </ul>
        </div>""" 

    else: 
        if not competitor_ids:
            return ""

        processor = MentionDataProcessor(df)
        model_stats = []

        for model_id in competitor_ids:
            long_name, short_name = VEHICLE_MODELS.get(
                model_id,
                (id_to_name.get(model_id, str(model_id)), id_to_name.get(model_id, str(model_id)))
            )
            volume = processor.get_vehicle_volume(model_id)
            positive_volume = processor.get_sentiment_volume(model_id, "Positive")
            negative_volume = processor.get_sentiment_volume(model_id, "Negative")

            positive_percentage = round((positive_volume / volume) * 100) if volume else 0
            negative_percentage = round((negative_volume / volume) * 100) if volume else 0

            top_market_country, top_market_percentage = processor.get_top_market(model_id)

            model_stats.append({
                "id": model_id,
                "long_name": long_name,
                "short_name": short_name,
                "volume": volume,
                "positive_volume": positive_volume,
                "positive_percentage": positive_percentage,
                "negative_volume": negative_volume,
                "negative_percentage": negative_percentage,
                "market_country": top_market_country,
                "market_percentage": top_market_percentage
            })

        if not model_stats:
            return ""

        volume_phrases = []
        for idx, stats in enumerate(model_stats):
            if idx == 0:
                volume_phrases.append(f"{stats['volume']} mentions were posted by {stats['long_name']} customers")
            else:
                volume_phrases.append(f"{stats['volume']} by {stats['long_name']} customers")
        volume_sentence = f"Across EU7, {format_list(volume_phrases)}."

        positive_leader = max(model_stats, key=lambda stat: (stat["positive_percentage"], stat["volume"]))
        negative_leader = max(model_stats, key=lambda stat: (stat["negative_percentage"], stat["volume"]))

        sentiment_sentence = (
            "In terms of sentiment, "
            f"{positive_leader['short_name']} had the highest share of positive sentiment feedback "
            f"({positive_leader['positive_percentage']}% - {positive_leader['positive_volume']} mentions) whilst "
            f"{negative_leader['short_name']} had the highest share of negative sentiment feedback "
            f"({negative_leader['negative_percentage']}% - {negative_leader['negative_volume']} mentions)."
        )

        market_groups = {}
        country_order = []
        for stats in model_stats:
            country = stats["market_country"]
            if country == "N/A":
                continue
            if country not in market_groups:
                market_groups[country] = []
                country_order.append(country)
            market_groups[country].append(f"{stats['short_name']} ({stats['market_percentage']})")

        if market_groups:
            ordered_groups = [(country, market_groups[country]) for country in country_order]
            market_segments = [
                f"from {country} for {format_list(models)}" for country, models in ordered_groups
            ]
            markets_sentence = f"In terms of markets, {format_list(market_segments)}."
        else:
            markets_sentence = "In terms of markets, insufficient data to report top countries."

        paragraphs = "\n\n".join([
            f"<p>{volume_sentence}</p>",
            f"<p>{sentiment_sentence}</p>",
            f"<p>{markets_sentence}</p>"
        ])

        return f"""
<div class="highlights-section">
{paragraphs}
</div>"""
        
                
print(model_intro(competitor_set, df))
