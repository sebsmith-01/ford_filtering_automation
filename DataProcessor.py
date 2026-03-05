import pandas as pd

EU7_COUNTRY_NAMES = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'UK': 'the UK',
    'NL': 'the Netherlands',
    'NO': 'Norway'
}

VEHICLE_MODELS = {1: ("Ford Electric Explorer", "Explorer"), 
                 2: ("Volkswagen ID.4", "ID.4"), 
                 3: ("Peugeot E-3008", "E-3008"),
                 4: ("Renault Megane E-Tech", "Megane E-Tech"), 
                 5: ("Skoda Enyaq", "Enyaq"), 
                 6: ("Volvo XC40 Recharge/EX40", "XC40 Recharge"), 
                 7: ("Ford Kuga MCA", "Kuga MCA"), 
                 11: ("Ford Puma MCA", "Puma MCA"),
                 16: ("Mustang Mach-E", "Mach-E"), 
                 17: ("Tesla Model Y", "Model Y"), 
                 18: ("Tesla Model 3", "Model 3"),
                 19: ("Polestar 2", "Polestar 2"), 
                 20: ("Ford Capri", "Capri"), 
                 21: ("Volkswagen ID.5", "ID.5"), 
                 22: ("Skoda Enyaq Coupe", "Enyaq Coupe"), 
                 23: ("Audi Q4 E-Tron Sportback", "Q4 E-Tron Sportback"), 
                 24: ("Kia EV6", "EV6"), 
                 25: ("Puma Gen-E", "Puma Gen-E"), 
                 26: ("Volvo EX30", "EX30"), 
                 27: ("Volkswagen ID.3", "ID.3"), 
                 28: ("Hyundai Kona Electric", "Kona Electric"), 
                 29: ("Peugeot E-2008", "E-2008")}

class DataProcessor: 
    def __init__(self, df: pd.DataFrame):
        self.df = df.drop_duplicates(["mentionid"])
        
    def get_vehicle_volume(self, model_id: int) -> int: 
        return len(self.df[self.df["desired_vehicle_id"] == model_id])
        
    def get_sentiment_volume(self, model_id: int, sentiment: str) -> int:
        return len(self.df[
            (self.df["desired_vehicle_id"] == model_id) &
            (self.df["overall_sentiment"] == sentiment)
        ]) 
        
    def get_sentiment_percentage(self, model_id: int, sentiment: str) -> str: # Returns percengage as a string
        volume = self.get_vehicle_volume(model_id)
        sentiment_volume = self.get_sentiment_volume(model_id, sentiment)
        percentage = round((sentiment_volume / volume) * 100)
        return f"{str(percentage)}%"
    
    def get_top_market(self, model_id: int) -> tuple[str, str]: # (Country, percentage)
        volume = self.get_vehicle_volume(model_id)
        
        if volume == 0:
            return ("N/A", "0%")

        model_mentions = self.df[self.df["desired_vehicle_id"] == model_id]
        country_counts = model_mentions["country_code"].value_counts()

        top_country_code = country_counts.idxmax()
        top_country_name = EU7_COUNTRY_NAMES.get(top_country_code, top_country_code)
        top_country_percentage = round((country_counts[top_country_code] / volume) * 100)

        return (top_country_name, f"{top_country_percentage}%")
    
    
