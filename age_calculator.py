from datetime import datetime
import pandas as pd

def calculate_age_in_days(date_of_birth):
    # Parse the date of birth
    dob = datetime.strptime(date_of_birth, "%d %b %Y")
    
    # Get the current date
    current_date = datetime.now()
    
    # Calculate the difference
    age_days = (current_date - dob).days
    
    return age_days

def add_age_column(df, date_column='dateOfBirth', new_column='AgeInDays'):
    # Apply the calculate_age_in_days function to the date column
    df[new_column] = df[date_column].apply(calculate_age_in_days)
    return df
