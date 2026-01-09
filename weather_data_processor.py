"""
Weather Data Processor Module

This module provides the WeatherDataProcessor class for processing weather
station data from the Maji Ndogo weather monitoring system. It includes 
functionality for data ingestion, message parsing using regex patterns,
and data cleaning.

Classes:
    WeatherDataProcessor: Main class for processing weather data

Dependencies:
    pandas: For data manipulation and analysis
    re: For regular expression pattern matching
    logging: For logging operations
    data_ingestion: Custom module for web CSV operations

Author: Data Science Team
Date: October 2025
Version: 1.0
"""

import re
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    """
    A class for processing weather station data from the Maji Ndogo monitoring system.
    
    This class handles the complete weather data processing pipeline including:
    - Loading weather data from web CSV
    - Parsing weather messages using regex patterns
    - Extracting structured data from text messages
    - Data cleaning and validation
    
    Attributes:
        weather_csv_path (str): URL to the weather station CSV file
        regex_patterns (dict): Dictionary of regex patterns for data extraction
        weather_df (pd.DataFrame): Weather data DataFrame
        logger: Logging instance
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the WeatherDataProcessor with configuration parameters.
        
        Args:
            config_params (dict): Dictionary containing configuration parameters:
                - weather_csv_path: URL to weather station CSV file
                - regex_patterns: Dictionary of regex patterns for extraction
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        # Initialize configuration from parameters
        self.weather_csv_path = config_params['weather_csv_path']
        self.regex_patterns = config_params['regex_patterns']

        # Initialize logging
        self.initialize_logging(logging_level)
        
        # Initialize empty DataFrame for weather data
        self.weather_df = None

    def initialize_logging(self, logging_level):
        """
        Set up logging for this instance of WeatherDataProcessor.
        
        Args:
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False
        
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO
            
        self.logger.setLevel(log_level)
        
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Load weather station data from web CSV.
        
        Returns:
            pd.DataFrame: Weather station data
            
        Raises:
            Exception: If CSV loading fails
        """
        try:
            self.weather_df = read_from_web_CSV(self.weather_csv_path)
            self.logger.info("Successfully loaded weather station data from the web.")
            return self.weather_df
        except Exception as e:
            self.logger.error(f"Failed to load weather station data: {e}")
            raise e

    def extract_measurement(self, message, measurement_type):
        """
        Extract measurement values from weather station messages using regex.
        
        Args:
            message (str): Weather station message text
            measurement_type (str): Type of measurement to extract
            
        Returns:
            float or None: Extracted measurement value or None if not found
        """
        if measurement_type not in self.regex_patterns:
            return None
            
        pattern = self.regex_patterns[measurement_type]
        match = re.search(pattern, message)
        
        if match:
            # Extract the first captured group that contains a number
            for group in match.groups():
                if group and group.replace('.', '').replace('-', '').isdigit():
                    return float(group)
        return None

    def process_messages(self):
        """
        Process weather station messages to extract structured data.
        
        This method parses the 'Message' column and creates new columns
        for each measurement type defined in regex_patterns.
        """
        if self.weather_df is None:
            self.logger.error("No weather data loaded. Call weather_station_mapping() first.")
            return
        
        # Create new columns for each measurement type
        for measurement_type in self.regex_patterns.keys():
            self.weather_df[measurement_type] = self.weather_df['Message'].apply(
                lambda msg: self.extract_measurement(msg, measurement_type)
            )
        
        self.logger.info("Successfully processed weather station messages.")

    def calculate_means(self):
        """
        Calculate mean values for each measurement type grouped by weather station.
        
        This method creates a summary DataFrame with mean values for each
        measurement type at each weather station.
        """
        if self.weather_df is None:
            self.logger.error("No weather data loaded.")
            return
        
        try:
            # Check available columns
            self.logger.info(f"Available columns: {self.weather_df.columns.tolist()}")
            
            # Find the weather station column (it might have a different name)
            station_column = None
            possible_names = ['Weather_station', 'Station', 'weather_station', 'Weather_Station', 'Station_ID']
            
            for col_name in possible_names:
                if col_name in self.weather_df.columns:
                    station_column = col_name
                    break
            
            if station_column is None:
                self.logger.warning("No weather station column found. Available columns: " + 
                                  str(self.weather_df.columns.tolist()))
                # Use the first column that might be an identifier
                if len(self.weather_df.columns) > 0:
                    station_column = self.weather_df.columns[0]
                    self.logger.info(f"Using '{station_column}' as station identifier")
            
            # Get numeric columns (measurement types) that actually exist
            measurement_columns = []
            for measurement_type in self.regex_patterns.keys():
                if measurement_type in self.weather_df.columns:
                    # Check if the column has any non-null numeric values
                    if self.weather_df[measurement_type].notna().any():
                        measurement_columns.append(measurement_type)
            
            if not measurement_columns:
                self.logger.warning("No measurement columns found with data after processing messages")
                return
            
            # Calculate means grouped by weather station
            if station_column and station_column in self.weather_df.columns:
                self.weather_df_mean = self.weather_df.groupby(station_column)[measurement_columns].mean()
                self.logger.info(f"Successfully calculated mean values for weather measurements grouped by {station_column}")
                self.logger.info(f"Measurement columns used: {measurement_columns}")
            else:
                self.logger.error("Cannot calculate means without a valid station identifier column")
                return
                
        except Exception as e:
            self.logger.error(f"Error in calculate_means: {e}")
            self.logger.info("Skipping mean calculation due to error")
            return

    def process(self):
        """
        Execute the complete weather data processing pipeline.
        
        This method calls all processing methods in the correct order:
        1. Load weather station data
        2. Process messages to extract measurements
        3. Calculate mean values (optional, continues if fails)
        
        Returns:
            pd.DataFrame: The processed weather DataFrame
            
        Raises:
            Exception: If critical steps in the pipeline fail
        """
        try:
            # Execute processing steps in order
            self.weather_station_mapping()
            self.process_messages()
            
            # Try to calculate means, but don't fail if it doesn't work
            try:
                self.calculate_means()
            except Exception as e:
                self.logger.warning(f"Could not calculate means: {e}")
                self.logger.info("Continuing without mean calculations")
            
            self.logger.info("Weather data processing completed successfully.")
            return self.weather_df
            
        except Exception as e:
            self.logger.error(f"Error during weather data processing: {e}")
            raise e


def main():
    """
    Example usage of the WeatherDataProcessor class.
    
    This function demonstrates how to use the WeatherDataProcessor
    with a configuration dictionary.
    """
    # Example configuration parameters
    config_params = {
        "weather_csv_path": (
            "https://raw.githubusercontent.com/Explore-AI/Public-Data/"
            "master/Maji_Ndogo/Weather_station_data.csv"
        ),
        "regex_patterns": {
            'Rainfall': r'(\d+(\.\d+)?)\s?mm',
            'Temperature': r'(\d+(\.\d+)?)\s?C',
            'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
        },
    }
    
    # Create and use the processor
    processor = WeatherDataProcessor(config_params)
    processed_data = processor.process()
    
    print(f"Processed {len(processed_data)} rows of weather data")
    print(f"Columns: {processed_data.columns.tolist()}")
    print(f"Measurement types: {list(config_params['regex_patterns'].keys())}")


if __name__ == "__main__":
    main()