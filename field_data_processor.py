"""
Field Data Processor Module

This module provides the FieldDataProcessor class for processing agricultural
field data from the Maji Ndogo farm survey database. It includes functionality
for data ingestion, column renaming, data corrections, and weather station mapping.

Classes:
    FieldDataProcessor: Main class for processing field data

Dependencies:
    pandas: For data manipulation and analysis
    logging: For logging operations
    data_ingestion: Custom module for database operations

Author: Data Science Team
Date: October 2025
Version: 1.0
"""

import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A class for processing agricultural field data from the Maji Ndogo farm survey.
    
    This class handles the complete data processing pipeline including:
    - Loading data from SQLite database
    - Renaming and swapping columns
    - Applying data corrections
    - Merging weather station mapping data
    
    Attributes:
        db_path (str): Path to the SQLite database
        sql_query (str): SQL query for data extraction
        columns_to_rename (dict): Dictionary mapping columns to rename
        values_to_rename (dict): Dictionary mapping values to correct
        weather_map_data (str): URL to weather mapping CSV file
        df (pd.DataFrame): Main data DataFrame
        engine: Database engine instance
        logger: Logging instance
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor with configuration parameters.
        
        Args:
            config_params (dict): Dictionary containing configuration parameters:
                - db_path: Path to SQLite database
                - sql_query: SQL query for data extraction
                - columns_to_rename: Dictionary for column renaming
                - values_to_rename: Dictionary for value corrections
                - weather_mapping_csv: URL to weather mapping CSV
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        # Initialize configuration from parameters
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        # Initialize logging
        self.initialize_logging(logging_level)
        
        # Initialize empty objects for DataFrame and engine
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Set up logging for this instance of FieldDataProcessor.
        
        Args:
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        logger_name = __name__ + ".FieldDataProcessor"
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

    def ingest_sql_data(self):
        """
        Load data from the SQLite database using the configured query.
        
        Returns:
            pd.DataFrame: The loaded data
            
        Raises:
            Exception: If database connection or query execution fails
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Rename and swap columns according to the configuration.
        
        This method swaps the names of two columns as specified in the
        columns_to_rename dictionary and converts Annual_yield to numeric.
        """
        # Extract the columns to rename from the configuration
        column1 = list(self.columns_to_rename.keys())[0]
        column2 = list(self.columns_to_rename.values())[0]

        # Create a temporary name to avoid naming conflicts
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.logger.info(f"Swapped columns: {column1} with {column2}")

        # Perform the column swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        
        # Convert Annual_yield to numeric
        self.df['Annual_yield'] = pd.to_numeric(
            self.df['Annual_yield'], errors='coerce'
        )

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply data corrections to the DataFrame.
        
        This method:
        1. Fixes negative elevation values by taking absolute values
        2. Cleans and corrects crop type names
        
        Args:
            column_name (str): Name of the crop type column to correct
            abs_column (str): Name of the elevation column to fix
        """
        # Fix negative elevation values by taking absolute values
        self.df[abs_column] = self.df[abs_column].abs()
        
        # Clean up crop type column: strip whitespace and convert to lowercase
        self.df[column_name] = self.df[column_name].str.strip().str.lower()
        
        # Apply corrections to crop names using the values_to_rename mapping
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )
        
        self.logger.info("Data corrections applied successfully.")

    def weather_station_mapping(self):
        """
        Load weather station mapping data from web CSV.
        
        Returns:
            pd.DataFrame: Weather station mapping data
            
        Raises:
            Exception: If CSV loading fails
        """
        return read_from_web_CSV(self.weather_map_data)

    def process(self):
        """
        Execute the complete data processing pipeline.
        
        This method calls all processing methods in the correct order:
        1. Ingest SQL data
        2. Rename columns
        3. Apply corrections
        4. Load and merge weather station mapping
        
        Returns:
            pd.DataFrame: The fully processed DataFrame
            
        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            # Execute processing steps in order
            self.ingest_sql_data()
            self.rename_columns()
            self.apply_corrections()
            weather_df = self.weather_station_mapping()
            
            # Merge the weather station data to the main DataFrame
            self.df = self.df.merge(weather_df, on='Field_ID', how='left')
            
            # Clean up unnecessary columns
            if 'Unnamed: 0' in self.df.columns:
                self.df = self.df.drop(columns="Unnamed: 0")
                
            self.logger.info("Data processing completed successfully.")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error during data processing: {e}")
            raise e


def main():
    """
    Example usage of the FieldDataProcessor class.
    
    This function demonstrates how to use the FieldDataProcessor
    with a configuration dictionary.
    """
    # Example configuration parameters
    config_params = {
        "sql_query": """
            SELECT *
            FROM geographic_features
            LEFT JOIN weather_features USING (Field_ID)
            LEFT JOIN soil_and_crop_features USING (Field_ID)
            LEFT JOIN farm_management_features USING (Field_ID)
        """,
        "db_path": 'sqlite:///Maji_Ndogo_farm_survey_small.db',
        "columns_to_rename": {
            'Annual_yield': 'Crop_type',
            'Crop_type': 'Annual_yield'
        },
        "values_to_rename": {
            'cassaval': 'cassava',
            'wheatn': 'wheat',
            'teaa': 'tea'
        },
        "weather_csv_path": (
            "https://raw.githubusercontent.com/Explore-AI/Public-Data/"
            "master/Maji_Ndogo/Weather_station_data.csv"
        ),
        "weather_mapping_csv": (
            "https://raw.githubusercontent.com/Explore-AI/Public-Data/"
            "master/Maji_Ndogo/Weather_data_field_mapping.csv"
        ),
    }
    
    # Create and use the processor
    processor = FieldDataProcessor(config_params)
    processed_data = processor.process()
    
    print(f"Processed {len(processed_data)} rows of data")
    print(f"Columns: {processed_data.columns.tolist()}")


if __name__ == "__main__":
    main()
