"""
Excel data transformation module for cleaning and reshaping price data.
"""
import logging
import pandas as pd
from datetime import datetime

from agroamigo_pipeline.utils.utils import clean_price_value

class ExcelDataTransformer:
    """Transforms Excel data into the required format."""
    
    def __init__(self, logger=None):
        """
        Initialize the data transformer.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def transform(self, df, metadata_dict):
        """
        Transform the DataFrame according to the required schema.
        
        Args:
            df (DataFrame): DataFrame to transform
            metadata_dict (dict): Dictionary with extracted metadata
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        # Extract metadata
        date_str = metadata_dict['date']
        geography_info = metadata_dict['geography_info']
        product_category_map = metadata_dict['product_category_map']
        
        # Remove Var % columns
        df = self.remove_var_percent_columns(df)
        
        # Prepare price data
        price_df = self.prepare_price_data(df, product_category_map, geography_info)
        
        # Add date and source information
        price_df['date'] = date_str
        price_df['source'] = 'anexo'  # Source for Excel files
        
        # Ensure all required columns exist
        required_columns = [
            'date', 'city', 'market', 'major_category', 'sub_category', 
            'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
        ]
        
        for col in required_columns:
            if col not in price_df.columns:
                price_df[col] = None if col in ['daily_min', 'daily_max'] else ""
        
        return price_df
    
    def remove_var_percent_columns(self, df):
        """
        Remove 'Var %' columns that are not needed for price data.
        
        Args:
            df (DataFrame): DataFrame to clean
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        try:
            # Identify columns with Var % in the name
            var_cols = [col for col in df.columns if isinstance(col, str) and 'var %' in col.lower()]
            
            # Remove these columns
            if var_cols:
                df = df.drop(columns=var_cols)
                self.logger.debug(f"Removed {len(var_cols)} Var % columns")
            
            return df
        except Exception as e:
            self.logger.warning(f"Error removing Var % columns: {str(e)}")
            return df
    
    def prepare_price_data(self, df, product_category_map, geography_info):
        """
        Prepare price data by transforming the Excel structure to match the target schema.
        
        Args:
            df (DataFrame): DataFrame with product data
            product_category_map (dict): Mapping of products to categories
            geography_info (list): Geography information for each column
            
        Returns:
            DataFrame: Transformed price data
        """
        output_data = []
        
        try:
            # Get first column, usually contains product names
            if len(df.columns) == 0:
                return pd.DataFrame()
                
            # Process each row
            for i, row in df.iterrows():
                product = row.iloc[0]
                
                # Skip non-product rows
                if not isinstance(product, str) or not product.strip():
                    continue
                
                product_clean = product.strip()
                
                # Skip category rows
                if product_clean in product_category_map.values():
                    continue
                
                # Skip rows with product headers
                if product_clean.lower() in ['producto', 'product', 'item', 'descripcion', 'description']:
                    continue
                
                # Get the category for this product
                category = product_category_map.get(product_clean, "Uncategorized")
                
                # Process each geography/market column
                for geo in geography_info:
                    column = geo['column']
                    
                    # Skip if column is not in the DataFrame
                    if column not in df.columns:
                        continue
                    
                    # Get the price value
                    price_value = row[column]
                    
                    # Skip non-numeric and "n.d." values
                    if pd.isna(price_value) or price_value == "n.d." or not isinstance(price_value, (int, float, str)):
                        continue
                    
                    # Clean and convert price
                    price_numeric = clean_price_value(price_value)
                    
                    if price_numeric is not None:
                        # Create output row
                        output_row = {
                            'product': product_clean,
                            'major_category': category,
                            'sub_category': '',  # No subcategory in anexo format
                            'city': geo['city'],
                            'market': geo['market'],
                            'daily_min': price_numeric,
                            'daily_max': price_numeric,  # Same value for min and max
                            'presentation': '',  # No presentation in anexo format
                            'units': '1 kilogramo'  # Default unit
                        }
                        
                        output_data.append(output_row)
            
            return pd.DataFrame(output_data)
            
        except Exception as e:
            self.logger.warning(f"Error preparing price data: {str(e)}")
            return pd.DataFrame() 