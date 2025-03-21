# services/parser.py
import io
import csv
import logging
import codecs
from datetime import datetime
from config.settings import COLUMNS, FILE_DELIMITER, ENCODING

logger = logging.getLogger(__name__)

class FileParser:
    """Parses the source data file"""
    
    def __init__(self, file_path):
        self.file_path = file_path
    
    def parse(self):
        """
        Parse the source file and return records
        
        Returns:
            list: List of dictionaries representing each record
        """
        records = []
        line_count = 0
        error_count = 0
        
        try:
            # Detect BOM if present
            with open(self.file_path, 'rb') as f:
                first_bytes = f.read(4)
            
            # Handle BOM if present (UTF-8, UTF-16, etc.)
            if first_bytes.startswith(codecs.BOM_UTF8):
                encoding = 'utf-8-sig'
            elif first_bytes.startswith(codecs.BOM_UTF16_LE) or first_bytes.startswith(codecs.BOM_UTF16_BE):
                encoding = 'utf-16'
            else:
                encoding = ENCODING
            
            with open(self.file_path, 'r', encoding=encoding) as file:
                # Skip header if present
                first_line = file.readline().strip()
                if first_line.startswith('nombre_ejecutora'):
                    logger.info("Skipping header line")
                else:
                    # Go back to start if there's no header
                    file.seek(0)
                
                # Process each line
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line or line.startswith('ÿþ'):  # Skip empty lines and BOM markers
                        continue
                    
                    try:
                        # Parse line using csv module to handle complex fields
                        csv_reader = csv.reader(io.StringIO(line), delimiter=FILE_DELIMITER)
                        values = next(csv_reader)
                        
                        # Validate record
                        if not self._validate_record(values, line_num):
                            error_count += 1
                            continue
                        
                        # Create record dictionary
                        record = self._create_record(values)
                        records.append(record)
                        line_count += 1
                        
                        # Log progress
                        if line_count % 1000 == 0:
                            logger.info(f"Processed {line_count} records...")
                    
                    except Exception as e:
                        logger.error(f"Error parsing line {line_num}: {str(e)}")
                        logger.debug(f"Line content: {line}")
                        error_count += 1
        
        except Exception as e:
            logger.error(f"Error reading file {self.file_path}: {str(e)}")
            raise
        
        logger.info(f"File parsing complete. Processed {line_count} records with {error_count} errors.")
        return records
    
    def _validate_record(self, values, line_num):
        """
        Validate record data
        
        Args:
            values (list): List of values from a line
            line_num (int): Line number for error reporting
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Check if we have enough fields
        if len(values) < len(COLUMNS):
            logger.warning(f"Line {line_num}: Not enough fields ({len(values)}/{len(COLUMNS)})")
            return False
        
        # Check required fields
        required_indices = [0, 1, 2, 3, 6, 7, 8]  # Indices of required fields
        for idx in required_indices:
            if idx >= len(values) or not values[idx].strip():
                field_name = COLUMNS[idx] if idx < len(COLUMNS) else f"Field {idx}"
                logger.warning(f"Line {line_num}: Missing required field '{field_name}'")
                return False
        
        return True
    
    def _create_record(self, values):
        """
        Create a record dictionary from values
        
        Args:
            values (list): List of values from a line
            
        Returns:
            dict: Record dictionary
        """
        record = {}
        
        # Pad or truncate values list to match expected columns
        padded_values = values + [''] * (len(COLUMNS) - len(values))
        padded_values = padded_values[:len(COLUMNS)]
        
        # Create dictionary with field names
        for i, field_name in enumerate(COLUMNS):
            record[field_name] = padded_values[i].strip() if i < len(padded_values) else ''
        
        # Convert numeric fields
        numeric_fields = ['stk', 'cpma', 'consumo_acum_4m', 'med', 'consumo_ult_mes', 
                         'stk_ult_mes', 'cpma_hace_12_meses_a', 'cpma_hace_24_meses_a', 
                         'cpma_hace_36_meses_a', 'consumo_acum_12m']
        
        for field in numeric_fields:
            try:
                if record[field] and record[field].lower() != 'null':
                    record[field] = float(record[field].replace(',', '.'))
                else:
                    record[field] = None
            except ValueError:
                logger.warning(f"Invalid numeric value for {field}: '{record[field]}', setting to None")
                record[field] = None
        
        # Convert date field
        try:
            if record['fechareporte']:
                record['fechareporte'] = datetime.strptime(record['fechareporte'], '%Y-%m-%d').date()
            else:
                record['fechareporte'] = None
        except ValueError:
            logger.warning(f"Invalid date format: {record['fechareporte']}, setting to None")
            record['fechareporte'] = None
        
        return record