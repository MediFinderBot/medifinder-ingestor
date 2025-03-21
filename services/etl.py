# services/etl.py
import logging
from datetime import datetime
from config.settings import BATCH_SIZE

logger = logging.getLogger(__name__)

class ETLProcessor:
    """Processes data records and loads them into the database"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.stats = {
            'regions_created': 0,
            'centers_created': 0,
            'centers_updated': 0,
            'products_created': 0,
            'products_updated': 0,
            'inventory_created': 0,
            'inventory_updated': 0,
            'errors': 0,
            'processed': 0,
            'skipped': 0
        }
        # Track processed items to detect which records need to be marked as zero stock
        self.processed_items = set()
        self.latest_report_date = None
    
    def process_records(self, records):
        """
        Process all records
        
        Args:
            records (list): List of record dictionaries
            
        Returns:
            dict: Processing statistics
        """
        total = len(records)
        logger.info(f"Starting ETL process for {total} records")
        
        # Process in batches for better performance
        for i in range(0, total, BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            self._process_batch(batch)
            logger.info(f"Processed {min(i+BATCH_SIZE, total)}/{total} records")
        
        logger.info("ETL process completed")
        return self.stats
    
    def _process_batch(self, batch):
        """Process a batch of records"""
        for record in batch:
            try:
                self._process_record(record)
                self.stats['processed'] += 1
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                logger.debug(f"Record data: {record}")
                self.stats['errors'] += 1
    
    def _process_record(self, record):
        """
        Process a single record
        
        Args:
            record (dict): Record dictionary
        """
        # Skip invalid records
        if not self._validate_record(record):
            self.stats['skipped'] += 1
            return
        
        try:
            # Track latest report date for cleanup
            if record['fechareporte'] and (not self.latest_report_date or 
                                          record['fechareporte'] > self.latest_report_date):
                self.latest_report_date = record['fechareporte']
            
            # Process region
            region_id = self._process_region(record)
            
            # Process medical center
            center_id = self._process_medical_center(record, region_id)
            
            # Process product
            product_id = self._process_product(record)
            
            # Process inventory
            self._process_inventory(record, center_id, product_id)
            
            # Track processed item
            self.processed_items.add((center_id, product_id))
            
        except Exception as e:
            logger.error(f"Error in record processing pipeline: {str(e)}")
            raise
    
    def _validate_record(self, record):
        """
        Validate if a record has all required data
        
        Args:
            record (dict): Record dictionary
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Check required fields
        required_fields = [
            'diresa', 'codpre', 'nombre_ejecutora', 'categoria',
            'codmed', 'nombre_prod', 'tipo_prod', 'fechareporte'
        ]
        
        for field in required_fields:
            if not record.get(field):
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate numeric fields
        if record.get('stk') is None:
            logger.warning("Missing stock quantity")
            return False
        
        return True
    
    def _process_region(self, record):
        """
        Process region data
        
        Args:
            record (dict): Record dictionary
            
        Returns:
            int: region_id
        """
        region_name = record['diresa']
        
        # Skip invalid regions
        if not region_name or region_name.upper() in ['NULL', 'NONE', '']:
            logger.warning(f"Invalid region name: {region_name}")
            raise ValueError(f"Invalid region name: {region_name}")
        
        try:
            region_id = self.db.get_or_create_region(region_name)
            self.stats['regions_created'] += 1
            return region_id
        except Exception as e:
            logger.error(f"Error processing region '{region_name}': {str(e)}")
            raise
    
    def _process_medical_center(self, record, region_id):
        """
        Process medical center data
        
        Args:
            record (dict): Record dictionary
            region_id (int): ID of the region
            
        Returns:
            int: center_id
        """
        # Extract medical center data
        center_data = {
            'code': record['codpre'],
            'name': record['nombre_ejecutora'],
            'region_id': region_id,
            'category': record['categoria'],
            'reporter_name': record['reportante'],
            'institution_type': record['institucion'],
            'reporter_type': record['tipo_reportante']
        }
        
        try:
            center_id = self.db.get_or_create_medical_center(center_data)
            return center_id
        except Exception as e:
            logger.error(f"Error processing medical center: {str(e)}")
            raise
    
    def _process_product(self, record):
        """
        Process product data
        
        Args:
            record (dict): Record dictionary
            
        Returns:
            int: product_id
        """
        # Get product type ID
        type_code = record['tipo_prod']
        type_id = self.db.get_product_type_id(type_code)
        
        if not type_id:
            logger.error(f"Invalid product type code: {type_code}")
            raise ValueError(f"Invalid product type code: {type_code}")
        
        # Extract product data
        product_data = {
            'code': record['codmed'],
            'name': record['nombre_prod'],
            'type_id': type_id
        }
        
        try:
            product_id = self.db.get_or_create_product(product_data)
            return product_id
        except Exception as e:
            logger.error(f"Error processing product: {str(e)}")
            raise
    
    def _process_inventory(self, record, center_id, product_id):
        """
        Process inventory data
        
        Args:
            record (dict): Record dictionary
            center_id (int): ID of the medical center
            product_id (int): ID of the product
        """
        # Extract inventory data
        inventory_data = {
            'center_id': center_id,
            'product_id': product_id,
            'current_stock': record['stk'] or 0,
            'avg_monthly_consumption': record['cpma'],
            'accumulated_consumption_4m': record['consumo_acum_4m'],
            'measurement': record['med'],
            'last_month_consumption': record['consumo_ult_mes'],
            'last_month_stock': record['stk_ult_mes'],
            'status_indicator': record['indicador'],
            'cpma_12_months_ago': record['cpma_hace_12_meses_a'],
            'cpma_24_months_ago': record['cpma_hace_24_meses_a'],
            'cpma_36_months_ago': record['cpma_hace_36_meses_a'],
            'accumulated_consumption_12m': record['consumo_acum_12m'],
            'report_date': record['fechareporte'] or datetime.now().date(),
            'status': record['estado'] or 'ACTIVO'
        }
        
        try:
            self.db.update_inventory(inventory_data)
        except Exception as e:
            logger.error(f"Error updating inventory: {str(e)}")
            raise
    
    def cleanup_missing_inventory(self):
        """
        Mark inventory items not in the current import as zero stock
        
        Returns:
            int: Number of records updated
        """
        if not self.latest_report_date:
            logger.warning("No report date found, skipping cleanup")
            return 0
        
        try:
            count = self.db.mark_missing_inventory_as_zero(self.latest_report_date)
            logger.info(f"Marked {count} inventory items as zero stock")
            return count
        except Exception as e:
            logger.error(f"Error during inventory cleanup: {str(e)}")
            return 0