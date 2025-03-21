# models/database.py
import time
import logging
import psycopg2
import psycopg2.extras
import sshtunnel
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool

from config.settings import get_db_config

# Configure SSH tunnel timeout
sshtunnel.SSH_TIMEOUT = 30.0
sshtunnel.TUNNEL_TIMEOUT = 30.0

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.db_config = get_db_config()
        self.pool = None
        self.tunnel = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Set up SSH tunnel and connection pool"""
        try:
            # Create SSH tunnel
            self.tunnel = sshtunnel.SSHTunnelForwarder(
                (self.db_config['ssh_host']),
                ssh_username=self.db_config['ssh_username'],
                ssh_password=self.db_config['ssh_password'],
                remote_bind_address=(
                    self.db_config['postgres_hostname'],
                    self.db_config['postgres_port']
                )
            )
            self.tunnel.start()
            logger.info("SSH tunnel established")
            
            # Create connection pool
            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                user=self.db_config['db_user'],
                password=self.db_config['db_password'],
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                database=self.db_config['db_name']
            )
            logger.info("Database connection pool created")
            
        except Exception as e:
            logger.error(f"Failed to set up database connection: {str(e)}")
            if self.tunnel and self.tunnel.is_active:
                self.tunnel.close()
            raise
    
    def close(self):
        """Close all connections and tunnel"""
        if self.pool:
            self.pool.closeall()
            logger.info("Closed all database connections")
        
        if self.tunnel and self.tunnel.is_active:
            self.tunnel.close()
            logger.info("Closed SSH tunnel")
    
    @contextmanager
    def get_connection(self, retries=3, retry_delay=2):
        """Get a connection from the pool with retry logic"""
        conn = None
        for attempt in range(retries):
            try:
                conn = self.pool.getconn()
                yield conn
                self.pool.putconn(conn)
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.warning(f"Database connection error (attempt {attempt+1}/{retries}): {str(e)}")
                if conn:
                    try:
                        self.pool.putconn(conn, close=True)
                    except:
                        pass
                
                if attempt == retries - 1:
                    logger.error("Max retries reached. Unable to get database connection.")
                    raise
                
                time.sleep(retry_delay)
    
    @contextmanager
    def get_cursor(self, commit=True):
        """Get a database cursor with automatic commit/rollback"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database operation failed: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a query and optionally fetch results"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
    
    def execute_batch(self, query, params_list):
        """Execute batch operations"""
        with self.get_cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, params_list)
    
    def get_or_create_region(self, region_name):
        """Get region_id or create new region"""
        with self.get_cursor() as cursor:
            # Try to get existing region
            cursor.execute(
                "SELECT region_id FROM regions WHERE name = %s",
                (region_name,)
            )
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Create new region
            cursor.execute(
                "INSERT INTO regions (name) VALUES (%s) RETURNING region_id",
                (region_name,)
            )
            return cursor.fetchone()[0]
    
    def get_or_create_medical_center(self, center_data):
        """Get center_id or create new medical center"""
        with self.get_cursor() as cursor:
            # Try to get existing center
            cursor.execute(
                "SELECT center_id FROM medical_centers WHERE code = %s",
                (center_data['code'],)
            )
            result = cursor.fetchone()
            
            if result:
                # Update existing center
                query = """
                    UPDATE medical_centers SET 
                        name = %(name)s,
                        region_id = %(region_id)s,
                        category = %(category)s,
                        reporter_name = %(reporter_name)s,
                        institution_type = %(institution_type)s,
                        reporter_type = %(reporter_type)s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = %(code)s
                """
                cursor.execute(query, center_data)
                return result[0]
            
            # Create new center
            query = """
                INSERT INTO medical_centers (
                    code, name, region_id, category, reporter_name, 
                    institution_type, reporter_type
                ) VALUES (
                    %(code)s, %(name)s, %(region_id)s, %(category)s, %(reporter_name)s,
                    %(institution_type)s, %(reporter_type)s
                ) RETURNING center_id
            """
            cursor.execute(query, center_data)
            return cursor.fetchone()[0]
    
    def get_or_create_product(self, product_data):
        """Get product_id or create new product"""
        with self.get_cursor() as cursor:
            # Try to get existing product
            cursor.execute(
                "SELECT product_id FROM products WHERE code = %s",
                (product_data['code'],)
            )
            result = cursor.fetchone()
            
            if result:
                # Update existing product
                query = """
                    UPDATE products SET 
                        name = %(name)s,
                        type_id = %(type_id)s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = %(code)s
                """
                cursor.execute(query, product_data)
                return result[0]
            
            # Create new product
            query = """
                INSERT INTO products (code, name, type_id)
                VALUES (%(code)s, %(name)s, %(type_id)s)
                RETURNING product_id
            """
            cursor.execute(query, product_data)
            return cursor.fetchone()[0]
    
    def get_product_type_id(self, type_code):
        """Get product type id by code"""
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT type_id FROM product_types WHERE code = %s",
                (type_code,)
            )
            result = cursor.fetchone()
            
            if not result:
                # Insert default types if needed
                cursor.execute(
                    """
                    INSERT INTO product_types (code, name) VALUES 
                    ('M', 'Medicine'), 
                    ('I', 'Instrument/Supply')
                    ON CONFLICT (code) DO NOTHING
                    """
                )
                cursor.execute(
                    "SELECT type_id FROM product_types WHERE code = %s",
                    (type_code,)
                )
                result = cursor.fetchone()
            
            return result[0] if result else None
    
    def update_inventory(self, inventory_data):
        """Update inventory with upsert logic"""
        with self.get_cursor() as cursor:
            query = """
                INSERT INTO inventory (
                    center_id, product_id, current_stock, avg_monthly_consumption,
                    accumulated_consumption_4m, measurement, last_month_consumption,
                    last_month_stock, status_indicator, cpma_12_months_ago,
                    cpma_24_months_ago, cpma_36_months_ago, accumulated_consumption_12m,
                    report_date, status
                ) VALUES (
                    %(center_id)s, %(product_id)s, %(current_stock)s, %(avg_monthly_consumption)s,
                    %(accumulated_consumption_4m)s, %(measurement)s, %(last_month_consumption)s,
                    %(last_month_stock)s, %(status_indicator)s, %(cpma_12_months_ago)s,
                    %(cpma_24_months_ago)s, %(cpma_36_months_ago)s, %(accumulated_consumption_12m)s,
                    %(report_date)s, %(status)s
                )
                ON CONFLICT (center_id, product_id, report_date) DO UPDATE SET
                    current_stock = EXCLUDED.current_stock,
                    avg_monthly_consumption = EXCLUDED.avg_monthly_consumption,
                    accumulated_consumption_4m = EXCLUDED.accumulated_consumption_4m,
                    measurement = EXCLUDED.measurement,
                    last_month_consumption = EXCLUDED.last_month_consumption,
                    last_month_stock = EXCLUDED.last_month_stock,
                    status_indicator = EXCLUDED.status_indicator,
                    cpma_12_months_ago = EXCLUDED.cpma_12_months_ago,
                    cpma_24_months_ago = EXCLUDED.cpma_24_months_ago,
                    cpma_36_months_ago = EXCLUDED.cpma_36_months_ago,
                    accumulated_consumption_12m = EXCLUDED.accumulated_consumption_12m,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(query, inventory_data)
    
    def mark_missing_inventory_as_zero(self, report_date):
        """Set stock to zero for inventory items not in current import"""
        with self.get_cursor() as cursor:
            query = """
                UPDATE inventory
                SET current_stock = 0, 
                    updated_at = CURRENT_TIMESTAMP,
                    status_indicator = 'Desabastecido'
                WHERE report_date < %s
                AND current_stock > 0
                RETURNING inventory_id
            """
            cursor.execute(query, (report_date,))
            return cursor.rowcount
    
    def record_processed_centers_products(self, report_date):
        """Record all center_id, product_id combinations processed in this run"""
        with self.get_cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMPORARY TABLE IF NOT EXISTS processed_inventory (
                    center_id INTEGER,
                    product_id INTEGER,
                    PRIMARY KEY (center_id, product_id)
                )
                """
            )
            return True