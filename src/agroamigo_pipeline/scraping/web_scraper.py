"""
Updated web scraper for Colombian agricultural price data.
Uses the new registry system for tracking downloads.
"""

import os
import queue
import threading
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
from lxml import etree
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import uuid

from agroamigo_pipeline.config.settings import SOURCE_DIR
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call
from agroamigo_pipeline.utils.date_utils import parse_spanish_date_scraper, parse_spanish_month_year
from agroamigo_pipeline.registry.webscraper_registry import WebScraperRegistry

class ThreadLogger:
    """Logger wrapper for thread-safe logging to a queue"""
    
    def __init__(self, log_queue):
        self.log_queue = log_queue
        self.thread_id = threading.get_ident()
    
    def _log(self, level, message):
        self.log_queue.put({
            'level': level,
            'message': message,
            'thread_id': self.thread_id,
            'timestamp': datetime.now()
        })
    
    def debug(self, message):
        self._log('DEBUG', message)
    
    def info(self, message):
        self._log('INFO', message)
    
    def warning(self, message):
        self._log('WARNING', message)
    
    def error(self, message):
        self._log('ERROR', message)

def log_consumer(log_queue, logger):
    """Consumes log messages from the queue and forwards them to the actual logger"""
    while True:
        try:
            log_entry = log_queue.get()
            if log_entry is None:  # Signal to stop
                break
            
            level = log_entry['level']
            message = f"[Thread-{log_entry['thread_id']}] {log_entry['message']}"
            
            if level == 'DEBUG':
                logger.debug(message)
            elif level == 'INFO':
                logger.info(message)
            elif level == 'WARNING':
                logger.warning(message)
            elif level == 'ERROR':
                logger.error(message)
            
            log_queue.task_done()
        except Exception as e:
            logger.error(f"Error in log consumer: {str(e)}")

class WebScraper:
    """Web scraper for Colombian agricultural price data."""
    
    def __init__(self, source_dir=SOURCE_DIR, max_workers=4, logger=None):
        """
        Initialize the web scraper.
        
        Args:
            source_dir (str): Directory to save downloaded files
            max_workers (int): Number of worker threads for parallel downloads
            logger (logging.Logger): Logger instance
        """
        self.source_dir = source_dir
        self.max_workers = max_workers
        
        # Initialize logging
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['scraper']
        else:
            self.logger = logger
        
        # Ensure source directory exists
        os.makedirs(source_dir, exist_ok=True)
        
        # Initialize registry
        self.registry = WebScraperRegistry(logger=self.logger)
        
        # Create log queue for thread-safe logging
        self.log_queue = queue.Queue()
        
        # Start log consumer thread
        self.log_thread = None
        
        self.logger.info(f"Web scraper initialized with source directory: {source_dir}")
    
    def process_daily_price_page(self, url, start_date, end_date, force_reprocess, log_queue):
        """
        Process a single page of daily price data.
        
        Args:
            url (str): The URL to process
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            force_reprocess (bool): Whether to force reprocessing of previously downloaded files
            log_queue (Queue): Queue for logging messages
            
        Returns:
            tuple: (failed_downloads, stats_dict)
        """
        # Initialize thread-local state
        thread_logger = ThreadLogger(log_queue)
        thread_logger.info(f"Processing page: {url}")
        
        # Initialize statistics tracking for this thread
        failed_downloads = []
        stats = {
            'month_key': None,
            'links_explored': 0,
            'already_downloaded': 0,
            'new_downloads': 0,
            'failed_downloads': 0
        }
        
        # Thread-local variables to track state
        page_year = None
        page_month = None
        current_month_key = None
        current_row_date = None
        
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Get the page year and month
            page_year, page_month = self._get_page_year(soup, thread_logger)
            
            if not page_year or not page_month:
                thread_logger.error("Could not determine page year/month, skipping page")
                return failed_downloads, {current_month_key: stats} if current_month_key else {}
            
            # Store the month key for statistics
            current_month_key = f"{page_year}-{page_month:02d}"
            stats['month_key'] = current_month_key
            thread_logger.debug(f"Processing data for month: {current_month_key}")
            
            # Find the data table
            found_table = self._find_data_table(soup, thread_logger)
            if not found_table:
                thread_logger.error("Could not find data table, skipping page")
                return failed_downloads, {current_month_key: stats} if current_month_key else {}
            
            # Process the found table
            thread_logger.info("Beginning to process table rows")
            
            for row in found_table.find_all('tr'):
                # Get date from first column
                date_cell = row.find('td')
                if not date_cell:
                    thread_logger.debug("Skipping row: No date cell found")
                    continue
                
                date_text = date_cell.text.strip()
                # Append year to date text
                date_text = f"{date_text} - {page_year}"
                
                try:
                    row_date = parse_spanish_date_scraper(date_text)
                    current_row_date = row_date  # Store the current row date for download_file
                    thread_logger.info(f"Processing row for date: {row_date}")
                    
                    if not (start_date <= row_date <= end_date):
                        thread_logger.debug(f"Skipping row: Date {row_date} outside requested range")
                        continue
                except ValueError:
                    thread_logger.warning(f"Skipping row: Invalid date format '{date_text}'")
                    continue
                
                # Process download links
                links_found = row.find_all('a')
                thread_logger.info(f"Found {len(links_found)} links in row for {row_date}")
                
                for link in links_found:
                    link_text = link.text.lower()
                    thread_logger.debug(f"Processing link: {link_text}")
                    
                    if 'boletín' in link_text or 'boletin' in link_text:
                        thread_logger.debug(f"Skipping bulletin link: {link_text}")
                        continue
                    
                    # Update link exploration count only for non-bulletin links
                    stats['links_explored'] += 1
                    
                    href = link.get('href')
                    if href:
                        full_url = urljoin("https://www.dane.gov.co", href)
                        filename = os.path.basename(href)
                        
                        # Format the date prefix
                        date_prefix = current_row_date.strftime('%Y%m%d')
                        full_filename = f"{date_prefix}-{filename}"
                        download_path = os.path.join(self.source_dir, full_filename)
                        
                        # Check registry instead of checking file system directly
                        if self.registry.file_exists(full_filename) and not force_reprocess:
                            thread_logger.debug(f"File exists in registry: {full_filename}")
                            stats['already_downloaded'] += 1
                            continue
                        
                        try:
                            thread_logger.info(f"Downloading file: {full_filename}")
                            download_start = time.time()
                            file_response = requests.get(full_url, stream=True)
                            file_response.raise_for_status()
                            
                            filepath = os.path.join(self.source_dir, full_filename)
                            with open(filepath, 'wb') as f:
                                for chunk in file_response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            download_duration = time.time() - download_start
                            thread_logger.info(f"Successfully downloaded: {full_filename}")
                            
                            # Determine file type based on extension
                            # In web_scraper.py, around line ~596
                            # Determine file type based on extension
                            file_extension = os.path.splitext(filename.lower())[1]
                            if file_extension == '.zip':
                                file_type = "zip"
                            elif file_extension in ['.xlsx', '.xls']:
                                file_type = "excel"
                            elif file_extension == '.pdf':
                                file_type = "pdf"
                            elif file_extension == '.rar':  # Add RAR support
                                file_type = "rar"
                            else:
                                file_type = "other"
                                thread_logger.warning(f"Unsupported file type: {file_extension} for {full_filename}")
                            
                            # Register successful download in registry
                            self.registry.register_download_attempt(
                                filename=full_filename,
                                file_date=current_row_date,
                                url=full_url,
                                file_type=file_type,
                                download_path=filepath,
                                month_key=current_month_key,
                                duration_seconds=download_duration
                            )
                            
                            stats['new_downloads'] += 1
                        except Exception as e:
                            error_message = str(e)
                            thread_logger.error(f"Error downloading {full_url}: {error_message}")
                            
                            # Determine file type based on extension
                            file_extension = os.path.splitext(filename.lower())[1]
                            if file_extension == '.zip':
                                file_type = "zip"
                            elif file_extension in ['.xlsx', '.xls']:
                                file_type = "excel"
                            elif file_extension == '.pdf':
                                file_type = "pdf"
                            else:
                                file_type = "other"
                                error_message += f" (Unsupported file type: {file_extension})"
                            
                            # Register failed download in registry
                            self.registry.register_download_attempt(
                                filename=full_filename,
                                file_date=current_row_date,
                                url=full_url,
                                file_type=file_type,
                                error=error_message,
                                month_key=current_month_key
                            )
                            
                            # Track the failed download
                            failed_downloads.append({
                                'url': full_url,
                                'filename': full_filename,
                                'month_key': current_month_key,
                                'error': error_message
                            })
                            stats['failed_downloads'] += 1
                    else:
                        thread_logger.warning(f"Link found without href attribute: {link_text}")
        except Exception as e:
            error_message = str(e)
            thread_logger.error(f"Error processing {url}: {error_message}")
            return failed_downloads, {current_month_key: stats} if current_month_key else {}
        
        thread_logger.info(f"Completed processing for {url}")
        return failed_downloads, {current_month_key: stats} if current_month_key else {}
    
    def get_navigation_links(self, start_date, end_date):
        """
        Extract and filter navigation links based on date range.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            
        Returns:
            list: List of URLs to process
        """
        self.logger.info("Fetching navigation links from main page")
        
        base_url = "https://www.dane.gov.co"
        main_url = f"{base_url}/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/componente-precios-mayoristas"
        
        response = requests.get(main_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        links = []
        total_links_found = 0
        links_in_date_range = 0
        
        # Create thread logger for consistent logging
        thread_logger = ThreadLogger(self.log_queue)
        
        # Get all pre containers using direct selector
        pre_containers = soup.select('div#cnpv > pre')
        
        if not pre_containers:
            pre_containers = soup.select('div[itemprop="articleBody"] div#cnpv > pre')
        
        if not pre_containers:
            # Try more flexible selectors
            pre_containers = soup.select('div#cnpv')
            if pre_containers:
                pre_in_cnpv = pre_containers[0].find_all('pre')
                if pre_in_cnpv:
                    pre_containers = pre_in_cnpv
        
        thread_logger.info(f"Found {len(pre_containers)} pre containers")
        
        for container in pre_containers:
            if not container:
                continue
                
            thread_logger.info("Processing pre container")

            # Process links directly from the pre container
            links_in_container = container.find_all('a')
            thread_logger.info(f"Found {len(links_in_container)} links to process in the pre container")
            
            for link in links_in_container:
                href = link.get('href')
                if href:
                    total_links_found += 1
                    try:
                        # Try to parse the date directly from the link text or href
                        link_text = link.text.strip()
                        page_date = None
                        
                        # Try various date parsing approaches
                        try:
                            # First try direct parsing from link text
                            page_date = parse_spanish_date_scraper(link_text)
                        except ValueError:
                            try:
                                # Try to parse from href
                                page_date = parse_spanish_date_scraper(href)
                            except ValueError:
                                # Try to extract year/month from href and use first day of month
                                year_month_match = re.search(r'(\d{4})[-_]?(\d{2})', href)
                                if year_month_match:
                                    year = int(year_month_match.group(1))
                                    month = int(year_month_match.group(2))
                                    # Use first day of month
                                    page_date = datetime(year, month, 1)
                                else:
                                    # If all else fails, try to extract year from link text or href
                                    year_match = re.search(r'(\d{4})', link_text)
                                    if not year_match:
                                        year_match = re.search(r'(\d{4})', href)
                                    
                                    if year_match:
                                        year = int(year_match.group(1))
                                        # If we can find a month name in the text
                                        month_names = [
                                            "enero", "febrero", "marzo", "abril", "mayo", "junio",
                                            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
                                        ]
                                        for i, month_name in enumerate(month_names, 1):
                                            if month_name in link_text.lower():
                                                page_date = datetime(year, i, 1)
                                                break
                        
                        # If we found a date, check if it's in range
                        if page_date:
                            thread_logger.debug(f"Found link for date {page_date}: {href}")
                            
                            # More lenient date range check - any overlap with the month
                            # This is a critical fix to ensure we capture pages that might have data within our range
                            page_month_start = datetime(page_date.year, page_date.month, 1)
                            if page_date.month == 12:
                                page_month_end = datetime(page_date.year + 1, 1, 1)
                            else:
                                page_month_end = datetime(page_date.year, page_date.month + 1, 1)
                            
                            # Check if there's any overlap between the page's month and our date range
                            date_overlap = (
                                (start_date <= page_month_end) and (end_date >= page_month_start)
                            )
                            
                            if date_overlap:
                                full_url = urljoin(base_url, href)
                                links.append(full_url)
                                links_in_date_range += 1
                                thread_logger.info(f"Added link for date {page_date} (within range)")
                            else:
                                thread_logger.debug(f"Skipping link for date {page_date} (outside range)")
                        else:
                            # If we can't determine the date, include it by default
                            # This ensures we don't miss any potentially relevant pages
                            thread_logger.warning(f"Could not determine date for link {href} - including by default")
                            full_url = urljoin(base_url, href)
                            links.append(full_url)
                            links_in_date_range += 1
                            
                    except Exception as e:
                        thread_logger.warning(f"Error processing link {href}: {str(e)}")
                        # Include links we can't process by default
                        full_url = urljoin(base_url, href)
                        links.append(full_url)
                        links_in_date_range += 1

        # Always add current month's page as it might contain relevant data
        if main_url not in links:
            links.append(main_url)
            links_in_date_range += 1
            thread_logger.info("Added main URL to links (current month)")

        thread_logger.info(
            f"Link gathering summary:\n"
            f"- Total links found: {total_links_found}\n"
            f"- Links within date range: {links_in_date_range}\n"
            f"- Date range: {start_date.date()} to {end_date.date()}"
        )
        
        self.logger.info(f"Found {links_in_date_range} pages to process within date range")
        return links
    
    def run(self, start_date, end_date, max_workers=None, force_reprocess=False):
        """
        Run the web scraper.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            max_workers (int): Number of worker threads, overrides instance setting if provided
            force_reprocess (bool): Whether to force reprocessing of previously downloaded files
            
        Returns:
            dict: Scraping statistics
        """
        log_function_call(self.logger, "run", args=(start_date, end_date, max_workers, force_reprocess))
        
        # Use provided max_workers or instance default
        max_workers = max_workers or self.max_workers
        
        # Start log consumer thread if not already running
        if not self.log_thread or not self.log_thread.is_alive():
            self.log_thread = threading.Thread(
                target=log_consumer,
                args=(self.log_queue, self.logger),
                daemon=True
            )
            self.log_thread.start()
        
        # Get navigation links
        navigation_links = self.get_navigation_links(start_date, end_date)
        self.logger.info(f"Found {len(navigation_links)} pages to process")
        
        # Initialize statistics
        all_failed_downloads = []
        all_stats = {}
        
        # Process pages with ThreadPoolExecutor
        self.logger.info(f"Starting multithreaded scraping with {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_url = {
                executor.submit(
                    self.process_daily_price_page,
                    url,
                    start_date,
                    end_date,
                    force_reprocess,
                    self.log_queue
                ): url for url in navigation_links
            }
            
            # Process results as they complete
            for future in future_to_url:
                url = future_to_url[future]
                try:
                    failed_downloads, stats_dict = future.result()
                    
                    # Merge results
                    all_failed_downloads.extend(failed_downloads)
                    
                    # Merge statistics dictionaries
                    for month_key, month_stats in stats_dict.items():
                        if month_key not in all_stats:
                            all_stats[month_key] = {
                                'links_explored': 0,
                                'already_downloaded': 0,
                                'new_downloads': 0,
                                'failed_downloads': 0
                            }
                        
                        # Add stats from this thread to the total
                        all_stats[month_key]['links_explored'] += month_stats['links_explored']
                        all_stats[month_key]['already_downloaded'] += month_stats['already_downloaded']
                        all_stats[month_key]['new_downloads'] += month_stats['new_downloads']
                        all_stats[month_key]['failed_downloads'] += month_stats['failed_downloads']
                    
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {str(e)}")
        
        # Log statistics
        stats = self._log_statistics(all_failed_downloads, all_stats)
        
        # Export registry to CSV
        self.registry.export_to_csv()
        
        # Signal to stop log consumer
        self.log_queue.put(None)
        self.log_thread.join()
        
        return stats
    
    def _log_statistics(self, failed_downloads, monthly_stats):
        """
        Log final statistics for the scraping run.
        
        Args:
            failed_downloads (list): List of failed downloads
            monthly_stats (dict): Dictionary of statistics by month
            
        Returns:
            dict: Overall statistics
        """
        # Prepare statistics report
        stats_report = "\nScraping Statistics Summary:\n"
        stats_report += "=" * 50 + "\n\n"
        
        # Monthly statistics
        stats_report += "Monthly Statistics:\n"
        stats_report += "-" * 30 + "\n"
        
        total_links_explored = 0
        total_already_downloaded = 0
        total_new_downloads = 0
        total_failed_downloads = 0
        
        for month, stats in sorted(monthly_stats.items()):
            stats_report += f"\nMonth: {month}\n"
            stats_report += f"  Links explored: {stats['links_explored']}\n"
            stats_report += f"  Already downloaded: {stats['already_downloaded']}\n"
            stats_report += f"  New downloads: {stats['new_downloads']}\n"
            stats_report += f"  Failed downloads: {stats['failed_downloads']}\n"
            
            if stats['links_explored'] > 0:
                success_rate = ((stats['already_downloaded'] + stats['new_downloads']) / 
                              stats['links_explored'] * 100)
                stats_report += f"  Success rate: {success_rate:.1f}%\n"
            
            # Accumulate totals
            total_links_explored += stats['links_explored']
            total_already_downloaded += stats['already_downloaded']
            total_new_downloads += stats['new_downloads']
            total_failed_downloads += stats['failed_downloads']
        
        # Overall statistics
        stats_report += "\nOverall Statistics:\n"
        stats_report += "-" * 30 + "\n"
        stats_report += f"Total months processed: {len(monthly_stats)}\n"
        stats_report += f"Total links explored: {total_links_explored}\n"
        stats_report += f"Total files already present: {total_already_downloaded}\n"
        stats_report += f"Total new downloads: {total_new_downloads}\n"
        stats_report += f"Total failed downloads: {total_failed_downloads}\n"
        
        # Overall success rate
        success_rate = 0
        if total_links_explored > 0:
            success_rate = ((total_already_downloaded + total_new_downloads) / 
                          total_links_explored * 100)
            stats_report += f"Overall success rate: {success_rate:.1f}%\n"
            
        # Add failed downloads section
        if failed_downloads:
            stats_report += "\nFailed Downloads:\n"
            stats_report += "-" * 30 + "\n"
            for failure in failed_downloads[:10]:  # Show first 10 failures
                stats_report += f"\nMonth: {failure['month_key']}\n"
                stats_report += f"File: {failure['filename']}\n"
                stats_report += f"URL: {failure['url']}\n"
                stats_report += f"Error: {failure['error']}\n"
            
            if len(failed_downloads) > 10:
                stats_report += f"\n... and {len(failed_downloads) - 10} more failed downloads.\n"
            
            stats_report += f"\nTotal failed downloads: {total_failed_downloads}\n"
        else:
            stats_report += "\nNo failed downloads to report.\n"
            
        self.logger.info(stats_report)
        
        # Return overall statistics
        return {
            'total_links_explored': total_links_explored,
            'total_already_downloaded': total_already_downloaded,
            'total_new_downloads': total_new_downloads,
            'total_failed_downloads': total_failed_downloads,
            'success_rate': success_rate,
            'months_processed': len(monthly_stats),
            'failed_downloads': failed_downloads
        }
    
    # Helper methods
    def _get_page_year(self, soup, logger):
        """Extract and store the year and month from the page header."""
        page_year = None
        page_month = None
        logger.debug(f"Get page year called")
        
        try:
            # Use lxml to find the header element
            dom = etree.HTML(str(soup))
            header_elements = dom.xpath('//*[@id="t3-content"]/div/div[1]/h1')
            
            if header_elements:
                header = header_elements[0].text
                logger.debug(f"Found page header text: '{header}'")
            else:
                # Fallback: Check for strong element in table
                strong_element = soup.select_one("#cnpv > table > tbody > tr:nth-child(1) > td > strong")
                if strong_element:
                    header = strong_element.text
                    logger.info(f"Found date in table strong element: '{header}'")
                else:
                    logger.error("Could not find header or strong element")
                    return page_year, page_month
            
            if "SIPSA- diario" in header:
                page_year = datetime.now().year
                page_month = datetime.now().month
                logger.info(f"Using current date for SIPSA- diario page: {page_year}-{page_month:02d}")
            else:
                # Parse the month and year from the header
                try:
                    month_num, year = parse_spanish_month_year(header)
                    page_year = year
                    page_month = month_num
                    logger.info(f"Successfully extracted date from header: {year}-{page_month:02d}")
                    
                except ValueError as e:
                    logger.error(f"Failed to parse month/year from header: {str(e)}")
                    logger.debug(f"Header content: '{header}'")
                    page_year = None
                    page_month = None
                
        except Exception as e:
            logger.error(f"Error extracting page year/month: {str(e)}")
            page_year = None
            page_month = None
        
        return page_year, page_month
    
    def _is_header_table(self, table, logger):
        """Check if the table is the header/description table with image."""
        # Check for image in table
        img = table.find('img', alt='Componente precios mayoristas')
        if img:
            logger.info("Found header table with component image - skipping")
            return True
        
        # Check for typical header table content
        text_content = table.get_text(strip=True).lower()
        if 'informa sobre las cotizaciones mayoristas' in text_content:
            logger.info("Found header table with description text - skipping")
            return True
        
        return False
    
    def _find_data_table(self, soup, logger):
        """
        Find the data table using various selectors in priority order.
        Returns the first matching table or None if no matches found.
        """
        # Case 1: Article body tables - get second table if it exists
        article_tables = soup.select("#t3-content > div > div.com-content-article__body > table")
        if len(article_tables) >= 2:
            if not self._is_header_table(article_tables[1], logger):
                logger.info("Found second table using article body CSS selector")
                return article_tables[1]
        elif len(article_tables) == 1 and not self._is_header_table(article_tables[0], logger):
            logger.info("Only one table found using article body CSS selector")
            return article_tables[0]

        # Case 2: Standard tables under #cnpv
        tables = soup.select('#cnpv > table')
        if len(tables) >= 2:
            if not self._is_header_table(tables[1], logger):
                logger.info("Found second table using standard CSS selector (under #cnpv)")
                return tables[1]
        elif len(tables) == 1 and not self._is_header_table(tables[0], logger):
            logger.info("Only one table found using standard CSS selector")
            return tables[0]

        # Case 3: Specific table with table-hover class
        hover_table = soup.select_one("#t3-content > div > div.com-content-article__body > div.row > div > table.table.table-hover.cc_cursor")
        if hover_table and not self._is_header_table(hover_table, logger):
            logger.info("Found table using specific table-hover CSS selector")
            return hover_table

        # Case 4: Generic article body table in row div
        row_table = soup.select_one("#t3-content > div > div.com-content-article__body > div.row > div > table")
        if row_table and not self._is_header_table(row_table, logger):
            logger.info("Found table using row div CSS selector")
            return row_table

        # Case 5: XPath fallback for tables
        logger.warning("CSS selectors failed, trying XPath selector...")
        dom = etree.HTML(str(soup))
        xpath_tables = dom.xpath('//div[@class="com-content-article__body"]//table')
        if len(xpath_tables) >= 2:
            table = BeautifulSoup(etree.tostring(xpath_tables[1]), 'html.parser')
            if not self._is_header_table(table, logger):
                logger.info("Found second table using XPath selector")
                return table
        elif len(xpath_tables) == 1:
            table = BeautifulSoup(etree.tostring(xpath_tables[0]), 'html.parser')
            if not self._is_header_table(table, logger):
                logger.info("Only one table found using XPath selector")
                return table

        # If no valid table found or only header table found, try to find Anexo links
        anexo_table = self._find_anexo_links(soup, logger)
        if anexo_table:
            return anexo_table

        logger.error("No suitable table or Anexo links found with any selector")
        return None
        
    def _find_anexo_links(self, soup, logger):
        """Search for and create a table from links in the article body."""
        logger.info("Starting _find_anexo_links method")
        
        article_body = soup.select_one("#t3-content > div > div.com-content-article__body")
        if not article_body:
            logger.warning("No article body found")
            return None
        
        # Find all links in paragraphs within the article body
        all_links = article_body.select("p a")
        if not all_links:
            all_links = article_body.find_all('a')  # Fallback to all links if none found in paragraphs
        
        valid_links = []
        for link in all_links:
            href = link.get('href')
            text = link.get_text(strip=True)
            
            if not href:
                continue
            
            # Try to extract year from the href
            year_match = re.search(r'_(\d{4})\.', href)
            if not year_match:
                year_match = re.search(r'/(\d{4})/', href)
            
            if year_match:
                year = year_match.group(1)
                
                # Handle different date formats
                if text.lower().startswith('anexo'):
                    date_text = text.replace('Anexo - ', '')
                else:
                    date_text = re.sub(r'\s*de\s+\d{4}', '', text)
                
                valid_links.append((link, date_text, year))

        if valid_links:
            try:
                new_html = BeautifulSoup('<div><table></table></div>', 'html.parser')
                table = new_html.find('table')
                
                for link, date_text, year in valid_links:
                    # Create new tags
                    row = new_html.new_tag('tr')
                    date_cell = new_html.new_tag('td')
                    link_cell = new_html.new_tag('td')
                    
                    # Format date consistently
                    date_cell.string = f"{date_text} - {year}"
                    
                    # Create a new link tag and copy attributes
                    new_link = new_html.new_tag('a')
                    new_link['href'] = link.get('href', '')
                    new_link.string = link.get_text(strip=True)
                    
                    # Add the new link to the cell
                    link_cell.append(new_link)
                    
                    # Build the row
                    row.append(date_cell)
                    row.append(link_cell)
                    table.append(row)

                return new_html
                
            except Exception as e:
                logger.error(f"Error creating table structure: {str(e)}")
                return None
        
        logger.info("No valid links found to process")
        return None