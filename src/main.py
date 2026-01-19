import argparse
import sys
import logging
import csv
from typing import List
from src.recipes.dggca_recipe import DggcaExtractor
from src.recipes.gst_recipe import GstExtractor

logger = logging.getLogger(__name__)

def parse_pages(pages_str: str) -> List[int]:
    """Parse page string '1,2,5-7' into list of integers."""
    pages = []
    if not pages_str:
        return None
    for part in pages_str.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            pages.extend(range(start, end + 1))
        else:
            pages.append(int(part))
    # fitz uses 0-indexed pages, user likely provides 1-indexed
    return [p - 1 for p in pages]

def process_gst_csv(input_csv: str, output_csv: str):
    """Process GST CSV input."""
    # This resembles the old logic but uses the new Extractor
    if not input_csv:
        logger.error("Input CSV required for GST batch mode")
        sys.exit(1)
        
    logger.info(f"Processing GST CSV: {input_csv}")
    
    ids_to_process = []
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "gstin" in row:
                    ids_to_process.append(row["gstin"].strip())
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_csv}")
        sys.exit(1)
    
    all_data = []
    extractor = GstExtractor(base_url="") # Dummy URL, updated later
    
    for gstin in ids_to_process:
        if not gstin: continue
        url = f"https://gst.jamku.app/gstin/{gstin}"
        logger.info(f"Scraping GSTIN: {gstin}")
        
        # We can re-instantiate or just update base_url if possible, 
        # but re-instantiating is safer for state
        # Or simpler:
        extractor = GstExtractor(base_url=url) 
        
        try:
            result = extractor.extract()
            if result:
                all_data.extend(result)
        except Exception as e:
            logger.error(f"Failed to scrape {gstin}: {e}")
            
    # Save results
    if all_data:
        extractor.save(all_data, output_csv)
        logger.info(f"Saved {len(all_data)} records to {output_csv}")
    else:
        logger.warning("No data extracted for GST")

def main():
    parser = argparse.ArgumentParser(description="Scraper Tool")
    parser.add_argument("--source", type=str, required=True, choices=["dggca", "gst"], help="Source to scrape")
    parser.add_argument("--input", type=str, help="Input file path (PDF for dggca, CSV for gst)")
    parser.add_argument("--output", type=str, required=True, help="Output file path")
    parser.add_argument("--pages", type=str, help="Pages to scrape (e.g. '1,2,3' or '1-5') for PDF")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    if args.source == "dggca":
        if not args.input:
             logger.error("Error: --input is required for dggca")
             sys.exit(1)
        
        pages = parse_pages(args.pages)
        extractor = DggcaExtractor(pdf_path=args.input, source=args.source)
        data = extractor.extract(pages=pages)
        extractor.save(data, args.output)
        logger.info(f"DGGCA extraction complete. Saved to {args.output}")
        
    elif args.source == "gst":
        # Check if input is a CSV file
        if args.input:
            process_gst_csv(args.input, args.output)
        else:
             logger.error("GST source requires --input pointing to a CSV file")
             sys.exit(1)

if __name__ == "__main__":
    main()
