# Pull & Bear Fashion Scraper

A comprehensive scraper for Pull & Bear fashion products that extracts product information, generates image embeddings using Google's SigLIP model, and stores everything in a Supabase database.

## Features

- **Complete Product Catalog**: Scrapes all products from Bershka's men's and women's categories (thousands of products)
- **Two-Step API Approach**: First fetches ALL product IDs, then batch-fetches product details
- **Image Embeddings**: Generates 768-dimensional embeddings using `google/siglip-base-patch16-384`
- **Supabase Integration**: Stores all data in PostgreSQL with vector support
- **Local File Support**: Can load product IDs from local JSON files when API is blocked
- **Modular Architecture**: Clean separation of concerns with YAML configuration

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Set Environment Variables
Create a `.env` file:
```
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
```

### 3. Configure Category URLs (Required)
Since Pull & Bear's API blocks direct requests, you need to provide the category API URLs.

Edit `category_urls.txt` and paste the URLs for each category:
```
# Men's categories
1030204838=https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/category/1030204838/product?showProducts=false&showNoStock=false&appId=1&languageId=-15&locale=en_GB
1030204731=https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/category/1030204731/product?showProducts=false&showNoStock=false&appId=1&languageId=-15&locale=en_GB
...
```

The scraper will automatically fetch the latest product IDs from these URLs. When Pull & Bear updates their catalog, just update the URLs in this file.

**Alternative**: You can still save local JSON files to `category_data/` folder as fallback, but URLs are preferred for automatic updates.

### 4. Run the Scraper
```bash
python pull_bear_scraper.py              # Full scrape
python pull_bear_scraper.py --limit 10   # Test with 10 products
```

## How It Works

### Two-Step Approach
1. **Step 1**: Load ALL product IDs from category endpoint (e.g., 888 for men's category)
   - Source: Local JSON files in `category_data/` or API

2. **Step 2**: Fetch product details in batches of 50
   - Uses `productsArray` endpoint with product IDs

### Data Flow
```
category_data/*.json → Product IDs → Batch API calls → Transform → Embeddings → Supabase
```

## Categories Scraped

| Category | ID | Gender |
|----------|-----|--------|
| Men's Jackets & Coats | 1030204838 | MAN |
| Men's Jeans | 1030204731 | MAN |
| Men's Pants | 1030204721 | MAN |
| Men's Sweatshirts & Hoodies | 1030204823 | MAN |
| Men's T-Shirts | 1030204792 | MAN |
| Men's Sweaters & Cardigans | 1030204757 | MAN |
| Men's Shirts | 1030204767 | MAN |

## Project Structure

```
├── cli.py              # Main entry point
├── api_ingestor.py     # API data extraction with JMESPath
├── transform.py        # Data transformation to Supabase schema
├── embeddings.py       # Image embedding generation (SigLIP)
├── http_client.py      # HTTP client with session management
├── db.py               # Supabase database operations
├── config.py           # Configuration management
├── sites.yaml          # Site-specific configurations
├── category_data/      # Local JSON files with product IDs (not in git)
└── CAPTURE_INSTRUCTIONS.md  # How to capture category data
```

## GitHub Actions

The scraper includes automated GitHub Actions workflow that runs daily at midnight and can also be triggered manually.

### Setup Required
Add these secrets to your repository (Settings → Secrets → Actions):
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase anon key

### Running
1. **Automatic**: Runs daily at midnight UTC
2. **Manual**: Go to Actions → "Scrape Pull & Bear Products" → Click "Run workflow" → Choose "full" or "test" mode

## Database Schema

```sql
create table public.products (
  id text not null,
  source text null,
  product_url text null,
  image_url text not null,
  brand text null,
  title text not null,
  description text null,
  category text null,
  gender text null,
  price double precision null,
  currency text null,
  created_at timestamp with time zone null default now(),
  metadata text null,
  second_hand boolean null default false,
  embedding public.vector null,
  constraint products_pkey primary key (id),
  constraint products_source_product_url_key unique (source, product_url)
);
```

## Output Fields

Each product record includes:
- **id**: Unique product identifier
- **source**: "scraper"
- **brand**: "Pull & Bear"
- **product_url**: Full product URL
- **image_url**: Product image URL
- **title**: Product name
- **gender**: "MAN" or "WOMAN"
- **category**: "footwear", "accessory", or null (for clothing)
- **price**: Price value (e.g., 45.9)
- **currency**: "EUR"
- **second_hand**: false
- **embedding**: 768-dimensional vector
- **created_at**: Timestamp

## Troubleshooting

### API Returns 403
The API blocks direct requests. Capture category JSON files from your browser using the instructions in `CAPTURE_INSTRUCTIONS.md`.

### Missing Products
Make sure all category JSON files are in `category_data/` folder. Check `CAPTURE_INSTRUCTIONS.md` for the full list of URLs.

### Embedding Errors
Some products have video URLs instead of images. These are automatically skipped.
