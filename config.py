import os
from typing import Dict, List, Any
try:
    from dotenv import load_dotenv
    import yaml
except ImportError:
    # Fallback if packages not available
    def load_dotenv():
        pass
    yaml = None


def load_env():
    """Load environment variables from .env file."""
    load_dotenv()


def get_supabase_env() -> Dict[str, str]:
    """Get Supabase environment variables."""
    return {
        "url": os.getenv("SUPABASE_URL", ""),
        "key": os.getenv("SUPABASE_KEY", "")
    }


def get_default_headers() -> Dict[str, str]:
    """Get default HTTP headers."""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en;q=0.9',
    }


def load_sites_config(config_file: str = "sites.yaml") -> List[Dict[str, Any]]:
    """Load sites configuration from YAML file."""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            # If it's a dict with a sites key, return the sites list
            if isinstance(config, dict) and 'sites' in config:
                return config['sites']
            # If it's already a list, return it
            elif isinstance(config, list):
                return config
            # If it's a dict but not with sites key, wrap it in a list
            else:
                return [config]
    except FileNotFoundError:
        print(f"Config file {config_file} not found, using default Bershka config")
        return [{
            "brand": "Bershka",
            "merchant": "Bershka",
            "source": "scraper",
            "country": "us",
            "debug": True,
            "respect_robots": False,
            "api": {
                "endpoints": [
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=709241579%2C713002755%2C713747231%2C697267077%2C694040393%2C694040395%2C697861745%2C707683118%2C692151749%2C697267076%2C707646400%2C692757290%2C692151747&categoryId=1030204838&appId=1", # men's jackets & coats
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=714029696%2C726630843%2C698445207%2C698445206%2C695245012%2C698445210%2C698445209%2C694046095%2C694046096%2C692161634%2C695434549%2C692161629%2C699548199%2C692161632%2C711204963%2C711750042%2C692156849%2C693139309%2C692156848%2C692156846%2C692156850%2C692156847%2C702129748%2C712816114%2C712816115%2C702129749%2C702129747%2C717735610%2C717735611&categoryId=1030204731&appId=1", # men's jeans
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=693840151%2C693840152%2C692157444%2C692157446%2C693840153%2C716359979%2C716359981%2C693840134%2C695878849%2C701515229%2C700056171%2C692161543&categoryId=1030204721&appId=1", # men's pants
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=721441957%2C721441953%2C721441707%2C721441627%2C721441624%2C692761898%2C692151489%2C692151625%2C692151623%2C725706752%2C718626101%2C718626105%2C718626100%2C717415323%2C695907697%2C714655498%2C707385032%2C692151909&categoryId=1030204823&appId=1", # men's sweatshirts & hoodies
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=725068027%2C725068025%2C692151703%2C715794594%2C699865416%2C695434559%2C702129746%2C699693856%2C696614819%2C723941794%2C697267101%2C699693860%2C692161447&categoryId=1030204792&appId=1", # men's t-shirts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=707909007%2C695245011%2C695245003%2C692764715%2C692161350%2C692151583%2C698445226%2C697075217%2C692156967%2C692156965%2C711518892%2C711204936&categoryId=1030204757&appId=1", # men's sweaters & cardigans
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=721179962%2C721179960%2C718008657%2C707646432%2C713002773%2C713002754%2C709246510%2C718008658%2C713002772%2C712667066%2C712667064%2C707646431&categoryId=1030204767&appId=1", # men's shirts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=725707109%2C725940647%2C692151623%2C715722822%2C692151624%2C693840151%2C714314099%2C692151625%2C693840152%2C722719695%2C692151909%2C693840134%2C701982111%2C692161482%2C700056168%2C715722833&categoryId=1030299061&appId=1", # men's sweatsuits
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=721441641%2C695245011%2C699865416%2C713100378%2C701515287%2C692161500%2C711204936%2C706360077%2C706360075%2C703079608%2C701724479%2C701515259&categoryId=1030204788&appId=1", # men's polo shirts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=692152424%2C692152415%2C692152414%2C692152416%2C692152417%2C692157175%2C692157177%2C692157176%2C692152433%2C692151514%2C692151512%2C692161618%2C692161580%2C692157466%2C692157115%2C692152435&categoryId=1030204713&appId=1", # men's shorts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=695245003%2C695245004%2C695245002%2C695245001%2C721804754%2C721804756%2C721804752%2C721804758%2C692151624%2C692151625%2C692151626%2C692151623%2C711646894%2C711646895%2C692151621%2C711647474%2C692151622%2C711647475%2C692980728%2C692980725%2C692757305%2C692980724%2C692757306%2C692980726%2C692980729%2C692157138&categoryId=29512&appId=1", # men's basics
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=692157226%2C706173177%2C692157225%2C692161294%2C721446627%2C721446707%2C721446549%2C692157299%2C713556117%2C692157378%2C721442003%2C701190230%2C701190232%2C697861797%2C697656599%2C707646399%2C706632120%2C720244852%2C694532981%2C714314448%2C706632152%2C721804777&categoryId=1030207045&appId=1", # men's shoes
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=705282962%2C721446639%2C705282954%2C692161476%2C726630670%2C696097123%2C701944141%2C702401342%2C719760980%2C702191697%2C692156987%2C695559561%2C692156991%2C697207081%2C698993659%2C714313586%2C696614839%2C692151669%2C695574455&categoryId=1030465398&appId=1", # men's bags & backpacks
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=707132786%2C706635138%2C706632138%2C706635140%2C706635137%2C706635136%2C721442172%2C721441635%2C721069131%2C721442170%2C707646409%2C713104033%2C716209001%2C706635139%2C710213806%2C706632143&categoryId=1030207098&appId=1", # men's accessories
                ],
                "items_path": "products",
                "field_map": {
                    "external_id": "id",
                    "product_id": "id",
                    "title": ["nameEn", "name"],
                    "description": ["bundleProductSummaries[0].detail.longDescription", "bundleProductSummaries[0].detail.description"],
                    "gender": "bundleProductSummaries[0].sectionNameEN",
                    "price": "bundleProductSummaries[0].detail.colors[0].sizes[0].price",
                    "currency": "'EUR'",
                    "image_url": "bundleProductSummaries[0].detail.colors[0].image.url",
                    "product_url": "bundleProductSummaries[0].productUrl",
                    "brand": "'Bershka'",
                    "sizes": "bundleProductSummaries[0].detail.colors[0].sizes[].name"
                },
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-GB,en;q=0.9"
                },
                "debug": True,
                "prewarm": [
                    "https://www.bershka.com/us/",
                    "https://www.bershka.com/us/men.html",
                    "https://www.bershka.com/us/women.html"
                ]
            }
        }]
    except Exception as e:
        print(f"Error loading config: {e}")
        return []


def get_site_configs(all_sites: List[Dict[str, Any]], filter_brands: str) -> List[Dict[str, Any]]:
    """Filter sites based on brand names."""
    if filter_brands.lower() == "all":
        return all_sites

    brand_list = [b.strip() for b in filter_brands.split(",")]
    return [site for site in all_sites if site.get("brand", "").lower() in [b.lower() for b in brand_list]]


# Pull & Bear Configuration
PULL_BEAR_BASE_URL = "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455"
PULL_BEAR_APP_ID = 1
PULL_BEAR_LANGUAGE_ID = -15
PULL_BEAR_LOCALE = "en_GB"

# Legacy Bershka Configuration (for backward compatibility)
BERSHKA_BASE_URL = "https://www.bershka.com/itxrest/3/catalog/store/45009578/40259549"
BERSHKA_APP_ID = 1
BERSHKA_LANGUAGE_ID = -15
BERSHKA_LOCALE = "en_GB"

# Common Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

BATCH_SIZE = 10
MAX_WORKERS = 4
EMBEDDING_MODEL = "google/siglip-base-patch16-384"
PRODUCT_LIMIT = 0  # 0 = no limit

# Category mappings for Pull & Bear
CATEGORY_IDS = {
    'men': {
        'jackets_coats': {'category_id': '1030204838'},
        'jeans': {'category_id': '1030204731'},
        'pants': {'category_id': '1030204721'},
        'sweatshirts_hoodies': {'category_id': '1030204823'},
        'tshirts': {'category_id': '1030204792'},
        'sweaters_cardigans': {'category_id': '1030204757'},
        'shirts': {'category_id': '1030204767'},
        'category_1030299061': {'category_id': '1030299061'},
        'category_1030204788': {'category_id': '1030204788'},
        'category_1030204713': {'category_id': '1030204713'},
        'category_29512': {'category_id': '29512'},
        'category_1030207045': {'category_id': '1030207045'},
        'category_1030465398': {'category_id': '1030465398'},
        'category_1030207098': {'category_id': '1030207098'},
    },
    'women': {
        # Add women's categories as needed
    }
}

GENDER_MAPPING = {
    'MAN': 'men',
    'WOMAN': 'women',
    '': 'unisex'
}

CATEGORY_CLASSIFICATION = {
    # Add category classifications as needed
}