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
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=692151586%2C692151587%2C695060900%2C722719832%2C704804416%2C692152260%2C692157406%2C692151999%2C704408069%2C704370425%2C722719971%2C722719825%2C692157301&categoryId=1030471396&appId=1", # women's jackets
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=697861766%2C692151732%2C692152123%2C704408070%2C692152311%2C695244989%2C704408069%2C692151731%2C721906242%2C703340762%2C696254671%2C696066096&categoryId=1030440811&appId=1", # women's coats & trenchcoats
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=721441398%2C721441391%2C699722884%2C701560419%2C707184228%2C715490006%2C700439164%2C698482086%2C717699773%2C722219092%2C721906760%2C701560417%2C701190246%2C701009646&categoryId=1030204693&appId=1" # women's jeans
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=710899511%2C727752473%2C723726346%2C700509289%2C717311816%2C711237328%2C723726843%2C721906831%2C721906727%2C721906214%2C721149649%2C721149413&categoryId=1030207192&appId=1", # women's pants
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=718008655%2C721442279%2C721442278%2C721442184%2C721442185%2C721442183%2C692757339%2C698625077%2C716676115%2C692757354%2C692757338%2C723726859%2C722720075%2C722719884%2C722719602&categoryId=1030204670&appId=10", #women's sweater & cardigans
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=710733667%2C700616155%2C720499944%2C693333044%2C714313538%2C711237333%2C724119720%2C723726871%2C723726483%2C721906732%2C721906202%2C721149562%2C721149493%2C720244318&categoryId=1030422324&appId=1", # women's tops & bodysuits
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=716359993%2C719808043%2C727752501%2C721441630%2C692151789%2C692151793%2C702818108%2C721149195%2C711204982%2C721149244%2C721149152%2C715345206%2C716359994%2C707132790&categoryId=1030204632&appId=1", # women's t-shirts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=722752759%2C693302814%2C725706279%2C716359997%2C725706485%2C717518380%2C713886255%2C721149494%2C721149024%2C721149196%2C721149193%2C715723026%2C714839223%2C714314176&categoryId=1030204661&appId=1", # women's sweatshirts & hoodies
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=717552874%2C721441453%2C721441951%2C698625044%2C708542822%2C717515960%2C693309804%2C716581539%2C725707048%2C710675020%2C715222798%2C706506056%2C711518893%2C711518894%2C710899510&categoryId=1030204617&appId=1", # women's dresses
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=718538479%2C724119615%2C720244810%2C719664741%2C718538478%2C713643363%2C721149120%2C709288325%2C707944440%2C707944439%2C700509263%2C697299765%2C695274345&categoryId=1030204645&appId=1", # women's shirts & blouses
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=708126985%2C710899520%2C697656592%2C721906208%2C726726262%2C696614828%2C695434577%2C697299748%2C696614827%2C709246514%2C725706975%2C697075210%2C697075209%2C698625036%2C698625034%2C701724705%2C709235669%2C708126966%2C705051409%2C693139281&categoryId=1030543096&appId=1", # women's blazers & suits
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=712968435%2C709914523%2C694880383%2C709914522%2C714029726%2C727664863%2C698445197%2C716360003%2C715350205%2C711237348%2C709288327%2C707132780%2C707132779%2C706506059&categoryId=1030475966&appId=1", # women's skirts & shorts
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=727752438%2C714655496%2C694046068%2C727664868%2C696066027%2C697267080%2C722719887%2C694040391%2C694046069%2C714314208%2C697267094%2C697465523%2C715296486%2C707385016%2C707385018%2C727752456%2C726058743%2C726058742&categoryId=1030299058&appId=1", # women's sweatsuits
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=722719741%2C711518876%2C721446151%2C721442268%2C721442196%2C702401354%2C719216416%2C696614821%2C702401346%2C716858000%2C694017960%2C698806627%2C699693850%2C726824903%2C726630720%2C721441965%2C726630886%2C723828186%2C701760141%2C720244387%2C701760107%2C701760108%2C699277205%2C701190265%2C701190249&categoryId=1030207001&appId=1", # women's shoes
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=710353106%2C723726730%2C712171965%2C725706748%2C721442112%2C721442092%2C726726544%2C694394797%2C696614824%2C692764710%2C708126958%2C721906554%2C692151870%2C708126959%2C694880399%2C692151474%2C718321453%2C702401350%2C721441974%2C721442033&categoryId=1030207022&appId=1", # women's bags
                    "https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/productsArray?languageId=-15&productIds=713100382%2C711204952%2C727664896%2C714029729%2C714029698%2C721446524%2C714029728%2C721446523%2C716980307%2C711204958%2C721446424%2C721442248%2C721442235%2C709914523%2C721446420%2C707909022%2C702191704&categoryId=1030204877&appId=1", # women's accessories
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