
# import asyncio
# import json
# from datetime import datetime
# from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
# from bs4 import BeautifulSoup
# from openai import OpenAI
# import re
# import gspread
# from google.oauth2.service_account import Credentials
# import os
# from typing import List, Dict
# import hashlib
# import pytz
# import time


# # ============================================================
# # Custom Exceptions
# # ============================================================
# class CreditExhaustedException(Exception):
#     pass


# # ============================================================
# # Perplexity Client
# # ============================================================
# perplexity_client = OpenAI(
#     api_key=os.environ.get("PERPLEXITY_API_KEY"),
#     base_url="https://api.perplexity.ai"
# )

# ANALYSIS_MODEL       = "sonar-pro"
# SCRIPT_MODEL         = "sonar-reasoning-pro"

# ANALYSIS_INPUT_COST  = 1.0 / 1_000_000
# ANALYSIS_OUTPUT_COST = 1.0 / 1_000_000
# SCRIPT_INPUT_COST    = 2.0 / 1_000_000
# SCRIPT_OUTPUT_COST   = 8.0 / 1_000_000

# IST = pytz.timezone('Asia/Kolkata')


# # ============================================================
# # Config
# # ============================================================
# GOOGLE_SHEETS_CREDENTIALS_FILE = "credentials.json"
# GOOGLE_SHEET_NAME               = "Kalakar Katta"
# GOOGLE_WORKSHEET_NAME           = "Sheet1"

# TARGET_NEWS_SCRIPTS  = 8   # from entertainment news sites
# TARGET_STORY_SCRIPTS = 2   # from indianfilmhistory.com
# TARGET_SCRIPTS       = TARGET_NEWS_SCRIPTS + TARGET_STORY_SCRIPTS  # = 10

# # ============================================================
# # Story Sites — Indian Film History blogs
# # ============================================================
# STORY_SITES = [
#     {
#         "name": "Indian Film History - Blogs",
#         "url": "https://www.indianfilmhistory.com/blogs",
#         "link_pattern": "indianfilmhistory.com",
#         "target": 2,
#     },
#     {
#         "name": "Indian Film History - Subdomain",
#         "url": "https://blogs.indianfilmhistory.com/",
#         "link_pattern": "indianfilmhistory.com",
#         "target": 2,
#     },
# ]

# # ============================================================
# # Film Industry Keywords
# # ============================================================
# FILM_KEYWORDS = [
#     'bollywood', 'बॉलीवुड', 'hindi film', 'हिंदी चित्रपट', 'हिंदी सिनेमा',
#     'box office', 'बॉक्स ऑफिस', 'ott release', 'ott', 'नेटफ्लिक्स', 'amazon prime',
#     'disney hotstar', 'zee5', 'sonyliv',
#     'मराठी चित्रपट', 'marathi film', 'मराठी सिनेमा', 'marathi cinema',
#     'मराठी नाटक', 'marathi natak', 'मराठी मालिका', 'marathi serial',
#     'मराठी कलाकार', 'zee marathi', 'star pravah', 'colors marathi',
#     'tollywood', 'kollywood', 'sandalwood', 'mollywood',
#     'tamil film', 'तमिळ चित्रपट', 'telugu film', 'तेलुगू चित्रपट',
#     'kannada film', 'कन्नड चित्रपट', 'malayalam film', 'मल्याळम चित्रपट',
#     'south indian film', 'साउथ इंडियन',
#     'चित्रपट', 'सिनेमा', 'film', 'movie', 'actor', 'actress',
#     'अभिनेता', 'अभिनेत्री', 'दिग्दर्शक', 'director', 'producer', 'निर्माता',
#     'trailer', 'ट्रेलर', 'teaser', 'टीझर', 'song', 'गाणे', 'music launch',
#     'film release', 'चित्रपट प्रदर्शन', 'premiere', 'प्रीमियर',
#     'award', 'पुरस्कार', 'filmfare', 'national award', 'राष्ट्रीय पुरस्कार',
#     'celebrity', 'सेलिब्रिटी', 'star', 'स्टार',
#     'shahrukh', 'shah rukh', 'salman khan', 'aamir khan', 'akshay kumar',
#     'deepika', 'priyanka', 'katrina', 'ranveer', 'ranbir',
#     'prabhas', 'allu arjun', 'vijay', 'rajinikanth', 'dhanush',
#     'mahesh babu', 'ntr', 'ram charan', 'yash', 'nayanthara',
#     'amitabh', 'hrithik', 'tiger shroff',
#     'नाना पाटेकर', 'nana patekar', 'सचिन', 'sachin pilgaonkar',
#     'महेश मांजरेकर', 'स्वप्निल जोशी', 'ankush chaudhary',
#     'सई ताम्हणकर', 'amruta khanvilkar', 'भारत जाधव',
#     'नागराज मंजुळे', 'nagraj manjule', 'sairat', 'झुंड', 'jhund',
#     # Extra history/story keywords for indianfilmhistory
#     'film history', 'cinema history', 'old bollywood', 'classic film',
#     'golden era', 'vintage', 'legendary', 'untold story', 'behind the scenes',
#     'interesting fact', 'did you know', 'kishore kumar', 'lata mangeshkar',
#     'guru dutt', 'raj kapoor', 'dilip kumar', 'dev anand', 'madhubala',
#     'nargis', 'meena kumari', 'waheeda rehman', 'zeenat aman',
# ]

# # ============================================================
# # Categories
# # ============================================================
# VALID_CATEGORIES = [
#     "bollywood", "marathi_cinema", "south_cinema",
#     "ott", "awards", "celebrity", "general_entertainment",
#     "film_story"   # ← NEW: for indianfilmhistory story scripts
# ]

# CATEGORY_DISPLAY = {
#     "bollywood":             "BOLLYWOOD",
#     "marathi_cinema":        "MARATHI",
#     "south_cinema":          "SOUTH",
#     "ott":                   "OTT",
#     "awards":                "AWARDS",
#     "celebrity":             "CELEBRITY",
#     "general_entertainment": "ENTERTAINMENT",
#     "film_story":            "🎭 STORY",
# }

# REFUSAL_KEYWORDS = [
#     "I appreciate", "I should clarify", "I'm Perplexity",
#     "search assistant", "I'm not able", "I cannot create",
#     "Would you like", "clarify my role", "I'm an AI",
#     "as an AI", "I don't create",
#     "मुझे खेद है", "मैं इस अनुरोध", "खोज परिणामों में",
#     "प्रदान किए गए", "कृपया स्पष्ट करें", "मैं सही तरीके",
#     "विशिष्ट तथ्य नहीं", "आवश्यक माहिती",
#     "मला खेद आहे", "मला क्षमस्व", "उत्तर देण्यासाठी आवश्यक",
#     "शोध परिणामांमध्ये", "कृपया एक पूर्ण बातमी",
#     "अधिक संबंधित शोध", "विशिष्ट घटना", "तपशील पुनः तपास",
#     "मी Perplexity", "मी perplexity", "माझी भूमिका",
#     "मूळ कार्याच्या विरुद्ध", "script लिहिण्याची विनंती",
#     "सूचना देणे", "संशोधित उत्तरे", "मी एक AI",
#     "script writer नाही", "मी तयार करू शकत नाही",
#     "शोध निकालांमध्ये", "मेल होत नाही", "script तयार करू शकतो पण",
#     "विस्तृत search results", "स्पष्ट करा"
# ]

# SKIP_TITLE_KEYWORDS = [
#     'राशीभविष्य', 'राशिभविष्य', 'ज्योतिष', 'पूजा', 'अध्यात्म',
#     'horoscope', 'rashifal', 'astrology', 'dharm', 'puja',
#     'utility', 'यूटिलिटी', 'आध्यात्मिक', 'spirituality',
#     'धार्मिक परंपरा', 'मंदिर', 'व्रत', 'उपवास', 'rashibhavishya',
#     'अध्यात्म बातम्या', 'धार्मिक', 'ज्योतिष',
#     'राजकारण', 'politics', 'crime', 'गुन्हा', 'अपघात', 'accident',
#     'शेअर बाजार', 'stock market', 'हवामान', 'weather'
# ]

# SKIP_CONTENT_KEYWORDS = [
#     'tv9 मराठी एक 24/7 मराठी भाषिक वृत्तवाहिनी',
#     'अध्यात्म बातम्यांचा विशेष विभाग आहे जो',
#     'राशीभविष्य, मंदिरातील पूजा, धार्मिक परंपरा',
#     'आध्यात्मिक जीवनाची संपूर्ण माहिती',
#     'धार्मिक आणि आध्यात्मिक विषयांवर सर्वांग माहिती',
#     'यूटिलिटी बातम्या म्हणजे काय',
#     'utility news definition'
# ]

# SKIP_URL_PATTERNS = [
#     'javascript:', 'mailto:', '#',
#     '/category/', '/tag/', '/author/',
#     'facebook.com', 'twitter.com', 'instagram.com',
#     'youtube.com', 'whatsapp.com', '/myaccount/',
#     '/install_app', '/advertisement', '/epaper',
#     'web-stories', 'photo-gallery', '/videos/',
#     '/games/', '/jokes/', '/terms-and-conditions',
#     '/topic/', '/widget/', '/livetv',
#     'articlelist', '/live',
#     '/utility/', '/utilities/',
#     '/adhyatma/', '/astrology/', '/rashifal/',
#     '/horoscope/', '/jyotish/', '/puja/',
#     '/dharm/', '/dharma/', '/spirituality/',
#     '/rashibhavishya/', '/religion/',
#     '/politics/', '/crime/', '/sports/', '/economy/',
#     '/business/', '/technology/', '/education/',
#     '/thane/', '/pune/', '/nashik/', '/mumbai-news/',
# ]

# SCRIPT_CTA = "तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."

# # ============================================================
# # Example story scripts (used as few-shot style reference in prompt)
# # ============================================================
# STORY_SCRIPT_EXAMPLE_1 = """ये दोस्ते हम नाही तोडेंगे — हे गाणं वाजलं की डोळ्यासमोर काय येतं?
# ती बाइक, साइडट्रॉली, आणि एक मेकांसाठी जीव द्यायला तयार असलेले — जय आणि वीरू.
# जय एकदम शांत, गंभीर — तर वीरू एकदम मस्तीखोर, इमोशनल.
# दोघांची केमिस्ट्री भन्नाट होती!
# पन्नास वर्षे झाली तरी ही जोडी मैत्रीचा गोल्ड स्टँडर्ड आहे.
# ती साइडट्रॉली वाली बाइक — फक्त बाइक नव्हती, ती मैत्रीचं प्रतीक होती!
# पण खरा ट्विस्ट आहे त्या टॉसच्या कॉइनमध्ये.
# शेवटच्या सीनमध्ये जेव्हा जय, वीरूला वाचवण्यासाठी स्वतःचा जीव देतो,
# तेव्हा गुपित उलगडतं — त्या कॉइनच्या दोन्ही बाजूंनी हेड्सच होतं!
# जय ने नेहमीच वीरूला जिंकवण्यासाठी स्वतःच्या जीवाशी खेळ केला.
# हीच असते खरी दोस्ती.
# गाल्या येतील, पैसा येईल — पण जय-वीरूसारखा मित्र?
# तुम्हाला असा एखादा जय आहे का?
# त्याला हा व्हिडिओ नक्की शेअर करा, टॅग करा आणि सांगा — ये दोस्ती हम नहीं तोडेंगे!
# आणि तुमच्याकडे असाच एखादा किस्सा असेल तर सांगा पटापट.
# तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."""

# STORY_SCRIPT_EXAMPLE_2 = """तुम्हाला वाटत होतं बॉलीवूड अभिनेत्रीला माझ्या डान्समध्ये परफेक्ट असायला हवं?
# पण झीनत अमान यांनी ही व्याख्याच बदलून टाकली!
# कधीच डान्सर ट्रेनिंग घेतलं नाही — तरीही 'ओ दिवाने' मध्ये त्यांनी असा काही जादू केला!
# तो विग आणि हाय किक्स आठवतायत का?
# सुरुवातीला दिग्दर्शकांना टेन्शन होतं — पण मग त्यांनी स्ट्रेटजी बदलली.
# झीनतला कोरियोग्राफ करायचं नाही — फक्त त्यांची स्टाईल फॉलो करायची!
# कठीण कोरियोग्राफीपेक्षा त्यांचा हा नैसर्गिक वावर लोकांच्या काळजाचा ठाव करून गेला.
# कलेपेक्षा आत्मविश्वास मोठा असतो — हे झीनत यांनी सिद्ध केलं!
# झीनत अमानचा हा किस्सा तुम्हाला कसा वाटला?
# कमेंट्समध्ये नक्की सांगा आणि अशाच वेगवेगळ्या किस्त्यांसाठी फॉलो करा.
# तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."""


# # ============================================================
# # Token Tracking
# # ============================================================
# total_input_tokens  = 0
# total_output_tokens = 0
# total_cost          = 0.0
# processed_hashes    = set()


# # ============================================================
# # News Sites — Entertainment sections
# # ============================================================
# NEWS_SITES = [
#     {
#         "name": "Maharashtra Times - Entertainment",
#         "url": "https://maharashtratimes.com/entertainment",
#         "link_pattern": "maharashtratimes.com",
#         "target": 3,
#         "fetch_limit": 30
#     },
#     {
#         "name": "TV9 Marathi - Entertainment",
#         "url": "https://www.tv9marathi.com/entertainment",
#         "link_pattern": "tv9marathi.com",
#         "target": 3,
#         "fetch_limit": 30
#     },
#     {
#         "name": "ABP Majha - Entertainment",
#         "url": "https://marathi.abplive.com/entertainment",
#         "link_pattern": "abplive.com",
#         "target": 3,
#         "fetch_limit": 30
#     },
#     {
#         "name": "Lokmat - Entertainment",
#         "url": "https://www.lokmat.com/entertainment/",
#         "link_pattern": "lokmat.com",
#         "target": 3,
#         "fetch_limit": 30
#     },
#     {
#         "name": "NDTV Marathi - Entertainment",
#         "url": "https://marathi.ndtv.com/entertainment",
#         "link_pattern": "marathi.ndtv.com",
#         "target": 2,
#         "fetch_limit": 25
#     },
#     {
#         "name": "Bollywood Hungama",
#         "url": "https://www.bollywoodhungama.com/news/bollywood/",
#         "link_pattern": "bollywoodhungama.com",
#         "target": 2,
#         "fetch_limit": 25
#     },
#     {
#         "name": "Pinkvilla",
#         "url": "https://www.pinkvilla.com/entertainment",
#         "link_pattern": "pinkvilla.com",
#         "target": 2,
#         "fetch_limit": 25
#     },
# ]


# # ============================================================
# # Filters
# # ============================================================
# def is_film_related(title: str, content: str) -> bool:
#     combined = (title + ' ' + content[:1500]).lower()
#     return any(kw.lower() in combined for kw in FILM_KEYWORDS)


# # ============================================================
# # Google Sheets Setup — with retry
# # ============================================================
# def setup_google_sheets(max_retries: int = 5, retry_delay: int = 10):
#     for attempt in range(1, max_retries + 1):
#         try:
#             scope = [
#                 'https://spreadsheets.google.com/feeds',
#                 'https://www.googleapis.com/auth/drive'
#             ]
#             creds = Credentials.from_service_account_file(
#                 GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scope
#             )
#             client = gspread.authorize(creds)

#             try:
#                 sheet = client.open(GOOGLE_SHEET_NAME)
#                 print(f"✅ Connected: '{GOOGLE_SHEET_NAME}'")
#             except gspread.SpreadsheetNotFound:
#                 sheet = client.create(GOOGLE_SHEET_NAME)
#                 print(f"✅ Created: '{GOOGLE_SHEET_NAME}'")

#             try:
#                 worksheet = sheet.worksheet(GOOGLE_WORKSHEET_NAME)
#                 print(f"✅ Worksheet: '{GOOGLE_WORKSHEET_NAME}'")
#                 current_rows = worksheet.row_count
#                 if current_rows < 2000:
#                     worksheet.add_rows(5000 - current_rows)
#                     print(f"✅ Expanded sheet: {current_rows} → 5000 rows")
#                 else:
#                     print(f"✅ Sheet has {current_rows} rows — OK")

#             except gspread.WorksheetNotFound:
#                 worksheet = sheet.add_worksheet(
#                     title=GOOGLE_WORKSHEET_NAME, rows=5000, cols=10
#                 )
#                 worksheet.update('A1:F1', [[
#                     'Timestamp (IST)', 'Type', 'Category', 'Title', 'Script', 'Source Link'
#                 ]])
#                 worksheet.format('A1:F1', {
#                     'textFormat': {
#                         'bold': True,
#                         'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
#                     },
#                     'backgroundColor': {'red': 0.5, 'green': 0.2, 'blue': 0.8},
#                     'horizontalAlignment': 'CENTER'
#                 })
#                 worksheet.set_column_width('A', 180)
#                 worksheet.set_column_width('B', 100)
#                 worksheet.set_column_width('C', 150)
#                 worksheet.set_column_width('D', 400)
#                 worksheet.set_column_width('E', 600)
#                 worksheet.set_column_width('F', 400)
#                 print(f"✅ Created worksheet with headers")

#             return worksheet

#         except gspread.exceptions.APIError as e:
#             error_str = str(e)
#             if any(code in error_str for code in ['503', '500', '429', '502']):
#                 if attempt < max_retries:
#                     print(f"⚠️ Google Sheets error (attempt {attempt}/{max_retries}) — retrying in {retry_delay}s...")
#                     time.sleep(retry_delay)
#                     continue
#                 else:
#                     print(f"❌ Google Sheets unavailable after {max_retries} attempts: {e}")
#                     return None
#             else:
#                 print(f"❌ Sheets API error: {e}")
#                 return None
#         except FileNotFoundError:
#             print(f"❌ credentials.json not found!")
#             return None
#         except Exception as e:
#             print(f"❌ Sheets setup error: {e}")
#             import traceback
#             traceback.print_exc()
#             return None


# # ============================================================
# # Save to Google Sheets — with retry + Type column
# # ============================================================
# def save_to_google_sheets(worksheet, script_type, category, title, script, source_link, max_retries: int = 3):
#     """
#     script_type: "NEWS" or "STORY"
#     """
#     for attempt in range(1, max_retries + 1):
#         try:
#             timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')

#             script = '\n'.join(str(i) for i in script) if isinstance(script, list) else str(script).strip()
#             script = script.replace('[', '').replace(']', '')
#             title = str(title).strip()
#             source_link = str(source_link).strip()
#             category = str(category).strip().lower()

#             if category not in VALID_CATEGORIES:
#                 category = "general_entertainment"

#             next_row = len(worksheet.get_all_values()) + 1
#             worksheet.append_row(
#                 [timestamp, script_type, CATEGORY_DISPLAY.get(category, category.upper()), title, script, source_link],
#                 value_input_option='RAW'
#             )

#             worksheet.format(f'A{next_row}:F{next_row}', {
#                 'textFormat': {
#                     'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
#                     'fontSize': 10
#                 },
#                 'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
#                 'wrapStrategy': 'WRAP',
#                 'verticalAlignment': 'TOP'
#             })

#             # Type column color
#             type_colors = {
#                 "NEWS":  {'red': 0.85, 'green': 0.93, 'blue': 1.0},
#                 "STORY": {'red': 1.0,  'green': 0.88, 'blue': 0.6},
#             }
#             worksheet.format(f'B{next_row}', {
#                 'textFormat': {'bold': True, 'fontSize': 10},
#                 'backgroundColor': type_colors.get(script_type, {'red': 1.0, 'green': 1.0, 'blue': 1.0}),
#                 'horizontalAlignment': 'CENTER'
#             })

#             category_colors = {
#                 'bollywood':             {'red': 1.0,  'green': 0.85, 'blue': 0.7},
#                 'marathi_cinema':        {'red': 0.8,  'green': 1.0,  'blue': 0.8},
#                 'south_cinema':          {'red': 0.7,  'green': 0.9,  'blue': 1.0},
#                 'ott':                   {'red': 0.9,  'green': 0.8,  'blue': 1.0},
#                 'awards':                {'red': 1.0,  'green': 0.95, 'blue': 0.6},
#                 'celebrity':             {'red': 1.0,  'green': 0.8,  'blue': 0.9},
#                 'general_entertainment': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
#                 'film_story':            {'red': 1.0,  'green': 0.85, 'blue': 0.5},
#             }
#             worksheet.format(f'C{next_row}', {
#                 'textFormat': {'bold': True, 'fontSize': 10},
#                 'backgroundColor': category_colors.get(category, category_colors['general_entertainment']),
#                 'horizontalAlignment': 'CENTER'
#             })

#             print(f"✅ [{script_type}][{CATEGORY_DISPLAY.get(category, category.upper())}] {title[:50]}...")
#             return True

#         except gspread.exceptions.APIError as e:
#             error_str = str(e)
#             if any(code in error_str for code in ['503', '500', '429', '502']):
#                 if attempt < max_retries:
#                     print(f"   ⚠️ Sheets error on save (attempt {attempt}/{max_retries}) — retrying in 8s...")
#                     time.sleep(8)
#                     continue
#                 else:
#                     print(f"   ❌ Save failed after {max_retries} retries: {e}")
#                     return False
#             print(f"❌ Save error: {e}")
#             return False
#         except Exception as e:
#             print(f"❌ Save error: {e}")
#             return False


# # ============================================================
# # Helpers
# # ============================================================
# def get_content_hash(title: str, content: str) -> str:
#     return hashlib.md5(
#         f"{title.lower()}{content[:200].lower()}".encode()
#     ).hexdigest()


# def sort_by_count(item):
#     return -item[1]


# def sort_by_priority(item):
#     return {'high': 1, 'medium': 2, 'low': 3}.get(item.get('importance', 'medium'), 2)


# def safe_truncate(text: str, max_chars: int) -> str:
#     if len(text) <= max_chars:
#         return text
#     truncated = text[:max_chars]
#     for punct in ['।', '.', '!', '?', '\n']:
#         last_pos = truncated.rfind(punct)
#         if last_pos > max_chars * 0.7:
#             return truncated[:last_pos + 1]
#     last_space = truncated.rfind(' ')
#     if last_space > max_chars * 0.7:
#         return truncated[:last_space]
#     return truncated


# # ============================================================
# # Safe API Response Extractor
# # ============================================================
# def extract_response_content(response) -> str:
#     raw_choice = response.choices[0]

#     if hasattr(raw_choice, 'message'):
#         msg = raw_choice.message
#         if hasattr(msg, 'content') and isinstance(msg.content, str):
#             return msg.content
#         elif hasattr(msg, 'content') and isinstance(msg.content, list):
#             return ' '.join(
#                 block.get('text', '') if isinstance(block, dict) else str(block)
#                 for block in msg.content
#             )
#         elif isinstance(msg, list):
#             return ' '.join(
#                 block.get('text', '') if isinstance(block, dict) else str(block)
#                 for block in msg
#             )
#         else:
#             return str(msg)
#     elif isinstance(raw_choice, dict):
#         msg = raw_choice.get('message', {})
#         return msg.get('content', '') if isinstance(msg, dict) else str(msg)
#     else:
#         return str(raw_choice)


# # ============================================================
# # Script Completion Check & Callback
# # ============================================================
# def is_script_complete(script: str) -> bool:
#     return script.strip().endswith(SCRIPT_CTA.strip())


# def get_last_line(script: str) -> str:
#     lines = [l.strip() for l in script.strip().split('\n') if l.strip()]
#     return lines[-1] if lines else ""


# async def complete_script_if_needed(script: str, article: Dict) -> str:
#     global total_input_tokens, total_output_tokens, total_cost

#     if is_script_complete(script):
#         return script

#     last_line = get_last_line(script)
#     print(f"   🔧 Script incomplete — last: '{last_line[:60]}' — completing...")

#     try:
#         response = perplexity_client.chat.completions.create(
#             model=SCRIPT_MODEL,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": f'फक्त मराठी lines लिहा. शेवटची line नक्की हीच: "{SCRIPT_CTA}"'
#                 },
#                 {
#                     "role": "user",
#                     "content": f"""खालील अर्धवट मराठी script पूर्ण करा.

# अर्धवट script:
# {script}

# नियम:
# - वरील script च्या पुढे फक्त उर्वरित lines लिहा
# - नवीन lines जोडा जेणेकरून एकूण 15-18 lines होतील
# - शेवटची line नक्की हीच: "{SCRIPT_CTA}"
# - फक्त नवीन lines लिहा, जुन्या lines परत लिहू नका
# - फक्त मराठीत लिहा"""
#                 }
#             ],
#             temperature=0.7,
#             max_tokens=600
#         )

#         if hasattr(response, 'usage'):
#             i_t = response.usage.prompt_tokens
#             o_t = response.usage.completion_tokens
#             total_input_tokens  += i_t
#             total_output_tokens += o_t
#             total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

#         completion = extract_response_content(response).strip()
#         completion = re.sub(r'<think>.*?</think>', '', completion, flags=re.DOTALL).strip()
#         completion = completion.replace('```', '').strip()

#         if any(kw.lower() in completion.lower() for kw in REFUSAL_KEYWORDS):
#             return script.strip() + f"\n\n{SCRIPT_CTA}"

#         completed = script.strip() + "\n\n" + completion.strip()
#         if not is_script_complete(completed):
#             completed = completed.strip() + f"\n\n{SCRIPT_CTA}"

#         print(f"   ✅ Script completed")
#         return completed

#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
#             raise CreditExhaustedException(str(e))
#         return script.strip() + f"\n\n{SCRIPT_CTA}"


# # ============================================================
# # Marathi Validator
# # ============================================================
# def is_valid_marathi_script(script: str) -> bool:
#     if len(script) < 100:
#         return False
#     if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
#         return False
#     devanagari = len(re.findall(r'[\u0900-\u097F]', script))
#     total      = len(script.replace(' ', '').replace('\n', ''))
#     return (devanagari / max(total, 1)) > 0.35


# # ============================================================
# # API Credit Check
# # ============================================================
# async def check_api_credits():
#     try:
#         perplexity_client.chat.completions.create(
#             model=ANALYSIS_MODEL,
#             messages=[{"role": "user", "content": "ok"}],
#             max_tokens=1
#         )
#         print("✅ API credits OK")
#         return True
#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in [
#             '402', '429', '401', 'insufficient', 'credit',
#             'quota', 'balance', 'payment', 'billing', 'rate limit', 'exceeded'
#         ]):
#             print("=" * 60)
#             print("❌ PERPLEXITY API CREDITS EXHAUSTED!")
#             print(f"   Error: {str(e)}")
#             print("=" * 60)
#             print("👉 Top up: https://www.perplexity.ai/settings/api")
#             return False
#         print(f"❌ Unknown API error: {e}")
#         return False


# # ============================================================
# # Fetch Article with Retry
# # ============================================================
# async def fetch_article_with_retry(crawler, url: str, retries: int = 3) -> str:
#     for attempt in range(1, retries + 1):
#         try:
#             result = await crawler.arun(
#                 url,
#                 config=CrawlerRunConfig(
#                     cache_mode=CacheMode.BYPASS,
#                     word_count_threshold=10,
#                     page_timeout=25000
#                 )
#             )
#             if result.success and len(result.markdown) > 50:
#                 return result.markdown
#             await asyncio.sleep(2)
#         except Exception:
#             await asyncio.sleep(2)
#     return ""


# # ============================================================
# # STEP A — Scrape Story Blogs from indianfilmhistory.com
# # Returns list of story dicts with category="film_story"
# # ============================================================
# async def scrape_story_sources() -> List[Dict]:
#     story_articles = []
#     seen_links = set()

#     async with AsyncWebCrawler(verbose=False) as crawler:
#         for site in STORY_SITES:
#             if len(story_articles) >= TARGET_STORY_SCRIPTS:
#                 break

#             print(f"\n{'='*60}")
#             print(f"📖 {site['name']} | Target: {site['target']}")
#             print(f"{'='*60}")

#             try:
#                 result = await crawler.arun(
#                     site['url'],
#                     config=CrawlerRunConfig(
#                         cache_mode=CacheMode.BYPASS,
#                         wait_for="body",
#                         word_count_threshold=10,
#                         page_timeout=30000,
#                         js_code="await new Promise(r => setTimeout(r, 3000));"
#                     )
#                 )

#                 if not result.success:
#                     print(f"❌ Failed to load: {site['url']}")
#                     continue

#                 soup = BeautifulSoup(result.html, 'html.parser')
#                 raw_links = []

#                 for link_tag in soup.find_all('a', href=True):
#                     href  = link_tag.get('href', '')
#                     title = link_tag.get_text(strip=True)

#                     if len(title) < 10 or len(title) > 300:
#                         continue
#                     if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
#                         continue

#                     if href.startswith('/'):
#                         base = site['url'].split('/') + '//' + site['url'].split('/')[2]
#                         href = base + href

#                     if href.startswith('http') and href not in seen_links:
#                         raw_links.append({'title': title, 'link': href})

#                 print(f"📋 Found {len(raw_links)} candidate links")

#                 for item in raw_links:
#                     if len(story_articles) >= TARGET_STORY_SCRIPTS:
#                         break
#                     if item['link'] in seen_links:
#                         continue

#                     print(f"   📖 Fetching: {item['title'][:55]}...")
#                     markdown = await fetch_article_with_retry(crawler, item['link'])
#                     if not markdown or len(markdown) < 100:
#                         print(f"   ⚠️ Too short — skipped")
#                         continue

#                     seen_links.add(item['link'])
#                     content_hash = get_content_hash(item['title'], markdown)

#                     if content_hash in processed_hashes:
#                         print(f"   🔄 Duplicate skipped")
#                         continue

#                     processed_hashes.add(content_hash)
#                     story_articles.append({
#                         'title':            item['title'],
#                         'link':             item['link'],
#                         'content':          safe_truncate(markdown, 4000),
#                         'hash':             content_hash,
#                         'category':         'film_story',
#                         'source':           site['name'],
#                         'scraped_at':       datetime.now(IST).isoformat(),
#                         'script_type':      'STORY',
#                         'detailed_summary': safe_truncate(markdown, 600),
#                         'key_points':       [item['title']],
#                         'importance':       'high',
#                     })
#                     print(f"   ✅ Story collected [{len(story_articles)}/{TARGET_STORY_SCRIPTS}]: {item['title'][:50]}")
#                     await asyncio.sleep(1)

#             except CreditExhaustedException:
#                 raise
#             except Exception as e:
#                 print(f"❌ Error scraping {site['name']}: {e}")

#             await asyncio.sleep(2)

#     # ── Fallback: if site scraping got nothing, use Perplexity to find stories ──
#     if len(story_articles) < TARGET_STORY_SCRIPTS:
#         needed = TARGET_STORY_SCRIPTS - len(story_articles)
#         print(f"\n📖 Only {len(story_articles)} stories scraped — fetching {needed} via Perplexity...")
#         extra = await fetch_film_stories_via_perplexity(needed)
#         story_articles.extend(extra)

#     return story_articles[:TARGET_STORY_SCRIPTS]


# # ============================================================
# # STEP B — Scrape News from Entertainment Sites (8 articles)
# # ============================================================
# async def scrape_film_sources() -> List[Dict]:
#     all_news = []

#     async with AsyncWebCrawler(verbose=False) as crawler:
#         for site in NEWS_SITES:
#             print(f"\n{'='*60}")
#             print(f"🎬 {site['name']} | Target: {site['target']}")
#             print(f"{'='*60}")

#             site_articles = []

#             try:
#                 result = await crawler.arun(
#                     site['url'],
#                     config=CrawlerRunConfig(
#                         cache_mode=CacheMode.BYPASS,
#                         wait_for="body",
#                         word_count_threshold=10,
#                         page_timeout=30000,
#                         js_code="await new Promise(r => setTimeout(r, 3000));"
#                     )
#                 )

#                 if not result.success:
#                     print(f"❌ Failed: {site['name']}")
#                     continue

#                 soup = BeautifulSoup(result.html, 'html.parser')
#                 raw_articles = []

#                 for link_tag in soup.find_all('a', href=True):
#                     href  = link_tag.get('href', '')
#                     title = link_tag.get_text(strip=True)

#                     if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
#                         continue

#                     if (15 < len(title) < 300 and
#                         site['link_pattern'] in href and
#                         not any(x in href.lower() for x in SKIP_URL_PATTERNS)):

#                         if href.startswith('/'):
#                             base = site['url'].split('/') + '//' + site['url'].split('/')[2]
#                             href = base + href

#                         if href.startswith('http'):
#                             raw_articles.append({'title': title, 'link': href})

#                 seen = set()
#                 unique_links = []
#                 for a in raw_articles:
#                     if a['link'] not in seen:
#                         unique_links.append(a)
#                         seen.add(a['link'])

#                 print(f"📋 Found {len(unique_links)} unique links")

#                 for article in unique_links:
#                     if len(site_articles) >= site['target']:
#                         break

#                     print(f"   🔗 [{len(site_articles)+1}/{site['target']}] {article['title'][:50]}...")

#                     markdown = await fetch_article_with_retry(crawler, article['link'])
#                     content  = markdown if markdown else article['title']

#                     if not is_film_related(article['title'], content):
#                         print(f"   🚫 Not film-related — skipped")
#                         continue

#                     if any(kw.lower() in content.lower() for kw in SKIP_CONTENT_KEYWORDS):
#                         print(f"   ⏭️  Skipped (utility/spiritual content)")
#                         continue

#                     content_hash = get_content_hash(article['title'], content)

#                     if content_hash not in processed_hashes:
#                         site_articles.append({
#                             'title':            article['title'],
#                             'link':             article['link'],
#                             'content':          safe_truncate(content, 3500),
#                             'hash':             content_hash,
#                             'has_full_content': bool(markdown),
#                             'script_type':      'NEWS',
#                         })
#                         processed_hashes.add(content_hash)
#                         tag = "✅" if markdown else "⚠️ fallback"
#                         print(f"   {tag} [{len(site_articles)}/{site['target']}] {article['title'][:50]}...")
#                     else:
#                         print(f"   🔄 Duplicate skipped")

#                     await asyncio.sleep(1)

#                 print(f"\n📦 {site['name']}: {len(site_articles)}/{site['target']} collected")

#                 if site_articles:
#                     filtered = await smart_analyze_with_category(site_articles, site['name'])
#                     all_news.extend(filtered)
#                     print(f"🧠 {site['name']}: Analyzed {len(filtered)} articles")

#             except CreditExhaustedException:
#                 raise
#             except Exception as e:
#                 print(f"❌ Error {site['name']}: {e}")

#             await asyncio.sleep(3)

#     return all_news


# # ============================================================
# # AI Categorization — Film-specific
# # ============================================================
# async def smart_analyze_with_category(articles: List[Dict], source_name: str):
#     global total_input_tokens, total_output_tokens, total_cost

#     all_filtered = []

#     for batch_start in range(0, len(articles), 5):
#         raw_batch = articles[batch_start:batch_start + 5]

#         batch = [
#             article for article in raw_batch
#             if not any(kw.lower() in article.get('content', '').lower()
#                        for kw in SKIP_CONTENT_KEYWORDS)
#         ]

#         if not batch:
#             continue

#         index_to_link  = {i: article['link']        for i, article in enumerate(batch)}
#         index_to_title = {i: article['title']       for i, article in enumerate(batch)}
#         index_to_type  = {i: article.get('script_type', 'NEWS') for i, article in enumerate(batch)}

#         articles_text = ""
#         for idx, article in enumerate(batch):
#             articles_text += f"INDEX_{idx}: {article['title']}\n{safe_truncate(article['content'], 500)}\n---\n"

#         prompt = f"""भारतीय चित्रपट उद्योग बातम्या विश्लेषक: खालील बातम्यांना category आणि Marathi summary द्या.

# ⚠️ नियम:
# 1. detailed_summary आणि key_points फक्त मराठीत लिहा
# 2. JSON मध्ये "index" field EXACTLY जसा दिला (0,1,2,3,4) तसाच परत द्या

# Categories:
# - bollywood, marathi_cinema, south_cinema, ott, awards, celebrity, general_entertainment

# JSON array format:
# [{{"index": 0, "category": "bollywood", "detailed_summary": "मराठी सारांश १५०-२०० शब्द", "importance": "high/medium/low", "key_points": ["मुद्दा १", "मुद्दा २", "मुद्दा ३"]}}]

# बातम्या:
# {articles_text}

# फक्त JSON array. Index 0 ते {len(batch)-1} पर्यंत."""

#         try:
#             response = perplexity_client.chat.completions.create(
#                 model=ANALYSIS_MODEL,
#                 messages=[
#                     {"role": "system", "content": "Return ONLY valid JSON array."},
#                     {"role": "user", "content": prompt}
#                 ],
#                 temperature=0.2,
#                 max_tokens=3000
#             )

#             if hasattr(response, 'usage'):
#                 i_t = response.usage.prompt_tokens
#                 o_t = response.usage.completion_tokens
#                 total_input_tokens  += i_t
#                 total_output_tokens += o_t
#                 c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
#                 total_cost += c
#                 print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

#             content = extract_response_content(response)
#             if not content.strip():
#                 raise ValueError("Empty content")

#             content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
#             match = re.search(r'\[.*\]', content, re.DOTALL)

#             if match:
#                 parsed = json.loads(match.group())
#                 for art in parsed:
#                     idx = art.get('index')
#                     if idx is not None and idx in index_to_link:
#                         art['link']        = index_to_link[idx]
#                         art['title']       = index_to_title[idx]
#                         art['script_type'] = index_to_type[idx]
#                     else:
#                         pos = len(all_filtered) % len(batch)
#                         art['link']        = index_to_link.get(pos, '')
#                         art['title']       = index_to_title.get(pos, art.get('title', ''))
#                         art['script_type'] = index_to_type.get(pos, 'NEWS')

#                     if art.get('category') not in VALID_CATEGORIES:
#                         art['category'] = 'general_entertainment'

#                 all_filtered.extend(parsed)
#                 print(f"   ✅ Categorized {len(parsed)}")
#             else:
#                 for i, article in enumerate(batch):
#                     all_filtered.append({
#                         'index':            i,
#                         'title':            article['title'],
#                         'category':         'general_entertainment',
#                         'detailed_summary': safe_truncate(article['content'], 600),
#                         'importance':       'medium',
#                         'link':             article['link'],
#                         'key_points':       [article['title']],
#                         'script_type':      article.get('script_type', 'NEWS'),
#                     })

#         except json.JSONDecodeError:
#             for i, article in enumerate(batch):
#                 all_filtered.append({
#                     'index':            i,
#                     'title':            article['title'],
#                     'category':         'general_entertainment',
#                     'detailed_summary': safe_truncate(article['content'], 600),
#                     'importance':       'medium',
#                     'link':             article['link'],
#                     'key_points':       [article['title']],
#                     'script_type':      article.get('script_type', 'NEWS'),
#                 })

#         except Exception as e:
#             error_str = str(e).lower()
#             if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
#                 raise CreditExhaustedException(str(e))
#             print(f"   ❌ AI error: {e}")
#             for i, article in enumerate(batch):
#                 all_filtered.append({
#                     'index':            i,
#                     'title':            article['title'],
#                     'category':         'general_entertainment',
#                     'detailed_summary': safe_truncate(article['content'], 600),
#                     'importance':       'medium',
#                     'link':             article['link'],
#                     'key_points':       [article['title']],
#                     'script_type':      article.get('script_type', 'NEWS'),
#                 })

#         await asyncio.sleep(1.5)

#     for art in all_filtered:
#         art['source']     = source_name
#         art['scraped_at'] = datetime.now(IST).isoformat()

#     return all_filtered


# # ============================================================
# # Perplexity Fallback — Film News
# # ============================================================
# async def fetch_film_news_via_perplexity(needed: int) -> List[Dict]:
#     global total_input_tokens, total_output_tokens, total_cost

#     print(f"\n🎬 Fetching {needed} more film news via Perplexity search...")

#     try:
#         response = perplexity_client.chat.completions.create(
#             model=ANALYSIS_MODEL,
#             messages=[
#                 {"role": "system", "content": "You are a film industry news researcher. Return ONLY valid JSON array."},
#                 {
#                     "role": "user",
#                     "content": f"""Find the latest {needed} news about Indian film industry (Bollywood, Marathi, South).

# Return JSON array with {needed} items:
# [{{
#   "title": "headline",
#   "detailed_summary": "150-200 word Marathi summary",
#   "category": "bollywood/marathi_cinema/south_cinema/ott/awards/celebrity/general_entertainment",
#   "importance": "high/medium/low",
#   "key_points": ["point1", "point2", "point3"],
#   "link": "source url or empty string"
# }}]
# Only JSON. All summaries in Marathi."""
#                 }
#             ],
#             temperature=0.3,
#             max_tokens=4000
#         )

#         if hasattr(response, 'usage'):
#             i_t = response.usage.prompt_tokens
#             o_t = response.usage.completion_tokens
#             total_input_tokens  += i_t
#             total_output_tokens += o_t
#             c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
#             total_cost += c
#             print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

#         content = extract_response_content(response)
#         content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
#         match = re.search(r'\[.*\]', content, re.DOTALL)

#         if match:
#             parsed = json.loads(match.group())
#             results = []
#             for art in parsed:
#                 if art.get('category') not in VALID_CATEGORIES:
#                     art['category'] = 'general_entertainment'
#                 art['source']      = 'Perplexity Search'
#                 art['scraped_at']  = datetime.now(IST).isoformat()
#                 art['script_type'] = 'NEWS'
#                 art['hash']        = get_content_hash(art.get('title', ''), art.get('detailed_summary', ''))
#                 results.append(art)
#             print(f"   ✅ Got {len(results)} news from Perplexity")
#             return results
#         return []

#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
#             raise CreditExhaustedException(str(e))
#         print(f"   ❌ Perplexity news search error: {e}")
#         return []


# # ============================================================
# # Perplexity Fallback — Film Stories/Anecdotes
# # ============================================================
# async def fetch_film_stories_via_perplexity(needed: int) -> List[Dict]:
#     global total_input_tokens, total_output_tokens, total_cost

#     print(f"\n📖 Fetching {needed} film history stories via Perplexity...")

#     try:
#         response = perplexity_client.chat.completions.create(
#             model=ANALYSIS_MODEL,
#             messages=[
#                 {"role": "system", "content": "You are an Indian film history researcher. Return ONLY valid JSON array."},
#                 {
#                     "role": "user",
#                     "content": f"""Find {needed} interesting untold stories, behind-the-scenes anecdotes, or iconic moments from Indian film history (Bollywood, Marathi, South cinema).

# These should be story-style content — interesting facts, friendships, struggles, iconic scenes, behind-the-camera moments — NOT current news.

# Return JSON array with {needed} items:
# [{{
#   "title": "Story title (e.g. 'Sholay च्या Jai-Viru जोडीचा खरा किस्सा')",
#   "detailed_summary": "200-250 word detailed Marathi summary of the story/anecdote",
#   "category": "film_story",
#   "importance": "high",
#   "key_points": ["मुद्दा १", "मुद्दा २", "मुद्दा ३"],
#   "link": "source url or empty string",
#   "persons_involved": "actor/director names",
#   "film_name": "film name if applicable"
# }}]
# Only JSON. All summaries in Marathi."""
#                 }
#             ],
#             temperature=0.4,
#             max_tokens=3000
#         )

#         if hasattr(response, 'usage'):
#             i_t = response.usage.prompt_tokens
#             o_t = response.usage.completion_tokens
#             total_input_tokens  += i_t
#             total_output_tokens += o_t
#             c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
#             total_cost += c
#             print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

#         content = extract_response_content(response)
#         content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
#         match = re.search(r'\[.*\]', content, re.DOTALL)

#         if match:
#             parsed = json.loads(match.group())
#             results = []
#             for art in parsed:
#                 art['category']    = 'film_story'
#                 art['source']      = 'Perplexity Film History'
#                 art['scraped_at']  = datetime.now(IST).isoformat()
#                 art['script_type'] = 'STORY'
#                 art['hash']        = get_content_hash(art.get('title', ''), art.get('detailed_summary', ''))
#                 results.append(art)
#             print(f"   ✅ Got {len(results)} stories from Perplexity")
#             return results
#         return []

#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
#             raise CreditExhaustedException(str(e))
#         print(f"   ❌ Perplexity story search error: {e}")
#         return []


# # ============================================================
# # Script Generation — NEWS type (filmy gossip tone)
# # ============================================================
# async def create_news_script(article: Dict) -> str:
#     global total_input_tokens, total_output_tokens, total_cost

#     category         = article.get('category', 'general_entertainment')
#     category_display = CATEGORY_DISPLAY.get(category, 'ENTERTAINMENT')

#     system_prompt = f"""तुम्ही एक मराठी Instagram Reel script writer आहात जे भारतीय चित्रपट उद्योगाबद्दल content बनवतात.
# फक्त मराठी भाषेत लिहा. AI आहात हे सांगू नका. फक्त script लिहा.

# Tone: उत्साही, मनोरंजक, filmy gossip style — insider feel.

# Structure (15-18 lines):
# - Line 1-2: 🎬 धक्कादायक filmy hook
# - Line 3-10: सर्व facts (नाव, चित्रपट, तारीख, box office, OTT)
# - Line 11-14: प्रश्न / विश्लेषण / ट्विस्ट
# - Line 15-18: CTA

# कठोर नियम:
# - फक्त मराठीत (film names, actor names सोडून)
# - 15-18 lines, heading/markdown नाही
# - शेवटची line नक्की: "{SCRIPT_CTA}"
# - script अर्धवट सोडू नका"""

#     summary    = safe_truncate(article.get('detailed_summary', article.get('title', '')), 600)
#     key_points = ', '.join(article.get('key_points', [article.get('title', '')]))

#     prompts = [
#         f"""Category: {category_display}
# शीर्षक: {article['title']}
# सारांश: {summary}
# मुद्दे: {key_points}

# 15-18 मराठी lines तयार करा. Tone: filmy gossip.
# शेवटची line: "{SCRIPT_CTA}" """,

#         f"""खालील चित्रपट बातमीवर 15 मराठी वाक्ये लिहा.
# बातमी: {article['title']}. {safe_truncate(summary, 200)}
# - filmy tone, actor/film नाव mention करा
# - शेवटची line: "{SCRIPT_CTA}" """
#     ]

#     for attempt in range(1, 3):
#         try:
#             response = perplexity_client.chat.completions.create(
#                 model=SCRIPT_MODEL,
#                 messages=[
#                     {"role": "system", "content": system_prompt},
#                     {"role": "user",   "content": prompts[attempt - 1]}
#                 ],
#                 temperature=0.85,
#                 max_tokens=2000
#             )

#             if hasattr(response, 'usage'):
#                 i_t = response.usage.prompt_tokens
#                 o_t = response.usage.completion_tokens
#                 total_input_tokens  += i_t
#                 total_output_tokens += o_t
#                 total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

#             script = extract_response_content(response).strip()
#             script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
#             script = script.replace('```', '').strip()

#             if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
#                 continue

#             if is_valid_marathi_script(script):
#                 return await complete_script_if_needed(script, article)

#         except CreditExhaustedException:
#             raise
#         except Exception as e:
#             error_str = str(e).lower()
#             if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
#                 raise CreditExhaustedException(str(e))
#             await asyncio.sleep(2)

#     # Fallback
#     title   = article.get('title', 'एक धक्कादायक चित्रपट बातमी')[:80]
#     summary = safe_truncate(article.get('detailed_summary', article.get('title', '')), 200)
#     return f"""थांबा! ही बातमी ऐकली का?

# {title}

# {summary}

# भारतीय चित्रपट उद्योगात ही बातमी सध्या जोरदार चर्चेत आहे.

# चाहत्यांमध्ये एकच खळबळ उडवली आहे.

# सोशल मीडियावर हे प्रकरण ट्रेंडिंगवर आहे.

# तुम्हाला काय वाटतं?

# {SCRIPT_CTA}"""


# # ============================================================
# # Script Generation — STORY type (Jai-Viru / Zeenat style)
# # ============================================================
# async def create_story_script(article: Dict) -> str:
#     global total_input_tokens, total_output_tokens, total_cost

#     content  = article.get('content', article.get('detailed_summary', ''))
#     summary  = safe_truncate(content, 800)
#     title    = article.get('title', '')
#     persons  = article.get('persons_involved', '')
#     film     = article.get('film_name', '')

#     system_prompt = f"""तुम्ही एक मराठी Instagram Reel script writer आहात जे भारतीय चित्रपट इतिहासातील interesting किस्से, anecdotes आणि behind-the-scenes stories सांगतात.

# Tone: conversational, warm, storytelling — जणू एखाद्या मित्राला गोष्ट सांगत आहात.
# Feel: "हे माहीत आहे का?", "विश्वास बसणार नाही!", "ते आठवतं का?"

# Reference style examples:
# ---
# Example 1 (Friendship story):
# {STORY_SCRIPT_EXAMPLE_1}
# ---
# Example 2 (Actress anecdote):
# {STORY_SCRIPT_EXAMPLE_2}
# ---

# Structure (12-16 lines):
# - Line 1: Hook — एखादा प्रश्न किंवा nostalgic reference
# - Line 2-10: Story facts — specific, vivid, engaging
# - Line 11-13: Emotional twist / lesson / question for audience
# - Line 14-16: CTA — share/tag/comment

# कठोर नियम:
# - फक्त मराठीत (film/actor names English ठेवू शकता)
# - No headings, no markdown, no bullet points
# - Conversational tone — जणू voice-over
# - शेवटची line नक्की हीच: "{SCRIPT_CTA}"
# - script अर्धवट सोडू नका"""

#     user_prompt = f"""खालील भारतीय चित्रपट इतिहासातील story/anecdote वर Instagram Reel script लिहा.

# Title: {title}
# Persons: {persons if persons else 'as mentioned in content'}
# Film: {film if film else 'as mentioned in content'}
# Story content:
# {summary}

# नियम:
# - Reference examples सारखाच conversational, engaging tone
# - Specific facts, names, moments वापरा
# - Emotional connection निर्माण करा
# - शेवटी audience ला tag/share/comment करायला सांगा
# - शेवटची line: "{SCRIPT_CTA}" """

#     for attempt in range(1, 3):
#         try:
#             response = perplexity_client.chat.completions.create(
#                 model=SCRIPT_MODEL,
#                 messages=[
#                     {"role": "system", "content": system_prompt},
#                     {"role": "user",   "content": user_prompt}
#                 ],
#                 temperature=0.9,
#                 max_tokens=2000
#             )

#             if hasattr(response, 'usage'):
#                 i_t = response.usage.prompt_tokens
#                 o_t = response.usage.completion_tokens
#                 total_input_tokens  += i_t
#                 total_output_tokens += o_t
#                 total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

#             script = extract_response_content(response).strip()
#             script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
#             script = script.replace('```', '').strip()

#             if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
#                 print(f"   ⚠️ Story attempt {attempt}: Refusal — retrying...")
#                 continue

#             if is_valid_marathi_script(script):
#                 return await complete_script_if_needed(script, article)

#             print(f"   ⚠️ Story attempt {attempt}: Not valid Marathi — retrying...")

#         except CreditExhaustedException:
#             raise
#         except Exception as e:
#             error_str = str(e).lower()
#             if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
#                 raise CreditExhaustedException(str(e))
#             await asyncio.sleep(2)

#     # Fallback story script
#     return f"""{title}

# {safe_truncate(summary, 300)}

# हा किस्सा भारतीय सिनेमाच्या इतिहासात खूप खास आहे.

# या कलाकाराने जे केलं, ते आजही लोकांच्या हृदयात घर करून आहे.

# तुम्हाला हा किस्सा माहीत होता का?

# तुमच्या मित्रांनाही सांगा हा अविश्वसनीय किस्सा!

# {SCRIPT_CTA}"""


# # ============================================================
# # Main Pipeline
# # ============================================================
# async def main():
#     global total_input_tokens, total_output_tokens, total_cost

#     print("=" * 70)
#     print("🎬 KALAKAR KATTA - FILM NEWS + STORY SCRAPER v2.0")
#     print(f"🔍 Analysis  : {ANALYSIS_MODEL}")
#     print(f"✍️  Scripts   : {SCRIPT_MODEL}")
#     print(f"📰 News      : {TARGET_NEWS_SCRIPTS} scripts")
#     print(f"📖 Stories   : {TARGET_STORY_SCRIPTS} scripts (indianfilmhistory.com)")
#     print(f"🎯 Total     : {TARGET_SCRIPTS} scripts")
#     print(f"🎭 Covers    : Bollywood | Marathi | South | OTT | Awards | Film History")
#     print(f"🕐 Timezone  : IST (Asia/Kolkata)")
#     print("=" * 70)

#     credits_ok = await check_api_credits()
#     if not credits_ok:
#         print("\n🛑 Stopping. Top up credits first.")
#         print("👉 https://www.perplexity.ai/settings/api")
#         return

#     start_time = datetime.now(IST)

#     # ── STEP 1: Scrape Stories ──────────────────────────────
#     print("\n" + "=" * 70)
#     print("STEP 1: SCRAPING FILM HISTORY STORIES")
#     print("=" * 70 + "\n")

#     try:
#         story_articles = await scrape_story_sources()
#     except CreditExhaustedException:
#         print("\n🛑 Credits exhausted during story scraping. Stopping.")
#         return

#     print(f"\n✅ Stories collected: {len(story_articles)}/{TARGET_STORY_SCRIPTS}")

#     # ── STEP 2: Scrape News ─────────────────────────────────
#     print("\n" + "=" * 70)
#     print("STEP 2: SCRAPING ENTERTAINMENT NEWS")
#     print("=" * 70 + "\n")

#     try:
#         news_articles_raw = await scrape_film_sources()
#     except CreditExhaustedException:
#         print("\n🛑 Credits exhausted during news scraping. Stopping.")
#         return

#     # De-duplicate news
#     news_articles = []
#     seen_hashes   = set(a.get('hash', '') for a in story_articles)
#     for article in news_articles_raw:
#         h = article.get('hash', get_content_hash(
#             article['title'], article.get('detailed_summary', '')
#         ))
#         if h not in seen_hashes:
#             news_articles.append(article)
#             seen_hashes.add(h)

#     print(f"\n✅ News articles scraped: {len(news_articles)}")

#     # ── Fallback: top up news if short ──
#     if len(news_articles) < TARGET_NEWS_SCRIPTS:
#         needed = TARGET_NEWS_SCRIPTS - len(news_articles)
#         print(f"⚡ Fetching {needed} more via Perplexity...")
#         try:
#             extra = await fetch_film_news_via_perplexity(needed)
#             for art in extra:
#                 h = art.get('hash', '')
#                 if h not in seen_hashes:
#                     news_articles.append(art)
#                     seen_hashes.add(h)
#         except CreditExhaustedException:
#             print("🛑 Credits exhausted during fallback.")

#     news_articles.sort(key=sort_by_priority)
#     selected_news    = news_articles[:TARGET_NEWS_SCRIPTS]
#     selected_stories = story_articles[:TARGET_STORY_SCRIPTS]

#     # Category breakdown
#     all_selected = selected_news + selected_stories
#     category_counts = {}
#     for article in all_selected:
#         cat = article.get('category', 'general_entertainment')
#         category_counts[cat] = category_counts.get(cat, 0) + 1

#     print("\n📊 Category Breakdown:")
#     for cat, count in sorted(category_counts.items(), key=sort_by_count):
#         display = CATEGORY_DISPLAY.get(cat, cat.upper())
#         print(f"   {display:<22} {'█' * count} ({count})")

#     print(f"\n🎯 News selected    : {len(selected_news)}/{TARGET_NEWS_SCRIPTS}")
#     print(f"📖 Stories selected : {len(selected_stories)}/{TARGET_STORY_SCRIPTS}")
#     print(f"⏱️  Scraping         : {(datetime.now(IST)-start_time).total_seconds():.0f}s\n")

#     # ── STEP 3: Generate Scripts → Google Sheets ────────────
#     print("=" * 70)
#     print("STEP 3: GENERATING SCRIPTS → GOOGLE SHEETS")
#     print("=" * 70 + "\n")

#     worksheet        = setup_google_sheets()
#     successful_saves = 0
#     failed_saves     = 0

#     if not worksheet:
#         print("❌ Cannot connect to Google Sheets. Aborting.")
#         return

#     # -- Process NEWS scripts (8) --
#     print(f"\n{'─'*60}")
#     print(f"📰 Generating {len(selected_news)} NEWS scripts...")
#     print(f"{'─'*60}")

#     for idx, article in enumerate(selected_news, 1):
#         cat_display = CATEGORY_DISPLAY.get(article.get('category', ''), 'ENTERTAINMENT')
#         print(f"\n[NEWS {idx:02d}/{len(selected_news)}] "
#               f"{article.get('source','')[:14]} | "
#               f"{cat_display:<14} | "
#               f"{article['title'][:40]}...")

#         try:
#             script = await create_news_script(article)
#         except CreditExhaustedException:
#             print(f"\n🛑 Credits exhausted at NEWS script {idx}")
#             print(f"   ✅ Saved so far: {successful_saves}")
#             print(f"👉 Top up: https://www.perplexity.ai/settings/api")
#             break

#         dev_chars   = len(re.findall(r'[\u0900-\u097F]', script))
#         total_ch    = len(script.replace(' ', '').replace('\n', ''))
#         marathi_pct = (dev_chars / max(total_ch, 1)) * 100
#         lang_tag    = "🇮🇳" if marathi_pct > 35 else "⚠️"
#         cta_tag     = "✅" if is_script_complete(script) else "⚠️ NO CTA"
#         print(f"   📝 {lang_tag} ({marathi_pct:.0f}%) | CTA:{cta_tag}")

#         success = save_to_google_sheets(
#             worksheet, "NEWS",
#             article.get('category', 'general_entertainment'),
#             article['title'], script,
#             article.get('link', '')
#         )
#         if success:
#             successful_saves += 1
#         else:
#             failed_saves += 1

#         await asyncio.sleep(1)

#     # -- Process STORY scripts (2) --
#     print(f"\n{'─'*60}")
#     print(f"📖 Generating {len(selected_stories)} STORY scripts...")
#     print(f"{'─'*60}")

#     for idx, article in enumerate(selected_stories, 1):
#         print(f"\n[STORY {idx:02d}/{len(selected_stories)}] "
#               f"{article.get('source','')[:16]} | "
#               f"🎭 STORY | "
#               f"{article['title'][:45]}...")

#         try:
#             script = await create_story_script(article)
#         except CreditExhaustedException:
#             print(f"\n🛑 Credits exhausted at STORY script {idx}")
#             print(f"   ✅ Saved so far: {successful_saves}")
#             print(f"👉 Top up: https://www.perplexity.ai/settings/api")
#             break

#         dev_chars   = len(re.findall(r'[\u0900-\u097F]', script))
#         total_ch    = len(script.replace(' ', '').replace('\n', ''))
#         marathi_pct = (dev_chars / max(total_ch, 1)) * 100
#         lang_tag    = "🇮🇳" if marathi_pct > 35 else "⚠️"
#         cta_tag     = "✅" if is_script_complete(script) else "⚠️ NO CTA"
#         print(f"   📖 {lang_tag} ({marathi_pct:.0f}%) | CTA:{cta_tag}")

#         success = save_to_google_sheets(
#             worksheet, "STORY",
#             'film_story',
#             article['title'], script,
#             article.get('link', '')
#         )
#         if success:
#             successful_saves += 1
#         else:
#             failed_saves += 1

#         await asyncio.sleep(1)

#     # ── Final Summary ────────────────────────────────────────
#     total_duration = (datetime.now(IST) - start_time).total_seconds()
#     total_tokens   = total_input_tokens + total_output_tokens

#     print("\n" + "=" * 70)
#     print("📈 FINAL SUMMARY — KALAKAR KATTA v2.0")
#     print("=" * 70)
#     print(f"   🔍 Analysis model     : {ANALYSIS_MODEL}")
#     print(f"   ✍️  Script model       : {SCRIPT_MODEL}")
#     print(f"   📰 News scripts       : {len(selected_news)}")
#     print(f"   📖 Story scripts      : {len(selected_stories)}")
#     print(f"   ✅ Total saved        : {successful_saves}")
#     print(f"   ❌ Failed             : {failed_saves}")
#     print(f"   ⏱️  Total time         : {total_duration:.0f}s ({total_duration/60:.1f} mins)")
#     print(f"   📥 Input tokens       : {total_input_tokens:,}")
#     print(f"   📤 Output tokens      : {total_output_tokens:,}")
#     print(f"   🔢 Total tokens       : {total_tokens:,}")
#     print(f"   💰 Total cost         : ${total_cost:.4f} (~₹{total_cost*84:.2f})")
#     print(f"   💵 Cost per script    : ${total_cost/max(successful_saves,1):.4f}")
#     print(f"   🕐 Finished at (IST)  : {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
#     if worksheet:
#         print(f"   📊 Sheet URL          : https://docs.google.com/spreadsheets/d/"
#               f"{worksheet.spreadsheet.id}")
#     print("=" * 70 + "\n")


# if __name__ == "__main__":
#     asyncio.run(main())










# ============================================================
# KALAKAR KATTA — Entertainment Reel Script Generator v3.0
# Garbage-Free Edition (based on Jabari Khabri reference)
# ============================================================
import asyncio
import json
from datetime import datetime
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
from openai import OpenAI
import re
import gspread
from google.oauth2.service_account import Credentials
import os
from typing import List, Dict
import hashlib
import pytz
import time


class CreditExhaustedException(Exception):
    pass


# ============================================================
# Perplexity Client
# ============================================================
perplexity_client = OpenAI(
    api_key=os.environ.get("PERPLEXITY_API_KEY"),
    base_url="https://api.perplexity.ai"
)

# ✅ FIX 1: sonar-pro for BOTH models — no reasoning model
ANALYSIS_MODEL       = "sonar-pro"
SCRIPT_MODEL         = "sonar-pro"   # ← was sonar-reasoning-pro (root cause of garbage)

ANALYSIS_INPUT_COST  = 1.0 / 1_000_000
ANALYSIS_OUTPUT_COST = 1.0 / 1_000_000
SCRIPT_INPUT_COST    = 3.0 / 1_000_000
SCRIPT_OUTPUT_COST   = 15.0 / 1_000_000

IST = pytz.timezone('Asia/Kolkata')


# ============================================================
# Config
# ============================================================
GOOGLE_SHEETS_CREDENTIALS_FILE = "credentials.json"
GOOGLE_SHEET_NAME               = "Kalakar Katta"
GOOGLE_WORKSHEET_NAME           = "Sheet1"

TARGET_NEWS_SCRIPTS  = 8
TARGET_STORY_SCRIPTS = 2
TARGET_SCRIPTS       = TARGET_NEWS_SCRIPTS + TARGET_STORY_SCRIPTS  # = 10

SCRIPT_CTA = "तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."

VALID_CATEGORIES = [
    "bollywood", "marathi_cinema", "south_cinema",
    "ott", "awards", "celebrity", "general_entertainment",
    "film_story"
]

CATEGORY_DISPLAY = {
    "bollywood":             "BOLLYWOOD",
    "marathi_cinema":        "MARATHI",
    "south_cinema":          "SOUTH",
    "ott":                   "OTT",
    "awards":                "AWARDS",
    "celebrity":             "CELEBRITY",
    "general_entertainment": "ENTERTAINMENT",
    "film_story":            "STORY",
}

# ✅ FIX 2: Massively expanded REFUSAL_KEYWORDS
REFUSAL_KEYWORDS = [
    # English
    "I appreciate", "I should clarify", "I'm Perplexity",
    "search assistant", "I'm not able", "I cannot create",
    "Would you like", "clarify my role", "I'm an AI",
    "as an AI", "I don't create", "I cannot write",
    "I'm unable", "I must decline", "I need to clarify",
    "search results", "provided results", "based on the search",
    "I can't find", "no information", "not found in",
    "I don't have", "I am unable",
    # Hindi
    "मुझे खेद है", "मैं इस अनुरोध", "खोज परिणामों में",
    "प्रदान किए गए", "कृपया स्पष्ट करें", "मैं सही तरीके",
    "विशिष्ट तथ्य नहीं",
    # Marathi — original
    "मला खेद आहे", "मला क्षमस्व", "उत्तर देण्यासाठी आवश्यक",
    "शोध परिणामांमध्ये", "कृपया एक पूर्ण बातमी",
    "अधिक संबंधित शोध", "विशिष्ट घटना", "तपशील पुनः तपास",
    "मी Perplexity", "मी perplexity", "माझी भूमिका",
    "मूळ कार्याच्या विरुद्ध", "script लिहिण्याची विनंती",
    "सूचना देणे", "संशोधित उत्तरे", "मी एक AI",
    "script writer नाही", "मी तयार करू शकत नाही",
    "शोध निकालांमध्ये", "मेल होत नाही",
    "विस्तृत search results", "स्पष्ट करा",
    # Marathi — new (from observed garbage outputs)
    "माहिती नाही", "माहिती उपलब्ध नाही",
    "search results मध्ये", "सर्च रिझल्ट्स मध्ये",
    "प्रदान केलेल्या", "प्रदान केलेले",
    "मी या विनंतीचे", "मी या विषयावर",
    "मी हे script", "मी हा script",
    "मी आपल्याला स्पष्टपणे",
    "सन्मानपूर्वक नकार", "नकार करत आहे",
    "वास्तविकता:", "खरे नसलेले", "अपुष्ट",
    "reliable sources", "verify करा",
    "माझ्या लक्षात आले",
    "फक्त हे सांगितले आहे", "उपलब्ध नाही",
    "कोणतीही माहिती नाही", "कोणताही उल्लेख नाही",
    "आवश्यक माहिती नाही", "शोधलेल्या फलिताम्ध्ये",
    "मी माफी मागतो", "क्षमा करा, पण मी",
    "मी या बातमी", "या विषयावर script",
]

SKIP_TITLE_KEYWORDS = [
    'राशीभविष्य', 'राशिभविष्य', 'ज्योतिष', 'पूजा', 'अध्यात्म',
    'horoscope', 'rashifal', 'astrology', 'dharm', 'puja',
    'utility', 'यूटिलिटी', 'आध्यात्मिक', 'spirituality',
    'धार्मिक परंपरा', 'मंदिर', 'व्रत', 'उपवास', 'rashibhavishya',
    'अध्यात्म बातम्या', 'धार्मिक', 'ज्योतिष',
    'राजकारण', 'politics', 'crime', 'गुन्हा', 'अपघात', 'accident',
    'शेअर बाजार', 'stock market', 'हवामान', 'weather',
]

SKIP_CONTENT_KEYWORDS = [
    'tv9 मराठी एक 24/7 मराठी भाषिक वृत्तवाहिनी',
    'अध्यात्म बातम्यांचा विशेष विभाग आहे जो',
    'राशीभविष्य, मंदिरातील पूजा, धार्मिक परंपरा',
    'आध्यात्मिक जीवनाची संपूर्ण माहिती',
    'धार्मिक आणि आध्यात्मिक विषयांवर सर्वांग माहिती',
    'यूटिलिटी बातम्या म्हणजे काय',
    'utility news definition',
]

SKIP_URL_PATTERNS = [
    'javascript:', 'mailto:', '#',
    '/category/', '/tag/', '/author/',
    'facebook.com', 'twitter.com', 'instagram.com',
    'youtube.com', 'whatsapp.com', '/myaccount/',
    '/install_app', '/advertisement', '/epaper',
    'web-stories', 'photo-gallery', '/videos/',
    '/games/', '/jokes/', '/terms-and-conditions',
    '/topic/', '/widget/', '/livetv',
    'articlelist', '/live',
    '/utility/', '/utilities/',
    '/adhyatma/', '/astrology/', '/rashifal/',
    '/horoscope/', '/jyotish/', '/puja/',
    '/dharm/', '/dharma/', '/spirituality/',
    '/rashibhavishya/', '/religion/',
    '/politics/', '/crime/', '/sports/', '/economy/',
    '/business/', '/technology/', '/education/',
    '/thane/', '/pune/', '/nashik/', '/mumbai-news/',
]

FILM_KEYWORDS = [
    'bollywood', 'बॉलीवुड', 'hindi film', 'हिंदी चित्रपट',
    'box office', 'बॉक्स ऑफिस', 'ott release', 'ott',
    'नेटफ्लिक्स', 'amazon prime', 'disney hotstar', 'zee5',
    'मराठी चित्रपट', 'marathi film', 'मराठी सिनेमा',
    'मराठी नाटक', 'मराठी मालिका', 'zee marathi', 'star pravah',
    'tollywood', 'kollywood', 'tamil film', 'telugu film',
    'चित्रपट', 'सिनेमा', 'film', 'movie', 'actor', 'actress',
    'अभिनेता', 'अभिनेत्री', 'दिग्दर्शक', 'director', 'producer',
    'trailer', 'ट्रेलर', 'teaser', 'टीझर', 'song', 'गाणे',
    'award', 'पुरस्कार', 'celebrity', 'सेलिब्रिटी', 'star',
    'shahrukh', 'salman khan', 'aamir khan', 'akshay kumar',
    'deepika', 'priyanka', 'katrina', 'ranveer', 'ranbir',
    'prabhas', 'allu arjun', 'vijay', 'rajinikanth',
    'amitabh', 'hrithik', 'tiger shroff',
    'नाना पाटेकर', 'सचिन', 'महेश मांजरेकर',
    'सई ताम्हणकर', 'नागराज मंजुळे',
    'film history', 'cinema history', 'old bollywood',
    'golden era', 'kishore kumar', 'lata mangeshkar',
    'guru dutt', 'raj kapoor', 'dilip kumar', 'madhubala',
]

# ============================================================
# Story Sites
# ============================================================
STORY_SITES = [
    {
        "name": "Indian Film History - Blogs",
        "url": "https://www.indianfilmhistory.com/blogs",
        "link_pattern": "indianfilmhistory.com",
        "target": 2,
    },
    {
        "name": "Indian Film History - Subdomain",
        "url": "https://blogs.indianfilmhistory.com/",
        "link_pattern": "indianfilmhistory.com",
        "target": 2,
    },
]

# ============================================================
# News Sites
# ============================================================
NEWS_SITES = [
    {
        "name": "Maharashtra Times - Entertainment",
        "url": "https://maharashtratimes.com/entertainment",
        "link_pattern": "maharashtratimes.com",
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "TV9 Marathi - Entertainment",
        "url": "https://www.tv9marathi.com/entertainment",
        "link_pattern": "tv9marathi.com",
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "ABP Majha - Entertainment",
        "url": "https://marathi.abplive.com/entertainment",
        "link_pattern": "abplive.com",
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "Lokmat - Entertainment",
        "url": "https://www.lokmat.com/entertainment/",
        "link_pattern": "lokmat.com",
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "NDTV Marathi - Entertainment",
        "url": "https://marathi.ndtv.com/entertainment",
        "link_pattern": "marathi.ndtv.com",
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "Bollywood Hungama",
        "url": "https://www.bollywoodhungama.com/news/bollywood/",
        "link_pattern": "bollywoodhungama.com",
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "Pinkvilla",
        "url": "https://www.pinkvilla.com/entertainment",
        "link_pattern": "pinkvilla.com",
        "target": 2,
        "fetch_limit": 25
    },
]

# ============================================================
# Story Script Examples (few-shot reference)
# ============================================================
STORY_SCRIPT_EXAMPLE_1 = """ये दोस्ते हम नाही तोडेंगे — हे गाणं वाजलं की डोळ्यासमोर काय येतं?
ती बाइक, साइडट्रॉली, आणि एक मेकांसाठी जीव द्यायला तयार असलेले — जय आणि वीरू.
जय एकदम शांत, गंभीर — तर वीरू एकदम मस्तीखोर, इमोशनल.
दोघांची केमिस्ट्री भन्नाट होती!
पन्नास वर्षे झाली तरी ही जोडी मैत्रीचा गोल्ड स्टँडर्ड आहे.
ती साइडट्रॉली वाली बाइक — फक्त बाइक नव्हती, ती मैत्रीचं प्रतीक होती!
पण खरा ट्विस्ट आहे त्या टॉसच्या कॉइनमध्ये.
शेवटच्या सीनमध्ये जेव्हा जय, वीरूला वाचवण्यासाठी स्वतःचा जीव देतो,
तेव्हा गुपित उलगडतं — त्या कॉइनच्या दोन्ही बाजूंनी हेड्सच होतं!
जय ने नेहमीच वीरूला जिंकवण्यासाठी स्वतःच्या जीवाशी खेळ केला.
हीच असते खरी दोस्ती.
गाल्या येतील, पैसा येईल — पण जय-वीरूसारखा मित्र?
तुम्हाला असा एखादा जय आहे का?
त्याला हा व्हिडिओ नक्की शेअर करा आणि सांगा!
तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."""

STORY_SCRIPT_EXAMPLE_2 = """तुम्हाला वाटत होतं बॉलीवूड अभिनेत्रीला डान्समध्ये परफेक्ट असायला हवं?
पण झीनत अमान यांनी ही व्याख्याच बदलून टाकली!
कधीच डान्सर ट्रेनिंग घेतलं नाही — तरीही 'ओ दिवाने' मध्ये अशी काही जादू!
तो विग आणि हाय किक्स आठवतायत का?
सुरुवातीला दिग्दर्शकांना टेन्शन होतं — पण मग स्ट्रेटजी बदलली.
झीनतला कोरियोग्राफ करायचं नाही — फक्त त्यांची स्टाईल फॉलो करायची!
कठीण कोरियोग्राफीपेक्षा त्यांचा नैसर्गिक वावर लोकांच्या काळजाचा ठाव घेऊन गेला.
कलेपेक्षा आत्मविश्वास मोठा असतो — हे झीनत यांनी सिद्ध केलं!
झीनत अमानचा हा किस्सा तुम्हाला कसा वाटला?
कमेंट्समध्ये नक्की सांगा!
तुमचं काय मत आहे? कमेंट करून सांगा आणि फॉलो करा कलाकार कट्टा.."""


# ============================================================
# Token Tracking
# ============================================================
total_input_tokens  = 0
total_output_tokens = 0
total_cost          = 0.0
processed_hashes    = set()


# ============================================================
# Filters
# ============================================================
def is_film_related(title: str, content: str) -> bool:
    combined = (title + ' ' + content[:1500]).lower()
    return any(kw.lower() in combined for kw in FILM_KEYWORDS)


# ============================================================
# Google Sheets Setup
# ============================================================
def setup_google_sheets(max_retries: int = 5, retry_delay: int = 10):
    for attempt in range(1, max_retries + 1):
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(
                GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scope
            )
            client = gspread.authorize(creds)

            try:
                sheet = client.open(GOOGLE_SHEET_NAME)
                print(f"✅ Connected: '{GOOGLE_SHEET_NAME}'")
            except gspread.SpreadsheetNotFound:
                sheet = client.create(GOOGLE_SHEET_NAME)
                print(f"✅ Created: '{GOOGLE_SHEET_NAME}'")

            try:
                worksheet = sheet.worksheet(GOOGLE_WORKSHEET_NAME)
                print(f"✅ Worksheet: '{GOOGLE_WORKSHEET_NAME}'")
                current_rows = worksheet.row_count
                if current_rows < 2000:
                    worksheet.add_rows(5000 - current_rows)
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(
                    title=GOOGLE_WORKSHEET_NAME, rows=5000, cols=10
                )
                worksheet.update('A1:F1', [[
                    'Timestamp (IST)', 'Type', 'Category', 'Title', 'Script', 'Source Link'
                ]])
                worksheet.format('A1:F1', {
                    'textFormat': {
                        'bold': True,
                        'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                    },
                    'backgroundColor': {'red': 0.5, 'green': 0.2, 'blue': 0.8},
                    'horizontalAlignment': 'CENTER'
                })
                print(f"✅ Created worksheet with headers")

            return worksheet

        except gspread.exceptions.APIError as e:
            error_str = str(e)
            if any(code in error_str for code in ['503', '500', '429', '502']):
                if attempt < max_retries:
                    print(f"⚠️ Sheets error (attempt {attempt}) — retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue
                return None
            return None
        except FileNotFoundError:
            print(f"❌ credentials.json not found!")
            return None
        except Exception as e:
            print(f"❌ Sheets setup error: {e}")
            return None


# ============================================================
# Save to Google Sheets
# ============================================================
def save_to_google_sheets(worksheet, script_type, category, title, script, source_link, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            timestamp   = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')
            script      = '\n'.join(str(i) for i in script) if isinstance(script, list) else str(script).strip()
            script      = script.replace('[', '').replace(']', '')
            title       = str(title).strip()
            source_link = str(source_link).strip()
            category    = str(category).strip().lower()

            if category not in VALID_CATEGORIES:
                category = "general_entertainment"

            next_row = len(worksheet.get_all_values()) + 1
            worksheet.append_row(
                [timestamp, script_type, CATEGORY_DISPLAY.get(category, category.upper()), title, script, source_link],
                value_input_option='RAW'
            )

            worksheet.format(f'A{next_row}:F{next_row}', {
                'textFormat': {'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0}, 'fontSize': 10},
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                'wrapStrategy': 'WRAP',
                'verticalAlignment': 'TOP'
            })

            type_colors = {
                "NEWS":  {'red': 0.85, 'green': 0.93, 'blue': 1.0},
                "STORY": {'red': 1.0,  'green': 0.88, 'blue': 0.6},
            }
            worksheet.format(f'B{next_row}', {
                'textFormat': {'bold': True, 'fontSize': 10},
                'backgroundColor': type_colors.get(script_type, {'red': 1.0, 'green': 1.0, 'blue': 1.0}),
                'horizontalAlignment': 'CENTER'
            })

            category_colors = {
                'bollywood':             {'red': 1.0,  'green': 0.85, 'blue': 0.7},
                'marathi_cinema':        {'red': 0.8,  'green': 1.0,  'blue': 0.8},
                'south_cinema':          {'red': 0.7,  'green': 0.9,  'blue': 1.0},
                'ott':                   {'red': 0.9,  'green': 0.8,  'blue': 1.0},
                'awards':                {'red': 1.0,  'green': 0.95, 'blue': 0.6},
                'celebrity':             {'red': 1.0,  'green': 0.8,  'blue': 0.9},
                'general_entertainment': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
                'film_story':            {'red': 1.0,  'green': 0.85, 'blue': 0.5},
            }
            worksheet.format(f'C{next_row}', {
                'textFormat': {'bold': True, 'fontSize': 10},
                'backgroundColor': category_colors.get(category, category_colors['general_entertainment']),
                'horizontalAlignment': 'CENTER'
            })

            print(f"✅ [{script_type}][{CATEGORY_DISPLAY.get(category, category.upper())}] {title[:50]}...")
            return True

        except gspread.exceptions.APIError as e:
            error_str = str(e)
            if any(code in error_str for code in ['503', '500', '429', '502']):
                if attempt < max_retries:
                    print(f"   ⚠️ Sheets retry {attempt}/{max_retries} in 8s...")
                    time.sleep(8)
                    continue
                return False
            return False
        except Exception as e:
            print(f"❌ Save error: {e}")
            return False


# ============================================================
# Helpers
# ============================================================
def get_content_hash(title: str, content: str) -> str:
    return hashlib.md5(f"{title.lower()}{content[:200].lower()}".encode()).hexdigest()

def sort_by_count(item):
    return -item[1]

def sort_by_priority(item):
    return {'high': 1, 'medium': 2, 'low': 3}.get(item.get('importance', 'medium'), 2)

def safe_truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    for punct in ['।', '.', '!', '?', '\n']:
        last_pos = truncated.rfind(punct)
        if last_pos > max_chars * 0.7:
            return truncated[:last_pos + 1]
    last_space = truncated.rfind(' ')
    if last_space > max_chars * 0.7:
        return truncated[:last_space]
    return truncated

def extract_response_content(response) -> str:
    raw_choice = response.choices[0]
    if hasattr(raw_choice, 'message'):
        msg = raw_choice.message
        if hasattr(msg, 'content') and isinstance(msg.content, str):
            return msg.content
        elif hasattr(msg, 'content') and isinstance(msg.content, list):
            return ' '.join(
                block.get('text', '') if isinstance(block, dict) else str(block)
                for block in msg.content
            )
        elif isinstance(msg, list):
            return ' '.join(
                block.get('text', '') if isinstance(block, dict) else str(block)
                for block in msg
            )
        else:
            return str(msg)
    elif isinstance(raw_choice, dict):
        msg = raw_choice.get('message', {})
        return msg.get('content', '') if isinstance(msg, dict) else str(msg)
    return str(raw_choice)

def is_script_complete(script: str) -> bool:
    return script.strip().endswith(SCRIPT_CTA.strip())

def get_last_line(script: str) -> str:
    lines = [l.strip() for l in script.strip().split('\n') if l.strip()]
    return lines[-1] if lines else ""

def is_valid_marathi_script(script: str) -> bool:
    if len(script) < 100:
        return False
    if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
        return False
    devanagari = len(re.findall(r'[\u0900-\u097F]', script))
    total      = len(script.replace(' ', '').replace('\n', ''))
    return (devanagari / max(total, 1)) > 0.35


# ============================================================
# Script Completion Callback
# ============================================================
async def complete_script_if_needed(script: str, article: Dict) -> str:
    global total_input_tokens, total_output_tokens, total_cost

    if is_script_complete(script):
        return script

    last_line = get_last_line(script)
    print(f"   🔧 Script incomplete — last: '{last_line[:60]}' — completing...")

    try:
        response = perplexity_client.chat.completions.create(
            model=SCRIPT_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": f'तुम्ही script writer आहात. फक्त मराठी lines लिहा. शेवटची line नक्की हीच: "{SCRIPT_CTA}"'
                },
                {
                    "role": "user",
                    "content": f"""खालील अर्धवट मराठी script पूर्ण करा.

अर्धवट script:
{script}

नियम:
- वरील script च्या पुढे फक्त उर्वरित lines लिहा
- एकूण 15-18 lines होतील असे नवीन lines जोडा
- शेवटची line नक्की हीच: "{SCRIPT_CTA}"
- फक्त नवीन lines लिहा, जुन्या lines परत लिहू नका
- फक्त मराठीत लिहा
- "search results", "माहिती नाही" असे लिहू नका"""
                }
            ],
            temperature=0.7,
            max_tokens=600
        )

        if hasattr(response, 'usage'):
            i_t = response.usage.prompt_tokens
            o_t = response.usage.completion_tokens
            total_input_tokens  += i_t
            total_output_tokens += o_t
            total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

        completion = extract_response_content(response).strip()
        completion = re.sub(r'<think>.*?</think>', '', completion, flags=re.DOTALL).strip()
        completion = completion.replace('```', '').strip()

        if any(kw.lower() in completion.lower() for kw in REFUSAL_KEYWORDS):
            return script.strip() + f"\n\n{SCRIPT_CTA}"

        completed = script.strip() + "\n\n" + completion.strip()
        if not is_script_complete(completed):
            completed = completed.strip() + f"\n\n{SCRIPT_CTA}"

        print(f"   ✅ Script completed")
        return completed

    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
            raise CreditExhaustedException(str(e))
        return script.strip() + f"\n\n{SCRIPT_CTA}"


# ============================================================
# API Credit Check
# ============================================================
async def check_api_credits():
    try:
        perplexity_client.chat.completions.create(
            model=ANALYSIS_MODEL,
            messages=[{"role": "user", "content": "ok"}],
            max_tokens=1
        )
        print("✅ API credits OK")
        return True
    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in [
            '402', '429', '401', 'insufficient', 'credit',
            'quota', 'balance', 'payment', 'billing', 'rate limit', 'exceeded'
        ]):
            print("❌ PERPLEXITY API CREDITS EXHAUSTED!")
            print("👉 Top up: https://www.perplexity.ai/settings/api")
            return False
        print(f"❌ Unknown API error: {e}")
        return False


# ============================================================
# Fetch Article with Retry
# ============================================================
async def fetch_article_with_retry(crawler, url: str, retries: int = 3) -> str:
    for attempt in range(1, retries + 1):
        try:
            result = await crawler.arun(
                url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    word_count_threshold=10,
                    page_timeout=25000
                )
            )
            if result.success and len(result.markdown) > 50:
                return result.markdown
            await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(2)
    return ""


# ============================================================
# Perplexity Fallback — Film News
# ============================================================
async def fetch_film_news_via_perplexity(needed: int) -> List[Dict]:
    global total_input_tokens, total_output_tokens, total_cost

    print(f"\n🔁 Fetching {needed} film articles via Perplexity fallback...")

    try:
        response = perplexity_client.chat.completions.create(
            model=ANALYSIS_MODEL,
            messages=[
                {"role": "system", "content": "You are a Bollywood/Indian cinema news researcher. Return ONLY valid JSON array."},
                {
                    "role": "user",
                    "content": f"""Find the latest {needed} entertainment/film news articles from India.
Focus: Bollywood, Marathi cinema, South Indian films, OTT releases, celebrity news.

Return JSON array with {needed} items:
[{{
  "title": "News headline",
  "detailed_summary": "150-200 word Marathi summary",
  "category": "bollywood/marathi_cinema/south_cinema/ott/awards/celebrity/general_entertainment",
  "importance": "high/medium/low",
  "key_points": ["मराठी मुद्दा १", "मराठी मुद्दा २", "मराठी मुद्दा ३"],
  "link": "source url or empty string",
  "script_type": "NEWS"
}}]
Rules:
- detailed_summary and key_points in Marathi
- Return only JSON array"""
                }
            ],
            temperature=0.3,
            max_tokens=3500
        )

        if hasattr(response, 'usage'):
            i_t = response.usage.prompt_tokens
            o_t = response.usage.completion_tokens
            total_input_tokens  += i_t
            total_output_tokens += o_t
            c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
            total_cost += c

        content = extract_response_content(response)
        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
        match = re.search(r'\[.*\]', content, re.DOTALL)

        if match:
            parsed = json.loads(match.group())
            results = []
            for art in parsed:
                if art.get('category') not in VALID_CATEGORIES:
                    art['category'] = 'general_entertainment'
                art['source']      = 'Perplexity Fallback'
                art['scraped_at']  = datetime.now(IST).isoformat()
                art['script_type'] = 'NEWS'
                art['hash']        = get_content_hash(art.get('title', ''), art.get('detailed_summary', ''))
                results.append(art)
            print(f"   ✅ Got {len(results)} articles from Perplexity fallback")
            return results
        return []

    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
            raise CreditExhaustedException(str(e))
        print(f"   ❌ Fallback error: {e}")
        return []


# ============================================================
# Perplexity Fallback — Film Stories
# ============================================================
async def fetch_film_stories_via_perplexity(needed: int) -> List[Dict]:
    global total_input_tokens, total_output_tokens, total_cost

    print(f"\n📖 Fetching {needed} film stories via Perplexity fallback...")

    try:
        response = perplexity_client.chat.completions.create(
            model=ANALYSIS_MODEL,
            messages=[
                {"role": "system", "content": "You are an Indian film history researcher. Return ONLY valid JSON array."},
                {
                    "role": "user",
                    "content": f"""Find {needed} interesting untold stories/anecdotes from Indian film history.
Focus: behind-the-scenes, interesting facts, personal stories of classic/modern film stars.

Return JSON array:
[{{
  "title": "Story headline",
  "detailed_summary": "300-400 word detailed story in Marathi",
  "category": "film_story",
  "importance": "high",
  "key_points": ["मराठी मुद्दा १", "मराठी मुद्दा २", "मराठी मुद्दा ३"],
  "persons_involved": "actor/actress names",
  "film_name": "film name if any",
  "link": "",
  "script_type": "STORY"
}}]
Rules:
- detailed_summary in Marathi (300-400 words), key_points in Marathi
- Interesting, emotional, surprising stories only
- Return only JSON array"""
                }
            ],
            temperature=0.4,
            max_tokens=4000
        )

        if hasattr(response, 'usage'):
            i_t = response.usage.prompt_tokens
            o_t = response.usage.completion_tokens
            total_input_tokens  += i_t
            total_output_tokens += o_t
            c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
            total_cost += c

        content = extract_response_content(response)
        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
        match = re.search(r'\[.*\]', content, re.DOTALL)

        if match:
            parsed = json.loads(match.group())
            results = []
            for art in parsed:
                art['category']    = 'film_story'
                art['source']      = 'Perplexity Story Fallback'
                art['scraped_at']  = datetime.now(IST).isoformat()
                art['script_type'] = 'STORY'
                art['hash']        = get_content_hash(art.get('title', ''), art.get('detailed_summary', ''))
                results.append(art)
            print(f"   ✅ Got {len(results)} stories from Perplexity fallback")
            return results
        return []

    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
            raise CreditExhaustedException(str(e))
        print(f"   ❌ Story fallback error: {e}")
        return []


# ============================================================
# Scrape Story Sources
# ============================================================
async def scrape_story_sources() -> List[Dict]:
    story_articles = []
    seen_links = set()

    async with AsyncWebCrawler(verbose=False) as crawler:
        for site in STORY_SITES:
            if len(story_articles) >= TARGET_STORY_SCRIPTS:
                break

            print(f"\n{'='*60}")
            print(f"📖 {site['name']} | Target: {site['target']}")
            print(f"{'='*60}")

            try:
                result = await crawler.arun(
                    site['url'],
                    config=CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        wait_for="body",
                        word_count_threshold=10,
                        page_timeout=30000,
                        js_code="await new Promise(r => setTimeout(r, 3000));"
                    )
                )

                if not result.success:
                    print(f"❌ Failed to load: {site['url']}")
                    continue

                soup = BeautifulSoup(result.html, 'html.parser')
                raw_links = []

                for link_tag in soup.find_all('a', href=True):
                    href  = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)

                    if len(title) < 10 or len(title) > 300:
                        continue
                    if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
                        continue

                    if href.startswith('/'):
                        base = site['url'].split('/') + '//' + site['url'].split('/')[2]
                        href = base + href

                    if href.startswith('http') and href not in seen_links:
                        raw_links.append({'title': title, 'link': href})

                print(f"📋 Found {len(raw_links)} candidate links")

                for item in raw_links:
                    if len(story_articles) >= TARGET_STORY_SCRIPTS:
                        break
                    if item['link'] in seen_links:
                        continue

                    print(f"   📖 Fetching: {item['title'][:55]}...")
                    markdown = await fetch_article_with_retry(crawler, item['link'])
                    if not markdown or len(markdown) < 100:
                        continue

                    seen_links.add(item['link'])
                    content_hash = get_content_hash(item['title'], markdown)

                    if content_hash in processed_hashes:
                        continue

                    processed_hashes.add(content_hash)
                    story_articles.append({
                        'title':            item['title'],
                        'link':             item['link'],
                        'content':          safe_truncate(markdown, 4000),
                        'hash':             content_hash,
                        'category':         'film_story',
                        'source':           site['name'],
                        'scraped_at':       datetime.now(IST).isoformat(),
                        'script_type':      'STORY',
                        'detailed_summary': safe_truncate(markdown, 800),
                        'key_points':       [item['title']],
                        'importance':       'high',
                        'persons_involved': '',
                        'film_name':        '',
                    })
                    print(f"   ✅ Story [{len(story_articles)}/{TARGET_STORY_SCRIPTS}]: {item['title'][:50]}")
                    await asyncio.sleep(1)

            except CreditExhaustedException:
                raise
            except Exception as e:
                print(f"❌ Error scraping {site['name']}: {e}")

            await asyncio.sleep(2)

    if len(story_articles) < TARGET_STORY_SCRIPTS:
        needed = TARGET_STORY_SCRIPTS - len(story_articles)
        extra = await fetch_film_stories_via_perplexity(needed)
        story_articles.extend(extra)

    return story_articles[:TARGET_STORY_SCRIPTS]


# ============================================================
# Scrape News Sources
# ============================================================
async def scrape_film_sources() -> List[Dict]:
    all_news = []

    async with AsyncWebCrawler(verbose=False) as crawler:
        for site in NEWS_SITES:
            print(f"\n{'='*60}")
            print(f"🎬 {site['name']} | Target: {site['target']}")
            print(f"{'='*60}")

            site_articles = []

            try:
                result = await crawler.arun(
                    site['url'],
                    config=CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        wait_for="body",
                        word_count_threshold=10,
                        page_timeout=30000,
                        js_code="await new Promise(r => setTimeout(r, 3000));"
                    )
                )

                if not result.success:
                    print(f"❌ Failed: {site['name']}")
                    continue

                soup = BeautifulSoup(result.html, 'html.parser')
                raw_articles = []

                for link_tag in soup.find_all('a', href=True):
                    href  = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)

                    if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
                        continue
                    if not (15 < len(title) < 300):
                        continue
                    if site['link_pattern'] not in href:
                        continue
                    if any(x in href.lower() for x in SKIP_URL_PATTERNS):
                        continue

                    if href.startswith('/'):
                        base = site['url'].split('/') + '//' + site['url'].split('/')[2]
                        href = base + href

                    if href.startswith('http'):
                        raw_articles.append({'title': title, 'link': href})

                seen = set()
                unique_links = []
                for a in raw_articles:
                    if a['link'] not in seen:
                        unique_links.append(a)
                        seen.add(a['link'])

                print(f"📋 Found {len(unique_links)} unique links")

                for article in unique_links:
                    if len(site_articles) >= site['target']:
                        break

                    print(f"   🔗 [{len(site_articles)+1}/{site['target']}] {article['title'][:50]}...")

                    markdown = await fetch_article_with_retry(crawler, article['link'])
                    content  = markdown if markdown else article['title']

                    if not is_film_related(article['title'], content):
                        print(f"   🚫 Not film-related — skipped")
                        continue

                    if any(kw.lower() in content.lower() for kw in SKIP_CONTENT_KEYWORDS):
                        print(f"   ⏭️  Skipped (utility/spiritual)")
                        continue

                    content_hash = get_content_hash(article['title'], content)

                    if content_hash not in processed_hashes:
                        site_articles.append({
                            'title':            article['title'],
                            'link':             article['link'],
                            'content':          safe_truncate(content, 3500),
                            'hash':             content_hash,
                            'has_full_content': bool(markdown),
                            'script_type':      'NEWS',
                        })
                        processed_hashes.add(content_hash)
                        tag = "✅" if markdown else "⚠️ fallback"
                        print(f"   {tag} [{len(site_articles)}/{site['target']}] {article['title'][:50]}...")
                    else:
                        print(f"   🔄 Duplicate skipped")

                    await asyncio.sleep(1)

                print(f"\n📦 {site['name']}: {len(site_articles)}/{site['target']} collected")

                if site_articles:
                    filtered = await smart_analyze_with_category(site_articles, site['name'])
                    all_news.extend(filtered)

            except CreditExhaustedException:
                raise
            except Exception as e:
                print(f"❌ Error {site['name']}: {e}")

            await asyncio.sleep(3)

    return all_news


# ============================================================
# AI Categorization
# ============================================================
async def smart_analyze_with_category(articles: List[Dict], source_name: str):
    global total_input_tokens, total_output_tokens, total_cost

    all_filtered = []

    for batch_start in range(0, len(articles), 5):
        raw_batch = articles[batch_start:batch_start + 5]
        batch = [
            a for a in raw_batch
            if not any(kw.lower() in a.get('content', '').lower() for kw in SKIP_CONTENT_KEYWORDS)
        ]

        if not batch:
            continue

        index_to_link  = {i: a['link']                    for i, a in enumerate(batch)}
        index_to_title = {i: a['title']                   for i, a in enumerate(batch)}
        index_to_type  = {i: a.get('script_type', 'NEWS') for i, a in enumerate(batch)}

        articles_text = ""
        for idx, article in enumerate(batch):
            articles_text += f"INDEX_{idx}: {article['title']}\n{safe_truncate(article['content'], 500)}\n---\n"

        prompt = f"""भारतीय चित्रपट बातम्या विश्लेषक: खालील बातम्यांना category आणि Marathi summary द्या.

⚠️ नियम:
1. detailed_summary आणि key_points फक्त मराठीत लिहा
2. JSON मध्ये "index" field EXACTLY जसा दिला (0,1,2,3,4) तसाच परत द्या

Categories: bollywood, marathi_cinema, south_cinema, ott, awards, celebrity, general_entertainment

JSON array format:
[{{"index": 0, "category": "bollywood", "detailed_summary": "मराठी सारांश १५०-२०० शब्द", "importance": "high/medium/low", "key_points": ["मुद्दा १", "मुद्दा २", "मुद्दा ३"]}}]

बातम्या:
{articles_text}

फक्त JSON array. Index 0 ते {len(batch)-1} पर्यंत."""

        try:
            response = perplexity_client.chat.completions.create(
                model=ANALYSIS_MODEL,
                messages=[
                    {"role": "system", "content": "Return ONLY valid JSON array. Use index field (0,1,2...). No title or link fields."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=3000
            )

            if hasattr(response, 'usage'):
                i_t = response.usage.prompt_tokens
                o_t = response.usage.completion_tokens
                total_input_tokens  += i_t
                total_output_tokens += o_t
                c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
                total_cost += c
                print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

            content = extract_response_content(response)
            if not content.strip():
                raise ValueError("Empty API response")

            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            match = re.search(r'\[.*\]', content, re.DOTALL)

            if match:
                parsed = json.loads(match.group())
                for art in parsed:
                    idx = art.get('index')
                    if idx is not None and idx in index_to_link:
                        art['link']        = index_to_link[idx]
                        art['title']       = index_to_title[idx]
                        art['script_type'] = index_to_type[idx]
                    else:
                        pos = len(all_filtered) % len(batch)
                        art['link']        = index_to_link.get(pos, '')
                        art['title']       = index_to_title.get(pos, art.get('title', ''))
                        art['script_type'] = index_to_type.get(pos, 'NEWS')

                    if art.get('category') not in VALID_CATEGORIES:
                        art['category'] = 'general_entertainment'

                all_filtered.extend(parsed)
                print(f"   ✅ Categorized {len(parsed)}")
            else:
                for i, article in enumerate(batch):
                    all_filtered.append({
                        'index':            i,
                        'title':            article['title'],
                        'category':         'general_entertainment',
                        'detailed_summary': safe_truncate(article['content'], 600),
                        'importance':       'medium',
                        'link':             article['link'],
                        'key_points':       [article['title']],
                        'script_type':      article.get('script_type', 'NEWS'),
                    })

        except json.JSONDecodeError:
            for i, article in enumerate(batch):
                all_filtered.append({
                    'index':            i,
                    'title':            article['title'],
                    'category':         'general_entertainment',
                    'detailed_summary': safe_truncate(article['content'], 600),
                    'importance':       'medium',
                    'link':             article['link'],
                    'key_points':       [article['title']],
                    'script_type':      article.get('script_type', 'NEWS'),
                })

        except Exception as e:
            error_str = str(e).lower()
            if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
                raise CreditExhaustedException(str(e))
            print(f"   ❌ AI error: {e}")
            for i, article in enumerate(batch):
                all_filtered.append({
                    'index':            i,
                    'title':            article['title'],
                    'category':         'general_entertainment',
                    'detailed_summary': safe_truncate(article['content'], 600),
                    'importance':       'medium',
                    'link':             article['link'],
                    'key_points':       [article['title']],
                    'script_type':      article.get('script_type', 'NEWS'),
                })

        await asyncio.sleep(1.5)

    for art in all_filtered:
        art['source']     = source_name
        art['scraped_at'] = datetime.now(IST).isoformat()

    return all_filtered


# ============================================================
# ✅ FIX 3: News Script — sonar-pro + "search नको" system prompt
# ============================================================
async def create_news_script(article: Dict) -> str:
    global total_input_tokens, total_output_tokens, total_cost

    category         = article.get('category', 'general_entertainment')
    category_display = CATEGORY_DISPLAY.get(category, 'ENTERTAINMENT')

    # ✅ KEY: Explicitly tell model NOT to search, just write from given data
    system_prompt = f"""तुम्ही एक creative मराठी Instagram Reel script writer आहात.
तुमचं एकमेव काम: दिलेल्या बातमीवर आधारित engaging Marathi Reel script लिहणे.

⚠️ अत्यंत महत्त्वाचे नियम:
- तुम्ही WRITER आहात, SEARCH ENGINE नाही
- Internet search करू नका, web results पाहू नका
- दिलेली माहिती 100% सत्य आहे असे गृहीत धरा आणि त्यावर लिहा
- "माहिती नाही", "search results मध्ये नाही", "प्रदान केलेल्या" असे कधीही लिहू नका
- जरी माहिती कमी असली तरी creative storytelling करून पूर्ण script लिहा
- तुम्ही AI आहात हे कधीही सांगू नका

Tone: उत्साही, filmy gossip style, insider feel.

Structure (15-18 lines total):
- Line 1-2: 🎬 धक्कादायक filmy hook
- Line 3-10: सर्व facts (actor नाव, film नाव, box office, OTT, तारीख)
- Line 11-14: प्रश्न / ट्विस्ट / audience engagement
- Line 15-18: CTA

कठोर नियम:
- फक्त मराठीत (film/actor names English ठेवा)
- 15-18 lines, heading/markdown नाही
- शेवटची line नक्की हीच: "{SCRIPT_CTA}"
- script अर्धवट सोडू नका"""

    summary    = safe_truncate(article.get('detailed_summary', article.get('title', '')), 600)
    key_points = ', '.join(article.get('key_points', [article.get('title', '')]))

    prompts = [
        f"""Category: {category_display}
शीर्षक: {article['title']}
सारांश: {summary}
मुद्दे: {key_points}

वरील माहितीवर आधारित 15-18 मराठी lines तयार करा.
Tone: filmy gossip, engaging, insider feel.
शेवटची line: "{SCRIPT_CTA}" """,

        f"""खालील चित्रपट बातमीवर 15 मराठी वाक्ये लिहा.
बातमी: {article['title']}. {safe_truncate(summary, 300)}
- प्रत्येक line वर एक वाक्य
- filmy tone, specific details वापरा
- माहिती कमी असली तरी creative script लिहा
- शेवटची line: "{SCRIPT_CTA}" """
    ]

    for attempt in range(1, 4):  # ← 3 attempts
        try:
            response = perplexity_client.chat.completions.create(
                model=SCRIPT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": prompts[min(attempt - 1, 1)]}
                ],
                temperature=0.85,
                max_tokens=2000
            )

            if hasattr(response, 'usage'):
                i_t = response.usage.prompt_tokens
                o_t = response.usage.completion_tokens
                total_input_tokens  += i_t
                total_output_tokens += o_t
                total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

            script = extract_response_content(response).strip()
            script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
            script = script.replace('```', '').strip()

            if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
                print(f"   ⚠️ Attempt {attempt}: Refusal — retrying...")
                await asyncio.sleep(1)
                continue

            if is_valid_marathi_script(script):
                return await complete_script_if_needed(script, article)

            print(f"   ⚠️ Attempt {attempt}: Not valid Marathi — retrying...")

        except CreditExhaustedException:
            raise
        except Exception as e:
            error_str = str(e).lower()
            if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
                raise CreditExhaustedException(str(e))
            print(f"   ⚠️ Attempt {attempt} error: {e}")
            await asyncio.sleep(2)

    # ✅ FIX 4: Real-content fallback (not generic template)
    title_fb   = article.get('title', 'एक धक्कादायक चित्रपट बातमी')[:80]
    summary_fb = safe_truncate(article.get('detailed_summary', article.get('title', '')), 400)
    key_pts    = article.get('key_points', [])
    key_line   = f"\n\n{key_pts[0]}" if key_pts else ""

    return f"""थांबा! ही बातमी ऐकली का?

{title_fb}

{summary_fb}{key_line}

हे प्रकरण सध्या चित्रपट उद्योगात मोठी चर्चा निर्माण करत आहे.

चाहत्यांमध्ये या बातमीमुळे एकच खळबळ उडाली आहे.

सोशल मीडियावर यावर जोरदार प्रतिक्रिया येत आहेत.

विशेष म्हणजे या प्रकरणाचा परिणाम संपूर्ण इंडस्ट्रीवर होणार आहे.

पण खरा प्रश्न आहे — पुढे काय होणार?

तुम्हाला काय वाटतं?

{SCRIPT_CTA}"""


# ============================================================
# ✅ FIX 5: Story Script — sonar-pro + "दिलेल्या story वर लिहा"
# ============================================================
async def create_story_script(article: Dict) -> str:
    global total_input_tokens, total_output_tokens, total_cost

    content  = article.get('content', article.get('detailed_summary', ''))
    summary  = safe_truncate(content, 800)
    title    = article.get('title', '')
    persons  = article.get('persons_involved', '')
    film     = article.get('film_name', '')

    system_prompt = f"""तुम्ही एक creative मराठी Instagram Reel script writer आहात.
तुमचं काम: दिलेल्या film history story/anecdote वर engaging Marathi script लिहणे.

⚠️ अत्यंत महत्त्वाचे नियम:
- तुम्ही WRITER आहात, SEARCH ENGINE नाही
- Internet search करू नका
- दिलेली story 100% सत्य आहे असे गृहीत धरा
- "माहिती नाही", "search results" असे कधीही लिहू नका
- माहिती कमी असली तरी creative storytelling करा

Tone: conversational, warm, nostalgic — जणू एखाद्या मित्राला गोष्ट सांगत आहात.

Reference examples (हा टोन वापरा):
---
{STORY_SCRIPT_EXAMPLE_1}
---
{STORY_SCRIPT_EXAMPLE_2}
---

Structure (12-16 lines):
- Line 1: Hook — nostalgic reference किंवा प्रश्न
- Line 2-10: Story — specific, vivid, emotional details
- Line 11-13: Twist / lesson / audience question
- Line 14-16: CTA

कठोर नियम:
- फक्त मराठीत (film/actor names English ठेवा)
- No headings, no markdown, conversational tone
- शेवटची line नक्की हीच: "{SCRIPT_CTA}"
- script अर्धवट सोडू नका"""

    user_prompt = f"""खालील film story/anecdote वर Instagram Reel script लिहा.

Title: {title}
Persons: {persons if persons else 'story मध्ये mentioned'}
Film: {film if film else 'story मध्ये mentioned'}
Story:
{summary}

नियम:
- दिलेल्या माहितीवरच script लिहा — search करू नका
- Specific facts, names, emotional moments वापरा
- Reference examples सारखा conversational tone ठेवा
- शेवटची line: "{SCRIPT_CTA}" """

    for attempt in range(1, 4):  # ← 3 attempts
        try:
            response = perplexity_client.chat.completions.create(
                model=SCRIPT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt}
                ],
                temperature=0.9,
                max_tokens=2000
            )

            if hasattr(response, 'usage'):
                i_t = response.usage.prompt_tokens
                o_t = response.usage.completion_tokens
                total_input_tokens  += i_t
                total_output_tokens += o_t
                total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

            script = extract_response_content(response).strip()
            script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
            script = script.replace('```', '').strip()

            if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
                print(f"   ⚠️ Story attempt {attempt}: Refusal — retrying...")
                await asyncio.sleep(1)
                continue

            if is_valid_marathi_script(script):
                return await complete_script_if_needed(script, article)

            print(f"   ⚠️ Story attempt {attempt}: Not valid — retrying...")

        except CreditExhaustedException:
            raise
        except Exception as e:
            error_str = str(e).lower()
            if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
                raise CreditExhaustedException(str(e))
            print(f"   ⚠️ Story attempt {attempt} error: {e}")
            await asyncio.sleep(2)

    # ✅ Real-content story fallback
    summary_fb = safe_truncate(summary, 400)
    person_line = f"\n\n{persons} यांची ही कहाणी आजही लोकांच्या हृदयात घर करून आहे." if persons else ""

    return f"""{title}

{summary_fb}{person_line}

हा किस्सा भारतीय सिनेमाच्या इतिहासात अत्यंत खास आहे.

विशेष म्हणजे हे तुम्हाला कदाचित आधी माहीत नसेल.

या गोष्टीने सिद्ध केलं की प्रतिभा कधीही थांबत नाही.

तुम्हाला हा किस्सा माहीत होता का? कमेंट्समध्ये सांगा!

तुमच्या मित्रांनाही tag करा जे cinema lovers आहेत.

{SCRIPT_CTA}"""


# ============================================================
# Main Pipeline
# ============================================================
async def main():
    global total_input_tokens, total_output_tokens, total_cost

    print("=" * 70)
    print("🎬 KALAKAR KATTA — ENTERTAINMENT REEL SCRIPT GENERATOR v3.0")
    print(f"🔍 Analysis : {ANALYSIS_MODEL}")
    print(f"✍️  Scripts  : {SCRIPT_MODEL}  ← sonar-pro (garbage-free)")
    print(f"🎯 Target   : {TARGET_SCRIPTS} scripts ({TARGET_NEWS_SCRIPTS} NEWS + {TARGET_STORY_SCRIPTS} STORY)")
    print(f"🕐 Timezone : IST (Asia/Kolkata)")
    print("=" * 70)

    credits_ok = await check_api_credits()
    if not credits_ok:
        print("\n🛑 Stopping. Top up credits first.")
        print("👉 https://www.perplexity.ai/settings/api")
        return

    start_time = datetime.now(IST)

    # ── STEP 1A: Scrape Story Sources ──
    print("\n" + "=" * 70)
    print("STEP 1A: SCRAPING STORY SOURCES (indianfilmhistory.com)")
    print("=" * 70 + "\n")

    try:
        story_articles = await scrape_story_sources()
    except CreditExhaustedException:
        print("\n🛑 Credits exhausted during story scraping.")
        return

    print(f"\n✅ Stories collected: {len(story_articles)}/{TARGET_STORY_SCRIPTS}")

    # ── STEP 1B: Scrape News Sources ──
    print("\n" + "=" * 70)
    print("STEP 1B: SCRAPING 7 ENTERTAINMENT NEWS SITES")
    print("=" * 70 + "\n")

    try:
        news_articles = await scrape_film_sources()
    except CreditExhaustedException:
        print("\n🛑 Credits exhausted during news scraping.")
        return

    # De-duplicate news
    unique_news = []
    seen_hashes = set()
    for article in news_articles:
        h = article.get('hash', get_content_hash(article['title'], article.get('detailed_summary', '')))
        if h not in seen_hashes:
            unique_news.append(article)
            seen_hashes.add(h)

    print(f"\n✅ Unique news articles: {len(unique_news)}/{TARGET_NEWS_SCRIPTS}")

    # Fallback if not enough news
    if len(unique_news) < TARGET_NEWS_SCRIPTS:
        needed = TARGET_NEWS_SCRIPTS - len(unique_news)
        print(f"⚡ Only {len(unique_news)} scraped — fetching {needed} via Perplexity fallback...")
        try:
            extra = await fetch_film_news_via_perplexity(needed)
            for art in extra:
                h = art.get('hash', '')
                if h not in seen_hashes:
                    unique_news.append(art)
                    seen_hashes.add(h)
        except CreditExhaustedException:
            print("⚠️ Credits exhausted during fallback — proceeding with available articles")

    # ── Combine and sort ──
    selected_news    = unique_news[:TARGET_NEWS_SCRIPTS]
    selected_stories = story_articles[:TARGET_STORY_SCRIPTS]

    # Category breakdown
    category_counts = {}
    for article in selected_news + selected_stories:
        cat = article.get('category', 'general_entertainment')
        category_counts[cat] = category_counts.get(cat, 0) + 1

    print("\n📊 Category Breakdown:")
    for cat, count in sorted(category_counts.items(), key=sort_by_count):
        print(f"   {CATEGORY_DISPLAY.get(cat, cat.upper()):<15} {'█' * count} ({count})")

    print(f"\n🎯 NEWS: {len(selected_news)} | STORY: {len(selected_stories)}")
    print(f"⏱️  Scraping: {(datetime.now(IST)-start_time).total_seconds():.0f}s\n")

    # ── STEP 2: Generate Scripts → Google Sheets ──
    print("=" * 70)
    print("STEP 2: GENERATING SCRIPTS → GOOGLE SHEETS")
    print("=" * 70 + "\n")

    worksheet        = setup_google_sheets()
    successful_saves = 0
    failed_saves     = 0

    if not worksheet:
        print("❌ Cannot connect to Google Sheets. Aborting.")
        return

    # Process news first, then stories
    all_to_process = (
        [(a, 'NEWS')  for a in selected_news] +
        [(a, 'STORY') for a in selected_stories]
    )

    for idx, (article, script_type) in enumerate(all_to_process, 1):
        cat = article.get('category', 'general_entertainment')
        src = article.get('source', '')[:16]
        print(f"\n[{idx:02d}/{len(all_to_process)}] [{script_type}] {src:<16} | "
              f"{CATEGORY_DISPLAY.get(cat, cat.upper()):<13} | {article['title'][:40]}...")

        try:
            if script_type == 'STORY':
                script = await create_story_script(article)
            else:
                script = await create_news_script(article)
        except CreditExhaustedException:
            print(f"\n🛑 Credits exhausted at script {idx}/{len(all_to_process)}")
            print(f"   ✅ Saved so far: {successful_saves} scripts")
            print(f"👉 Top up: https://www.perplexity.ai/settings/api")
            break

        dev_chars   = len(re.findall(r'[\u0900-\u097F]', script))
        total_ch    = len(script.replace(' ', '').replace('\n', ''))
        marathi_pct = (dev_chars / max(total_ch, 1)) * 100
        lang_tag    = "🇮🇳" if marathi_pct > 35 else "⚠️"
        cta_tag     = "✅" if is_script_complete(script) else "⚠️ NO CTA"
        print(f"   📝 {lang_tag} ({marathi_pct:.0f}%) | CTA:{cta_tag} | "
              f"🔗 {article.get('link','')[:55]}...")

        success = save_to_google_sheets(
            worksheet,
            script_type,
            cat,
            article['title'],
            script,
            article.get('link', '')
        )
        if success:
            successful_saves += 1
        else:
            failed_saves += 1

        await asyncio.sleep(1)

    total_duration = (datetime.now(IST) - start_time).total_seconds()
    total_tokens   = total_input_tokens + total_output_tokens

    print("\n" + "=" * 70)
    print("📈 FINAL SUMMARY — KALAKAR KATTA v3.0")
    print("=" * 70)
    print(f"   🔍 Analysis model     : {ANALYSIS_MODEL}")
    print(f"   ✍️  Script model       : {SCRIPT_MODEL}")
    print(f"   📰 News collected     : {len(selected_news)}")
    print(f"   📖 Stories collected  : {len(selected_stories)}")
    print(f"   ✅ Scripts saved      : {successful_saves}")
    print(f"   ❌ Failed             : {failed_saves}")
    print(f"   ⏱️  Total time         : {total_duration:.0f}s ({total_duration/60:.1f} mins)")
    print(f"   📥 Input tokens       : {total_input_tokens:,}")
    print(f"   📤 Output tokens      : {total_output_tokens:,}")
    print(f"   🔢 Total tokens       : {total_tokens:,}")
    print(f"   💰 Total cost         : ${total_cost:.4f} (~₹{total_cost*84:.2f})")
    print(f"   💵 Cost per script    : ${total_cost/max(successful_saves,1):.4f}")
    print(f"   🕐 Finished at (IST)  : {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    if worksheet:
        print(f"   📊 Sheet URL          : https://docs.google.com/spreadsheets/d/"
              f"{worksheet.spreadsheet.id}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())