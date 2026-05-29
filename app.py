from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import os
import json
import time
import gc
import ctypes

# Linux環境でPythonがOSにメモリを返却するためのヘルパー
try:
    _libc = ctypes.cdll.LoadLibrary('libc.so.6')
    def _malloc_trim():
        _libc.malloc_trim(0)
except Exception:
    def _malloc_trim():
        pass
import threading
import requests as http_requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

WATCHLIST_FILE    = 'watchlist.json'
METADATA_FILE     = 'metadata.json'
DEFAULT_WATCHLIST = ['AAPL', 'NVDA', '7203', '1605']

# ── スキャン対象銘柄リスト & 名前辞書 ─────────────────────────────────────────
SCAN_STOCKS = [
    # 日本株
    '7203', '6758', '8306', '9984', '7974', '6861', '8035', '9433', '9432',
    '7751', '6954', '8766', '4661', '7267', '7201', '6752', '6902', '5401',
    '8411', '8316', '4568', '4523', '6367', '6301', '6326', '6501', '6503',
    '7741', '8801', '3382', '6920', '4502', '2914', '2802', '9022', '9020',
    '4519', '8604', '6702', '8002', '8031', '8058', '9843', '6098', '4755',
    '6645', '7832', '9766', '8267', '7550', '1605', '5020', '7309', '4578',
    '6178', '2413', '4704',
    # 米国株
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'JPM', 'V',
    'JNJ', 'WMT', 'PG', 'MA', 'UNH', 'HD', 'CVX', 'LLY', 'ABBV', 'MRK',
    'KO', 'PEP', 'XOM', 'BAC', 'AVGO', 'COST', 'ORCL', 'CSCO', 'AMD',
    'CRM', 'DIS', 'NFLX',
]

STOCK_NAMES = {
    # 日本株
    '7203': 'トヨタ自動車', '6758': 'ソニーグループ', '8306': '三菱UFJ FG',
    '9984': 'ソフトバンクグループ', '7974': '任天堂', '6861': 'キーエンス',
    '8035': '東京エレクトロン', '9433': 'KDDI', '9432': '日本電信電話(NTT)',
    '7751': 'キヤノン', '6954': 'ファナック', '8766': '東京海上HD',
    '4661': 'オリエンタルランド', '7267': 'ホンダ', '7201': '日産自動車',
    '6752': 'パナソニックHD', '6902': 'デンソー', '5401': '日本製鉄',
    '8411': 'みずほFG', '8316': '三井住友FG', '4568': '第一三共',
    '4523': 'エーザイ', '6367': 'ダイキン工業', '6301': 'コマツ',
    '6326': 'クボタ', '6501': '日立製作所', '6503': '三菱電機',
    '7741': 'HOYA', '8801': '三井不動産', '3382': 'セブン&アイHD',
    '6920': 'レーザーテック', '4502': '武田薬品工業', '2914': '日本たばこ産業',
    '2802': '味の素', '9022': 'JR東海', '9020': 'JR東日本',
    '4519': '中外製薬', '8604': '野村HD', '6702': '富士通',
    '8002': '丸紅', '8031': '三井物産', '8058': '三菱商事',
    '9843': 'ニトリHD', '6098': 'リクルートHD', '4755': '楽天グループ',
    '6645': 'オムロン', '7832': 'バンダイナムコHD', '9766': 'コナミグループ',
    '8267': 'イオン', '7550': 'ゼンショーHD', '1605': 'INPEX',
    '5020': 'ENEOSホールディングス', '7309': 'シマノ', '4578': '大塚HD',
    '6178': '日本郵政', '2413': 'エムスリー', '4704': 'トレンドマイクロ',
    # 米国株
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'NVDA': 'NVIDIA',
    'GOOGL': 'Alphabet (Google)', 'AMZN': 'Amazon', 'META': 'Meta',
    'TSLA': 'Tesla', 'JPM': 'JPMorgan Chase', 'V': 'Visa',
    'JNJ': 'Johnson & Johnson', 'WMT': 'Walmart', 'PG': 'Procter & Gamble',
    'MA': 'Mastercard', 'UNH': 'UnitedHealth', 'HD': 'Home Depot',
    'CVX': 'Chevron', 'LLY': 'Eli Lilly', 'ABBV': 'AbbVie',
    'MRK': 'Merck', 'KO': 'Coca-Cola', 'PEP': 'PepsiCo',
    'XOM': 'ExxonMobil', 'BAC': 'Bank of America', 'AVGO': 'Broadcom',
    'COST': 'Costco', 'ORCL': 'Oracle', 'CSCO': 'Cisco',
    'AMD': 'AMD', 'CRM': 'Salesforce', 'DIS': 'Disney', 'NFLX': 'Netflix',
}
SCAN_CACHE_TTL = 12 * 3600  # 12時間

# ── 発掘スキャン対象 ─────────────────────────────────────────────────────────
# 日本株追加分（日経225 + 主要TOPIX）SCAN_STOCKSにない銘柄
JP_DISCOVERY_STOCKS = [
    # 重工業
    '7011', '7012', '7013', '6302', '6305', '6361', '6383', '6113',
    # 電子部品/半導体
    '6981', '6762', '6723', '6857', '6971', '6963', '6770', '4062',
    '6146', '6273', '6479', '6481', '6504', '6506', '6594', '7735',
    # 電機
    '6701', '6724', '6841', '6952', '6988',
    # 化学/素材
    '4063', '4188', '4183', '4042', '3402', '4005', '4021', '4061', '4091', '4208',
    '3407', '3436', '5201', '5333', '5411', '5802', '4631', '3105', '3401', '3861',
    # 精密機器
    '4901', '7733', '7731', '4543', '7762',
    # 食品/飲料/水産
    '2503', '2502', '2801', '2269', '4452', '2282', '1332', '2002',
    # 不動産
    '8830', '3231', '3289', '8802',
    # 金融/保険/証券
    '8750', '8725', '8308', '8354', '7186', '8309', '8601', '8630', '8697',
    # 海運
    '9101', '9104', '9107',
    # 自動車/タイヤ
    '5108', '7270', '7269', '6471', '5110', '8015', '7211', '7261', '7272', '5101',
    # 医薬品
    '4507', '4527', '4536', '4151',
    # IT/サービス/エンタメ
    '4307', '9613', '2432', '4689', '4324', '6460', '7951', '3659', '1721',
    # 小売
    '9983', '2651', '3099', '8233', '3048', '9831', '8252',
    # 商社
    '8001', '8053', '2768',
    # 建設
    '1801', '1802', '1803', '1812', '1928', '1925',
    # 交通/物流
    '9005', '9021', '9064', '9201', '9202',
    # エネルギー/公共
    '5019', '5631', '5803', '9503', '9531', '9501', '9532', '9735',
]

# 米国株追加分（S&P500主要）SCAN_STOCKSにない銘柄
US_DISCOVERY_STOCKS = [
    # テクノロジー
    'INTC', 'QCOM', 'TXN', 'MU', 'LRCX', 'AMAT', 'KLAC', 'ADI', 'MRVL', 'NXPI',
    'ADBE', 'NOW', 'INTU', 'ADSK', 'CDNS', 'SNPS', 'ANSS', 'FTNT', 'CTSH',
    'HPQ', 'HPE', 'NTAP', 'MSI', 'GLW', 'APH',
    'PYPL', 'UBER', 'PLTR', 'SNOW', 'DDOG', 'ABNB',
    # 金融
    'GS', 'MS', 'C', 'WFC', 'BLK', 'AXP', 'SCHW', 'USB',
    'CME', 'ICE', 'SPGI', 'MCO', 'AON', 'MMC', 'FI', 'FIS', 'GPN',
    'STT', 'BK', 'BX', 'KKR', 'AMP',
    # ヘルスケア
    'TMO', 'DHR', 'ISRG', 'MDT', 'BSX', 'SYK', 'ABT',
    'AMGN', 'GILD', 'BMY', 'PFE', 'REGN', 'VRTX',
    'ELV', 'HCA', 'CVS', 'CI', 'HUM', 'ZTS', 'BDX', 'IDXX', 'EW', 'RMD',
    # 一般消費財
    'NKE', 'SBUX', 'MCD', 'CMG', 'TGT', 'LOW', 'TJX',
    'BKNG', 'MAR', 'HLT', 'ROST', 'ORLY', 'AZO', 'DHI', 'LEN', 'YUM', 'DRI', 'GRMN',
    # 通信/メディア
    'T', 'VZ', 'TMUS', 'CMCSA', 'CHTR', 'EA', 'TTWO',
    # 生活必需品
    'MDLZ', 'PM', 'MO', 'CL', 'GIS', 'KMB', 'STZ', 'SYY', 'ADM',
    # 産業/防衛
    'CAT', 'DE', 'HON', 'ETN', 'GE', 'RTX', 'LMT', 'NOC', 'EMR',
    'UPS', 'FDX', 'UNP', 'NSC', 'CSX', 'PH', 'ITW', 'ADP', 'WM', 'RSG',
    'MMM', 'BA', 'FAST', 'CTAS', 'OTIS', 'CARR',
    # エネルギー
    'COP', 'SLB', 'PSX', 'EOG', 'OXY', 'VLO', 'MPC', 'KMI', 'WMB', 'DVN', 'HAL',
    # 素材
    'LIN', 'APD', 'ECL', 'SHW', 'NUE', 'FCX', 'PPG', 'NEM', 'DD', 'DOW',
    # 公共
    'NEE', 'DUK', 'SO', 'AEP', 'EXC', 'XEL', 'WEC',
    # 不動産
    'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'DLR', 'WELL', 'SPG', 'O', 'VICI',
    # 自動車
    'F', 'GM',
]

JP_DISCOVERY_NAMES = {
    '7011': '三菱重工業', '7012': '川崎重工業', '7013': 'IHI',
    '6302': '住友重機械工業', '6305': '日立建機', '6361': '荏原製作所',
    '6383': 'ダイフク', '6113': 'アマダ',
    '6981': '村田製作所', '6762': 'TDK', '6723': 'ルネサスエレクトロニクス',
    '6857': 'アドバンテスト', '6971': '京セラ', '6963': 'ローム',
    '6770': 'アルプスアルパイン', '4062': 'イビデン',
    '6146': 'ディスコ', '6273': 'SMC', '6479': 'ミネベアミツミ',
    '6481': 'THK', '6504': '富士電機', '6506': '安川電機', '6594': 'ニデック', '7735': 'SCREEN HD',
    '6701': 'NEC', '6724': 'セイコーエプソン', '6841': '横河電機',
    '6952': 'カシオ計算機', '6988': '日東電工',
    '4063': '信越化学工業', '4188': '三菱ケミカルグループ', '4183': '三井化学',
    '4042': '東ソー', '3402': '東レ', '4005': '住友化学', '4021': '日産化学',
    '4061': 'デンカ', '4091': '日本酸素HD', '4208': 'UBE',
    '3407': '旭化成', '3436': 'SUMCO', '5201': 'AGC', '5333': '日本碍子',
    '5411': 'JFEホールディングス', '5802': '住友電気工業',
    '4631': 'DIC', '3105': '日清紡HD', '3401': '帝人', '3861': '王子HD',
    '4901': '富士フイルムHD', '7733': 'オリンパス', '7731': 'ニコン',
    '4543': 'テルモ', '7762': 'シチズン時計',
    '2503': 'キリンHD', '2502': 'アサヒグループHD', '2801': 'キッコーマン',
    '2269': '明治HD', '4452': '花王', '2282': '日本ハム',
    '1332': 'ニッスイ', '2002': '日清製粉グループ',
    '8830': '住友不動産', '3231': '野村不動産HD', '3289': '東急不動産HD', '8802': '三菱地所',
    '8750': '第一生命HD', '8725': 'MS&ADインシュアランス',
    '8308': 'りそなHD', '8354': 'ふくおかFG', '7186': 'コンコルディアFG',
    '8309': '三井住友トラストHD', '8601': '大和証券グループ',
    '8630': 'SOMPOホールディングス', '8697': '日本取引所グループ',
    '9101': '日本郵船', '9104': '商船三井', '9107': '川崎汽船',
    '5108': 'ブリヂストン', '7270': 'SUBARU', '7269': 'スズキ',
    '6471': 'NSK（日本精工）', '5110': '住友ゴム工業', '8015': '豊田通商',
    '7211': '三菱自動車工業', '7261': 'マツダ', '7272': 'ヤマハ発動機', '5101': '横浜ゴム',
    '4507': '塩野義製薬', '4527': 'ロート製薬', '4536': '参天製薬', '4151': '協和キリン',
    '4307': '野村総合研究所', '9613': 'NTTデータグループ',
    '2432': 'DeNA', '4689': 'LINEヤフー', '4324': '電通グループ',
    '6460': 'セガサミーHD', '7951': 'ヤマハ', '3659': 'ネクソン', '1721': 'コムシスHD',
    '9983': 'ファーストリテイリング', '2651': 'ローソン',
    '3099': '三越伊勢丹HD', '8233': '高島屋', '3048': 'ビックカメラ',
    '9831': 'ヤマダHD', '8252': '丸井グループ',
    '8001': '伊藤忠商事', '8053': '住友商事', '2768': '双日',
    '1801': '大成建設', '1802': '大林組', '1803': '清水建設',
    '1812': '鹿島建設', '1928': '積水ハウス', '1925': '大和ハウス工業',
    '9005': '東急', '9021': 'JR西日本', '9064': 'ヤマトHD',
    '9201': '日本航空（JAL）', '9202': 'ANAホールディングス',
    '5019': '出光興産', '5631': '日本製鋼所', '5803': 'フジクラ',
    '9503': '関西電力', '9531': '東京ガス', '9501': '東京電力HD',
    '9532': '大阪ガス', '9735': 'セコム',
}

US_DISCOVERY_NAMES = {
    'INTC': 'Intel', 'QCOM': 'Qualcomm', 'TXN': 'Texas Instruments', 'MU': 'Micron',
    'LRCX': 'Lam Research', 'AMAT': 'Applied Materials', 'KLAC': 'KLA Corp',
    'ADI': 'Analog Devices', 'MRVL': 'Marvell Technology', 'NXPI': 'NXP Semiconductors',
    'ADBE': 'Adobe', 'NOW': 'ServiceNow', 'INTU': 'Intuit', 'ADSK': 'Autodesk',
    'CDNS': 'Cadence Design', 'SNPS': 'Synopsys', 'ANSS': 'Ansys',
    'FTNT': 'Fortinet', 'CTSH': 'Cognizant', 'HPQ': 'HP Inc', 'HPE': 'Hewlett Packard Enterprise',
    'NTAP': 'NetApp', 'MSI': 'Motorola Solutions', 'GLW': 'Corning', 'APH': 'Amphenol',
    'PYPL': 'PayPal', 'UBER': 'Uber', 'PLTR': 'Palantir', 'SNOW': 'Snowflake',
    'DDOG': 'Datadog', 'ABNB': 'Airbnb',
    'GS': 'Goldman Sachs', 'MS': 'Morgan Stanley', 'C': 'Citigroup',
    'WFC': 'Wells Fargo', 'BLK': 'BlackRock', 'AXP': 'American Express',
    'SCHW': 'Charles Schwab', 'USB': 'U.S. Bancorp',
    'CME': 'CME Group', 'ICE': 'Intercontinental Exchange', 'SPGI': 'S&P Global',
    'MCO': "Moody's", 'AON': 'Aon', 'MMC': 'Marsh McLennan',
    'FI': 'Fiserv', 'FIS': 'Fidelity National Info', 'GPN': 'Global Payments',
    'STT': 'State Street', 'BK': 'Bank of New York Mellon', 'BX': 'Blackstone',
    'KKR': 'KKR & Co', 'AMP': 'Ameriprise Financial',
    'TMO': 'Thermo Fisher', 'DHR': 'Danaher', 'ISRG': 'Intuitive Surgical',
    'MDT': 'Medtronic', 'BSX': 'Boston Scientific', 'SYK': 'Stryker', 'ABT': 'Abbott',
    'AMGN': 'Amgen', 'GILD': 'Gilead Sciences', 'BMY': 'Bristol-Myers Squibb',
    'PFE': 'Pfizer', 'REGN': 'Regeneron', 'VRTX': 'Vertex Pharmaceuticals',
    'ELV': 'Elevance Health', 'HCA': 'HCA Healthcare', 'CVS': 'CVS Health',
    'CI': 'Cigna', 'HUM': 'Humana', 'ZTS': 'Zoetis', 'BDX': 'Becton Dickinson',
    'IDXX': 'IDEXX Laboratories', 'EW': 'Edwards Lifesciences', 'RMD': 'ResMed',
    'NKE': 'Nike', 'SBUX': 'Starbucks', 'MCD': "McDonald's", 'CMG': 'Chipotle',
    'TGT': 'Target', 'LOW': "Lowe's", 'TJX': 'TJX Companies',
    'BKNG': 'Booking Holdings', 'MAR': 'Marriott', 'HLT': 'Hilton',
    'ROST': 'Ross Stores', 'ORLY': "O'Reilly Auto", 'AZO': 'AutoZone',
    'DHI': 'D.R. Horton', 'LEN': 'Lennar', 'YUM': 'Yum! Brands',
    'DRI': 'Darden Restaurants', 'GRMN': 'Garmin',
    'T': 'AT&T', 'VZ': 'Verizon', 'TMUS': 'T-Mobile', 'CMCSA': 'Comcast',
    'CHTR': 'Charter Communications', 'EA': 'Electronic Arts', 'TTWO': 'Take-Two Interactive',
    'MDLZ': 'Mondelez', 'PM': 'Philip Morris', 'MO': 'Altria',
    'CL': 'Colgate-Palmolive', 'GIS': 'General Mills', 'KMB': 'Kimberly-Clark',
    'STZ': 'Constellation Brands', 'SYY': 'Sysco', 'ADM': 'Archer-Daniels-Midland',
    'CAT': 'Caterpillar', 'DE': 'Deere & Co', 'HON': 'Honeywell',
    'ETN': 'Eaton', 'GE': 'GE Aerospace', 'RTX': 'RTX', 'LMT': 'Lockheed Martin',
    'NOC': 'Northrop Grumman', 'EMR': 'Emerson Electric',
    'UPS': 'UPS', 'FDX': 'FedEx', 'UNP': 'Union Pacific', 'NSC': 'Norfolk Southern',
    'CSX': 'CSX', 'PH': 'Parker Hannifin', 'ITW': 'Illinois Tool Works',
    'ADP': 'ADP', 'WM': 'Waste Management', 'RSG': 'Republic Services',
    'MMM': '3M', 'BA': 'Boeing', 'FAST': 'Fastenal', 'CTAS': 'Cintas',
    'OTIS': 'Otis Worldwide', 'CARR': 'Carrier Global',
    'COP': 'ConocoPhillips', 'SLB': 'SLB', 'PSX': 'Phillips 66',
    'EOG': 'EOG Resources', 'OXY': 'Occidental Petroleum', 'VLO': 'Valero Energy',
    'MPC': 'Marathon Petroleum', 'KMI': 'Kinder Morgan', 'WMB': 'Williams Companies',
    'DVN': 'Devon Energy', 'HAL': 'Halliburton',
    'LIN': 'Linde', 'APD': 'Air Products', 'ECL': 'Ecolab', 'SHW': 'Sherwin-Williams',
    'NUE': 'Nucor', 'FCX': 'Freeport-McMoRan', 'PPG': 'PPG Industries',
    'NEM': 'Newmont', 'DD': 'DuPont', 'DOW': 'Dow',
    'NEE': 'NextEra Energy', 'DUK': 'Duke Energy', 'SO': 'Southern Company',
    'AEP': 'American Electric Power', 'EXC': 'Exelon', 'XEL': 'Xcel Energy', 'WEC': 'WEC Energy',
    'AMT': 'American Tower', 'PLD': 'Prologis', 'CCI': 'Crown Castle',
    'EQIX': 'Equinix', 'PSA': 'Public Storage', 'DLR': 'Digital Realty',
    'WELL': 'Welltower', 'SPG': 'Simon Property', 'O': 'Realty Income',
    'VICI': 'VICI Properties',
    'F': 'Ford', 'GM': 'General Motors',
}

# ── セクター分類辞書（全発掘スキャン対象 + SCAN_STOCKS）─────────────────────────
DISC_SECTORS = {
    # JP SCAN_STOCKS
    '7203':'自動車', '7267':'自動車', '7201':'自動車', '7974':'ゲーム',
    '6758':'電機', '6752':'電機', '6501':'電機', '6503':'電機', '6645':'電機',
    '6902':'自動車部品', '7751':'精密機器', '7741':'精密機器', '7309':'精密機器',
    '6954':'機械', '6861':'電子部品', '8035':'半導体', '6920':'半導体',
    '9984':'IT・通信', '9433':'IT・通信', '9432':'IT・通信', '6702':'IT・通信',
    '6178':'IT・通信', '4704':'IT・通信', '2413':'IT・通信',
    '8306':'金融', '8411':'金融', '8316':'金融', '8604':'金融', '8766':'保険',
    '4661':'娯楽', '7832':'ゲーム', '9766':'ゲーム',
    '4568':'医薬品', '4523':'医薬品', '4502':'医薬品', '4519':'医薬品', '4578':'医薬品',
    '8801':'不動産', '8267':'小売', '3382':'小売', '9843':'小売', '7550':'小売',
    '5401':'鉄鋼', '2802':'食品', '2914':'食品',
    '9022':'交通', '9020':'交通',
    '8002':'商社', '8031':'商社', '8058':'商社',
    '6098':'サービス', '4755':'IT・通信', '6301':'機械', '6326':'機械',
    '6367':'機械', '5020':'エネルギー', '1605':'エネルギー',
    # JP DISCOVERY
    '7011':'重工業', '7012':'重工業', '7013':'重工業', '6113':'機械',
    '6302':'機械', '6305':'機械', '6361':'機械', '6383':'機械',
    '6981':'電子部品', '6762':'電子部品', '6723':'半導体', '6857':'半導体',
    '6971':'電子部品', '6963':'半導体', '6770':'電子部品', '4062':'半導体',
    '6146':'半導体', '6273':'機械', '6479':'電子部品', '6481':'機械',
    '6504':'電機', '6506':'電機', '6594':'電機', '7735':'半導体',
    '6701':'電機', '6724':'精密機器', '6841':'電機', '6952':'電機', '6988':'電子部品',
    '4063':'化学', '4188':'化学', '4183':'化学', '4042':'化学', '3402':'素材',
    '4005':'化学', '4021':'化学', '4061':'化学', '4091':'化学', '4208':'化学',
    '3407':'化学', '3436':'素材', '5201':'素材', '5333':'素材',
    '5411':'鉄鋼', '5802':'素材', '4631':'化学', '3105':'素材', '3401':'素材', '3861':'素材',
    '4901':'精密機器', '7733':'精密機器', '7731':'精密機器', '4543':'医療機器', '7762':'精密機器',
    '2503':'食品', '2502':'食品', '2801':'食品', '2269':'食品',
    '4452':'日用品', '2282':'食品', '1332':'食品', '2002':'食品',
    '8830':'不動産', '3231':'不動産', '3289':'不動産', '8802':'不動産',
    '8750':'保険', '8725':'保険', '8308':'金融', '8354':'金融', '7186':'金融',
    '8309':'金融', '8601':'証券', '8630':'保険', '8697':'金融',
    '9101':'海運', '9104':'海運', '9107':'海運',
    '5108':'ゴム・タイヤ', '7270':'自動車', '7269':'自動車', '6471':'自動車部品',
    '5110':'ゴム・タイヤ', '8015':'商社', '7211':'自動車', '7261':'自動車',
    '7272':'自動車', '5101':'ゴム・タイヤ',
    '4507':'医薬品', '4527':'医薬品', '4536':'医薬品', '4151':'医薬品',
    '4307':'IT・通信', '9613':'IT・通信', '2432':'IT・通信', '4689':'IT・通信',
    '4324':'IT・通信', '6460':'ゲーム', '7951':'楽器', '3659':'IT・通信', '1721':'IT・通信',
    '9983':'小売', '2651':'小売', '3099':'小売', '8233':'小売',
    '3048':'小売', '9831':'小売', '8252':'小売',
    '8001':'商社', '8053':'商社', '2768':'商社',
    '1801':'建設', '1802':'建設', '1803':'建設', '1812':'建設',
    '1928':'建設', '1925':'建設',
    '9005':'交通', '9021':'交通', '9064':'物流', '9201':'交通', '9202':'交通',
    '5019':'エネルギー', '5631':'機械', '5803':'素材',
    '9503':'エネルギー', '9531':'エネルギー', '9501':'エネルギー',
    '9532':'エネルギー', '9735':'サービス',
    # US SCAN_STOCKS
    'AAPL':'テクノロジー', 'MSFT':'テクノロジー', 'NVDA':'テクノロジー',
    'GOOGL':'テクノロジー', 'AMZN':'一般消費財', 'META':'テクノロジー',
    'TSLA':'自動車', 'JPM':'金融', 'V':'金融', 'JNJ':'ヘルスケア',
    'WMT':'生活必需品', 'PG':'生活必需品', 'MA':'金融', 'UNH':'ヘルスケア',
    'HD':'一般消費財', 'CVX':'エネルギー', 'LLY':'ヘルスケア', 'ABBV':'ヘルスケア',
    'MRK':'ヘルスケア', 'KO':'生活必需品', 'PEP':'生活必需品', 'XOM':'エネルギー',
    'BAC':'金融', 'AVGO':'テクノロジー', 'COST':'生活必需品', 'ORCL':'テクノロジー',
    'CSCO':'テクノロジー', 'AMD':'テクノロジー', 'CRM':'テクノロジー',
    'DIS':'通信・メディア', 'NFLX':'通信・メディア',
    # US DISCOVERY
    'INTC':'テクノロジー', 'QCOM':'テクノロジー', 'TXN':'テクノロジー',
    'MU':'テクノロジー', 'LRCX':'テクノロジー', 'AMAT':'テクノロジー',
    'KLAC':'テクノロジー', 'ADI':'テクノロジー', 'MRVL':'テクノロジー',
    'NXPI':'テクノロジー', 'ADBE':'テクノロジー', 'NOW':'テクノロジー',
    'INTU':'テクノロジー', 'ADSK':'テクノロジー', 'CDNS':'テクノロジー',
    'SNPS':'テクノロジー', 'ANSS':'テクノロジー', 'FTNT':'テクノロジー',
    'CTSH':'テクノロジー', 'HPQ':'テクノロジー', 'HPE':'テクノロジー',
    'NTAP':'テクノロジー', 'MSI':'テクノロジー', 'GLW':'テクノロジー',
    'APH':'テクノロジー', 'PYPL':'金融', 'UBER':'一般消費財',
    'PLTR':'テクノロジー', 'SNOW':'テクノロジー', 'DDOG':'テクノロジー', 'ABNB':'一般消費財',
    'GS':'金融', 'MS':'金融', 'C':'金融', 'WFC':'金融', 'BLK':'金融',
    'AXP':'金融', 'SCHW':'金融', 'USB':'金融',
    'CME':'金融', 'ICE':'金融', 'SPGI':'金融', 'MCO':'金融', 'AON':'金融',
    'MMC':'金融', 'FI':'金融', 'FIS':'金融', 'GPN':'金融',
    'STT':'金融', 'BK':'金融', 'BX':'金融', 'KKR':'金融', 'AMP':'金融',
    'TMO':'ヘルスケア', 'DHR':'ヘルスケア', 'ISRG':'ヘルスケア', 'MDT':'ヘルスケア',
    'BSX':'ヘルスケア', 'SYK':'ヘルスケア', 'ABT':'ヘルスケア',
    'AMGN':'ヘルスケア', 'GILD':'ヘルスケア', 'BMY':'ヘルスケア',
    'PFE':'ヘルスケア', 'REGN':'ヘルスケア', 'VRTX':'ヘルスケア',
    'ELV':'ヘルスケア', 'HCA':'ヘルスケア', 'CVS':'ヘルスケア', 'CI':'ヘルスケア',
    'HUM':'ヘルスケア', 'ZTS':'ヘルスケア', 'BDX':'ヘルスケア', 'IDXX':'ヘルスケア',
    'EW':'ヘルスケア', 'RMD':'ヘルスケア',
    'NKE':'一般消費財', 'SBUX':'一般消費財', 'MCD':'一般消費財', 'CMG':'一般消費財',
    'TGT':'一般消費財', 'LOW':'一般消費財', 'TJX':'一般消費財',
    'BKNG':'一般消費財', 'MAR':'一般消費財', 'HLT':'一般消費財', 'ROST':'一般消費財',
    'ORLY':'一般消費財', 'AZO':'一般消費財', 'DHI':'一般消費財', 'LEN':'一般消費財',
    'YUM':'一般消費財', 'DRI':'一般消費財', 'GRMN':'テクノロジー',
    'T':'通信', 'VZ':'通信', 'TMUS':'通信', 'CMCSA':'通信',
    'CHTR':'通信', 'EA':'通信・メディア', 'TTWO':'通信・メディア',
    'MDLZ':'生活必需品', 'PM':'生活必需品', 'MO':'生活必需品', 'CL':'生活必需品',
    'GIS':'生活必需品', 'KMB':'生活必需品', 'STZ':'生活必需品', 'SYY':'生活必需品',
    'ADM':'生活必需品',
    'CAT':'産業', 'DE':'産業', 'HON':'産業', 'ETN':'産業', 'GE':'産業',
    'RTX':'産業', 'LMT':'産業', 'NOC':'産業', 'EMR':'産業',
    'UPS':'産業', 'FDX':'産業', 'UNP':'産業', 'NSC':'産業', 'CSX':'産業',
    'PH':'産業', 'ITW':'産業', 'ADP':'産業', 'WM':'産業', 'RSG':'産業',
    'MMM':'産業', 'BA':'産業', 'FAST':'産業', 'CTAS':'産業',
    'OTIS':'産業', 'CARR':'産業',
    'COP':'エネルギー', 'SLB':'エネルギー', 'PSX':'エネルギー', 'EOG':'エネルギー',
    'OXY':'エネルギー', 'VLO':'エネルギー', 'MPC':'エネルギー', 'KMI':'エネルギー',
    'WMB':'エネルギー', 'DVN':'エネルギー', 'HAL':'エネルギー',
    'LIN':'素材', 'APD':'素材', 'ECL':'素材', 'SHW':'素材', 'NUE':'素材',
    'FCX':'素材', 'PPG':'素材', 'NEM':'素材', 'DD':'素材', 'DOW':'素材',
    'NEE':'公共', 'DUK':'公共', 'SO':'公共', 'AEP':'公共', 'EXC':'公共',
    'XEL':'公共', 'WEC':'公共',
    'AMT':'不動産', 'PLD':'不動産', 'CCI':'不動産', 'EQIX':'不動産',
    'PSA':'不動産', 'DLR':'不動産', 'WELL':'不動産', 'SPG':'不動産',
    'O':'不動産', 'VICI':'不動産',
    'F':'自動車', 'GM':'自動車',
}

SUPABASE_URL       = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY       = os.environ.get('SUPABASE_KEY', '')
GEMINI_API_KEY     = os.environ.get('GEMINI_API_KEY', '')

# ── AI解説キャッシュ（永続化 + メモリキャッシュ）────────────────────────────
AI_COMMENTS_FILE = 'ai_comments.json'
_AI_CACHE_TTL    = 14 * 24 * 3600   # 2週間


def _load_ai_comments() -> dict:
    """ai_comments.json または Supabase から読み込む（古いエントリは除外）"""
    data: dict = {}
    if SUPABASE_URL and SUPABASE_KEY:
        sb = _sb_load('ai_comments')
        if isinstance(sb, dict):
            data = sb
    if not data:
        try:
            with open(AI_COMMENTS_FILE, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
    # 期限切れエントリを除去してから返す
    now = time.time()
    return {k: v for k, v in data.items()
            if isinstance(v, dict) and now - v.get('ts', 0) < _AI_CACHE_TTL}


def _save_ai_comments(cache: dict) -> None:
    """ai_comments.json と Supabase に保存"""
    try:
        with open(AI_COMMENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[ai_comments] save error: {e}')
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('ai_comments', cache)


# 起動時にキャッシュをファイルから読み込む（Supabase/_sb_load使用のため後で初期化）
_ai_cache: dict = {}


def _sb_headers():
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    }


def _sb_load(key):
    """Supabaseのsettingsテーブルからvalueを取得する"""
    try:
        res = http_requests.get(
            f'{SUPABASE_URL}/rest/v1/settings?key=eq.{key}&select=value',
            headers=_sb_headers(), timeout=5
        )
        data = res.json()
        if data:
            return data[0]['value']
    except Exception as e:
        print(f'Supabase load error ({key}): {e}')
    return None


def _sb_save(key, value):
    """Supabaseのsettingsテーブルにvalueをupsertする"""
    try:
        headers = _sb_headers()
        headers['Prefer'] = 'resolution=merge-duplicates'
        http_requests.post(
            f'{SUPABASE_URL}/rest/v1/settings',
            json={'key': key, 'value': value},
            headers=headers, timeout=5
        )
        return True
    except Exception as e:
        print(f'Supabase save error ({key}): {e}')
    return False


def load_watchlist():
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('watchlist')
        if data is not None:
            return data
    # ローカル fallback
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_WATCHLIST.copy()


def save_watchlist(wl):
    if SUPABASE_URL and SUPABASE_KEY:
        if _sb_save('watchlist', wl):
            return
    with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False)


def load_metadata():
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('metadata')
        if data is not None:
            return data
    # ローカル fallback
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_metadata(meta):
    if SUPABASE_URL and SUPABASE_KEY:
        if _sb_save('metadata', meta):
            return
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def enrich_with_metadata(result: dict) -> dict:
    entry = load_metadata().get(result['ticker'], {})
    result['custom_name'] = entry.get('custom_name', '')
    result['memo']        = entry.get('memo', '')
    return result


def normalize_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if t.endswith('.T'):
        return t
    if t.isdigit():
        return t + '.T'
    return t


def display_ticker(symbol: str) -> str:
    return symbol[:-2] if symbol.endswith('.T') else symbol


def strip_tz(index):
    try:
        return index.tz_convert(None)
    except TypeError:
        return index


def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = prices.ewm(span=fast, min_periods=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, min_periods=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, min_periods=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist


def get_signal(macd, sig):
    valid_m = macd.dropna()
    valid_s = sig.dropna()
    if len(valid_m) < 2 or len(valid_s) < 2:
        return '様子見', 'neutral'

    prev_m, curr_m = float(valid_m.iloc[-2]), float(valid_m.iloc[-1])
    prev_s, curr_s = float(valid_s.iloc[-2]), float(valid_s.iloc[-1])

    if prev_m <= prev_s and curr_m > curr_s:
        return '買い', 'buy'
    if prev_m >= prev_s and curr_m < curr_s:
        return '売り', 'sell'
    if curr_m > curr_s:
        return '上昇トレンド', 'bullish'
    if curr_m < curr_s:
        return '下降トレンド', 'bearish'
    return '様子見', 'neutral'


def scan_stock_data(ticker: str) -> dict:
    """スキャン専用の軽量取得（stock.infoをスキップして高速化）"""
    symbol = normalize_ticker(ticker)
    stock  = yf.Ticker(symbol)

    start_str = (datetime.now() - timedelta(days=365 * 10 + 90)).strftime('%Y-%m-%d')
    raw = stock.history(start=start_str, interval='1wk')
    if raw.empty:
        raise ValueError(f'データが見つかりません: {symbol}')

    hist = raw.resample('MS').agg(
        Close=('Close', 'last'),
    ).dropna(subset=['Close'])

    disp      = display_ticker(symbol)
    is_jp     = symbol.endswith('.T')
    currency  = 'JPY' if is_jp else 'USD'
    curr_price = float(hist['Close'].iloc[-1])
    prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else curr_price

    # fast_info だけ使う（info より大幅に速い）
    try:
        fi = stock.fast_info
        currency   = getattr(fi, 'currency', currency) or currency
        lp = getattr(fi, 'last_price', None)
        pc = getattr(fi, 'previous_close', None)
        if lp: curr_price = float(lp)
        if pc: prev_close = float(pc)
    except Exception:
        pass

    change     = curr_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    close = hist['Close']
    macd, sig, _ = calculate_macd(close)
    signal_text, signal_type = get_signal(macd, sig)

    last_gc = last_dc = None
    m_vals = macd.dropna()
    s_vals = sig.reindex(m_vals.index).dropna()
    common = m_vals.index.intersection(s_vals.index)
    for i in range(1, len(common)):
        pm, cm = float(m_vals[common[i-1]]), float(m_vals[common[i]])
        ps, cs = float(s_vals[common[i-1]]), float(s_vals[common[i]])
        if pm <= ps and cm > cs:
            last_gc = common[i].strftime('%Y-%m')
        elif pm >= ps and cm < cs:
            last_dc = common[i].strftime('%Y-%m')

    # ミニチャート用: 直近18ヶ月のデータ
    chart_n = min(18, len(hist))
    chart_close = [round(float(v), 4) for v in hist['Close'].iloc[-chart_n:].tolist()]
    macd_hist_full = (macd - sig).fillna(0)
    chart_macd_hist = [round(float(v), 6) for v in macd_hist_full.iloc[-chart_n:].tolist()]

    return {
        'ticker':          disp,
        'symbol':          symbol,
        'name':            STOCK_NAMES.get(disp, disp),
        'currency':        currency,
        'current_price':   curr_price,
        'change':          change,
        'change_pct':      change_pct,
        'signal':          signal_text,
        'signal_type':     signal_type,
        'last_gc':         last_gc,
        'last_dc':         last_dc,
        'custom_name':     '',
        'memo':            '',
        'chart_close':     chart_close,
        'chart_macd_hist': chart_macd_hist,
        'next_earnings':   None,  # _run_scan_thread で順次取得
    }


def fetch_stock_data(ticker: str) -> dict:
    symbol = normalize_ticker(ticker)
    stock = yf.Ticker(symbol)

    # Yahoo Finance caps monthly interval at ~7 years; fetch weekly and resample
    start_str = (datetime.now() - timedelta(days=365 * 10 + 90)).strftime('%Y-%m-%d')
    raw = stock.history(start=start_str, interval='1wk')
    if raw.empty:
        raise ValueError(f'データが見つかりません: {symbol}')

    hist = raw.resample('MS').agg(
        Open=('Open', 'first'),
        High=('High', 'max'),
        Low=('Low', 'min'),
        Close=('Close', 'last'),
        Volume=('Volume', 'sum'),
    ).dropna(subset=['Close'])

    name = symbol
    currency = 'USD'
    current_price = float(hist['Close'].iloc[-1])
    prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current_price

    try:
        info = stock.info or {}
        name = info.get('longName') or info.get('shortName') or symbol
        currency = info.get('currency') or currency
        current_price = float(
            info.get('currentPrice') or info.get('regularMarketPrice') or current_price
        )
        prev_close = float(info.get('previousClose') or prev_close)
    except Exception:
        try:
            fi = stock.fast_info
            currency = getattr(fi, 'currency', currency) or currency
            current_price = float(getattr(fi, 'last_price', None) or current_price)
            prev_close = float(getattr(fi, 'previous_close', None) or prev_close)
        except Exception:
            pass

    change = current_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    close = hist['Close']
    macd, sig, hist_vals = calculate_macd(close)
    signal_text, signal_type = get_signal(macd, sig)

    idx = strip_tz(hist.index)
    all_dates = idx.strftime('%Y-%m').tolist()

    def _last(series):
        d = series.dropna()
        return round(float(d.iloc[-1]), 6) if not d.empty else 0.0

    def padded(series):
        """Align series with all_dates; use None where NaN (MACD warmup)."""
        return [None if pd.isna(v) else round(float(v), 6) for v in series]

    # ゴールデンクロス・デッドクロスの発生日を検出
    last_gc = last_dc = None
    m_vals = macd.dropna()
    s_vals = sig.reindex(m_vals.index).dropna()
    common = m_vals.index.intersection(s_vals.index)
    for i in range(1, len(common)):
        pm, cm = float(m_vals[common[i-1]]), float(m_vals[common[i]])
        ps, cs = float(s_vals[common[i-1]]), float(s_vals[common[i]])
        if pm <= ps and cm > cs:
            last_gc = common[i].strftime('%Y-%m')
        elif pm >= ps and cm < cs:
            last_dc = common[i].strftime('%Y-%m')

    return {
        'ticker': display_ticker(symbol),
        'symbol': symbol,
        'name': name,
        'currency': currency,
        'current_price': current_price,
        'change': change,
        'change_pct': change_pct,
        'signal': signal_text,
        'signal_type': signal_type,
        'macd_value': _last(macd),
        'signal_value': _last(sig),
        'histogram_value': _last(hist_vals),
        'last_gc': last_gc,
        'last_dc': last_dc,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'chart': {
            'dates':     all_dates,
            'close':     [round(float(x), 2) for x in hist['Close']],
            'macd':      padded(macd),
            'signal':    padded(sig),
            'histogram': padded(hist_vals),
        },
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/watchlist', methods=['GET'])
def get_watchlist():
    return jsonify({'watchlist': load_watchlist()})


@app.route('/api/watchlist/all')
def get_all_stocks():
    """Fetch all watchlist stocks in parallel and return together."""
    wl = load_watchlist()
    if not wl:
        return jsonify({'watchlist': [], 'data': {}, 'errors': {}})

    def safe_fetch(ticker):
        try:
            return ticker, fetch_stock_data(ticker), None
        except Exception as e:
            return ticker, None, str(e)

    data, errors = {}, {}
    with ThreadPoolExecutor(max_workers=min(len(wl), 6)) as ex:
        for ticker, result, err in ex.map(safe_fetch, wl):
            if err:
                errors[ticker] = err
            else:
                data[ticker] = enrich_with_metadata(result)

    return jsonify({'watchlist': wl, 'data': data, 'errors': errors})


@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    body = request.get_json(silent=True) or {}
    raw = body.get('ticker', '').strip().upper()
    if not raw:
        return jsonify({'error': 'ティッカーを入力してください'}), 400

    disp = display_ticker(normalize_ticker(raw))
    wl = load_watchlist()
    if disp in wl:
        return jsonify({'error': 'すでにリストにあります'}), 400

    try:
        h = yf.Ticker(normalize_ticker(disp)).history(period='5d', interval='1d')
        if h.empty:
            return jsonify({'error': f'銘柄が見つかりません: {raw}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    wl.append(disp)
    save_watchlist(wl)
    meta = load_metadata()
    auto_memo = str(body.get('memo', '')).strip()
    if disp not in meta:
        meta[disp] = {'custom_name': '', 'memo': auto_memo}
        save_metadata(meta)
    elif auto_memo and not meta[disp].get('memo'):
        meta[disp]['memo'] = auto_memo
        save_metadata(meta)
    return jsonify({'success': True, 'watchlist': wl})


@app.route('/api/watchlist/order', methods=['PUT'])
def reorder_watchlist():
    body = request.get_json(silent=True) or {}
    new_order = body.get('order', [])
    wl = load_watchlist()
    if not isinstance(new_order, list) or set(new_order) != set(wl):
        return jsonify({'error': '銘柄リストが一致しません'}), 400
    save_watchlist(new_order)
    return jsonify({'success': True, 'watchlist': new_order})


@app.route('/api/watchlist/<ticker>', methods=['DELETE'])
def remove_from_watchlist(ticker):
    t = ticker.upper()
    wl = load_watchlist()
    if t not in wl:
        return jsonify({'error': '見つかりません'}), 404
    wl.remove(t)
    save_watchlist(wl)
    return jsonify({'success': True, 'watchlist': wl})


@app.route('/api/metadata', methods=['GET'])
def get_metadata():
    return jsonify(load_metadata())


@app.route('/api/metadata/<ticker>', methods=['PUT'])
def update_metadata(ticker):
    body  = request.get_json(silent=True) or {}
    disp  = display_ticker(normalize_ticker(ticker))
    meta  = load_metadata()
    entry = meta.get(disp, {})
    if 'custom_name' in body:
        entry['custom_name'] = str(body['custom_name']).strip()
    if 'memo' in body:
        entry['memo'] = str(body['memo'])
    elif 'memo_if_empty' in body and not entry.get('memo'):
        entry['memo'] = str(body['memo_if_empty'])
    meta[disp] = entry
    save_metadata(meta)
    return jsonify({'success': True, 'ticker': disp, **entry})


@app.route('/api/ai-comment', methods=['POST'])
def ai_comment():
    """ウォッチリスト銘柄のAI解説を生成（Google Gemini API 無料利用）"""
    if not GEMINI_API_KEY:
        return jsonify({'error': (
            'GEMINI_API_KEY が設定されていません。\n'
            'https://aistudio.google.com/app/apikey で無料取得し、'
            'Render の Environment Variables に GEMINI_API_KEY として登録してください。'
        )}), 503

    body   = request.get_json(silent=True) or {}
    ticker = body.get('ticker', '').strip()
    if not ticker:
        return jsonify({'error': 'ticker が指定されていません'}), 400

    # キャッシュ確認（1時間）
    cached = _ai_cache.get(ticker)
    if cached and time.time() - cached['ts'] < _AI_CACHE_TTL:
        return jsonify({'comment': cached['text'], 'cached': True})

    # プロンプト構築
    name         = body.get('name', ticker)
    signal       = body.get('signal', '')
    macd_val     = body.get('macd_value', 0)
    sig_val      = body.get('signal_value', 0)
    hist_val     = body.get('histogram_value', 0)
    price        = body.get('current_price', 0)
    change_pct   = body.get('change_pct', 0)
    currency     = body.get('currency', 'USD')
    last_gc      = body.get('last_gc') or 'なし'
    last_dc      = body.get('last_dc') or 'なし'
    is_jp        = (currency == 'JPY')
    market_label = '日本株' if is_jp else '米国株'
    price_str    = f'{price:,.0f}円' if is_jp else f'${price:.2f}'

    prompt = f"""あなたは株式市場の専門アナリストです。以下の月足MACDデータを基に、この銘柄の現在の状況と直近の動向を日本語で3〜4文で簡潔に解説してください。
投資家にとってわかりやすく、具体的な数値や日付を交えて説明してください。

【銘柄情報】
銘柄名: {name}
ティッカー: {ticker}（{market_label}）
現在値: {price_str}（前日比 {change_pct:+.2f}%）

【月足MACDシグナル】
現在のシグナル: {signal}
MACD値: {macd_val:+.4f}
シグナル線: {sig_val:.4f}
ヒストグラム: {hist_val:+.4f}
最終ゴールデンクロス（GC）: {last_gc}
最終デッドクロス（DC）: {last_dc}

月足MACDは長期トレンドを示す指標です。上記を踏まえ、トレンドの強さ・方向性・注目すべきポイントを解説してください。"""

    # 利用可能なモデルを順番に試す（404なら次のモデルへ）
    _GEMINI_MODELS = [
        'gemini-2.0-flash-lite',
        'gemini-2.0-flash',
        'gemini-1.5-flash-latest',
        'gemini-1.5-flash-8b',
        'gemini-1.5-flash',
    ]
    payload = {
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'maxOutputTokens': 600, 'temperature': 0.7},
    }
    last_err = 'モデルが見つかりませんでした'
    for model in _GEMINI_MODELS:
        url = (
            'https://generativelanguage.googleapis.com/v1beta/models/'
            f'{model}:generateContent?key={GEMINI_API_KEY}'
        )
        try:
            resp = http_requests.post(url, json=payload, timeout=30)
        except Exception as e:
            return jsonify({'error': f'通信エラー: {e}'}), 502

        if resp.status_code == 200:
            text = resp.json()['candidates'][0]['content']['parts'][0]['text']
            _ai_cache[ticker] = {'text': text, 'ts': time.time()}
            _save_ai_comments(_ai_cache)
            return jsonify({'comment': text})
        elif resp.status_code == 404:
            last_err = f'モデル {model} は利用不可'
            continue   # 次のモデルを試す
        elif resp.status_code == 429:
            return jsonify({'error': (
                'APIの利用制限に達しました（1分15回・1日1,500回まで）。\n'
                'しばらく待ってから再度お試しください。'
            )}), 429
        else:
            return jsonify({'error': f'Gemini API エラー ({resp.status_code}): {resp.text[:200]}'}), 502

    return jsonify({'error': f'利用可能なGeminiモデルが見つかりません: {last_err}'}), 502


# 発掘スキャン名前辞書をSTOCK_NAMESにマージ
for _names in (JP_DISCOVERY_NAMES, US_DISCOVERY_NAMES):
    for _k, _v in _names.items():
        if _k not in STOCK_NAMES:
            STOCK_NAMES[_k] = _v

# ── スキャン バックグラウンドスレッド管理 ─────────────────────────────────────
_scan_lock  = threading.Lock()
_scan_state = {'status': 'idle', 'results': None, 'updated_at': 0, 'phase': '',
               'total': 0, 'done': 0, 'current': '', 'current_name': '',
               'earn_total': 0, 'earn_done': 0, 'earn_current': '', 'earn_current_name': '',
               'start_time': 0}

# ── 発掘スキャン スレッド管理（JP/US共用）────────────────────────────────────
def _empty_disc_state():
    return {'status': 'idle', 'results': None, 'updated_at': 0,
            'total': 0, 'done': 0, 'current': '', 'current_name': '', 'start_time': 0}

_disc_lock  = threading.Lock()
_disc_state = {'jp': _empty_disc_state(), 'us': _empty_disc_state()}

def load_auto_scan_list():
    """Supabaseから自動取得済み値上がり銘柄リストを返す (stocks, updated_at)"""
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('auto_scan_list')
        if isinstance(data, dict):
            return data.get('stocks', []), data.get('updated_at', 0)
    return [], 0


def save_auto_scan_list(stocks):
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('auto_scan_list', {'stocks': stocks, 'updated_at': time.time()})


def fetch_next_earnings(symbol: str) -> str | None:
    """yfinance calendar から次回決算日を取得（認証を内部処理・軽量）"""
    try:
        stock = yf.Ticker(symbol)
        cal = stock.calendar
        if not cal:
            return None
        dates = cal.get('Earnings Date', [])
        if not isinstance(dates, list):
            dates = [dates] if dates else []
        today = datetime.now().date()
        for d in sorted(dates):
            try:
                # datetime.date オブジェクトの場合
                if hasattr(d, 'strftime'):
                    if d > today:
                        return d.strftime('%Y-%m-%d')
                else:
                    ts = pd.Timestamp(d)
                    if ts.tzinfo:
                        ts = ts.tz_convert(None)
                    if ts.date() > today:
                        return ts.strftime('%Y-%m-%d')
            except Exception:
                pass
        return None
    except Exception:
        return None


def fetch_top_gainers(limit=100):
    """Yahoo Finance スクリーナーで値上がり上位銘柄を取得（JP + US）"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json',
    }
    results = []
    seen = set()
    for region in ['JP', 'US']:
        try:
            r = http_requests.get(
                'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved',
                params={
                    'scrIds': 'day_gainers',
                    'count': 60,
                    'region': region,
                    'formatted': 'false',
                    'corsDomain': 'finance.yahoo.com',
                },
                headers=headers,
                timeout=15,
            )
            quotes = r.json()['finance']['result'][0]['quotes']
            for q in quotes:
                sym = q.get('symbol', '')
                if not sym:
                    continue
                disp = display_ticker(sym)
                if disp not in seen:
                    seen.add(disp)
                    results.append(disp)
        except Exception as e:
            print(f'fetch_top_gainers [{region}] error: {e}')
    return results[:limit]


def load_scan_list():
    """デフォルト + 自動取得 + 手動追加 を重複排除してマージ"""
    custom = []
    auto_stocks = []
    if SUPABASE_URL and SUPABASE_KEY:
        d = _sb_load('scan_list')
        if isinstance(d, list):
            custom = d
        auto_data, _ = load_auto_scan_list()
        if isinstance(auto_data, list):
            auto_stocks = auto_data

    default_disps = set(display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS)
    seen = set(default_disps)

    extra_auto = []
    for t in auto_stocks:
        if t not in seen:
            seen.add(t)
            extra_auto.append(t)

    extra_custom = []
    for t in custom:
        if t not in seen:
            seen.add(t)
            extra_custom.append(t)

    return SCAN_STOCKS + extra_auto + extra_custom, custom


def save_scan_list(custom):
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_list', custom)


def _invalidate_scan_cache():
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': [], 'updated_at': 0})
    with _scan_lock:
        _scan_state['status'] = 'idle'


def _is_recent_gc(last_gc, months=6):
    """last_gc (YYYY-MM形式) が直近N ヶ月以内かどうか判定"""
    if not last_gc:
        return False
    try:
        y, m = map(int, last_gc.split('-'))
        now = datetime.now()
        diff = (now.year - y) * 12 + (now.month - m)
        return 0 <= diff <= months
    except Exception:
        return False


def _run_discovery_thread(market: str):
    """発掘スキャン: JP or US の全対象銘柄（SCAN_STOCKS含む）をスキャンし3種に分類して返す"""
    if market == 'jp':
        # SCAN_STOCKS の日本株（数字ティッカー）＋ JP_DISCOVERY_STOCKS（重複排除）
        base_tickers = [t for t in SCAN_STOCKS if t.isdigit()]
        seen = set(base_tickers)
        for t in JP_DISCOVERY_STOCKS:
            dt = display_ticker(normalize_ticker(t))
            if dt not in seen:
                base_tickers.append(t)
                seen.add(dt)
        targets    = base_tickers
        disc_names = JP_DISCOVERY_NAMES
    else:
        # SCAN_STOCKS の米国株（英字ティッカー）＋ US_DISCOVERY_STOCKS（重複排除）
        base_tickers = [t for t in SCAN_STOCKS if not t.isdigit()]
        seen = set(base_tickers)
        for t in US_DISCOVERY_STOCKS:
            if t not in seen:
                base_tickers.append(t)
                seen.add(t)
        targets    = base_tickers
        disc_names = US_DISCOVERY_NAMES

    total = len(targets)

    with _disc_lock:
        _disc_state[market].update({
            'status': 'running', 'total': total, 'done': 0,
            'current': '', 'current_name': '', 'start_time': time.time(),
        })

    all_results = []
    _counter    = [0]

    def safe(t):
        name = STOCK_NAMES.get(t) or disc_names.get(t, t)
        with _disc_lock:
            _disc_state[market]['current']      = t
            _disc_state[market]['current_name'] = name
        try:
            r = scan_stock_data(t)
            if r:
                # 発掘銘柄で STOCK_NAMES 未登録の場合は disc_names を使う
                if r.get('name') == r.get('ticker'):
                    r['name'] = disc_names.get(r['ticker'], r['ticker'])
                # セクター付与
                r['sector'] = DISC_SECTORS.get(r['ticker'], '')
        except Exception:
            r = None
        with _disc_lock:
            _counter[0] += 1
            _disc_state[market]['done'] = _counter[0]
        return r

    with ThreadPoolExecutor(max_workers=12) as ex:
        for data in ex.map(safe, targets):
            if data:
                all_results.append(data)

    # メタデータ（memo）を発掘結果に反映
    meta = load_metadata()
    for r in all_results:
        entry = meta.get(r['ticker'], {})
        if entry.get('custom_name'):
            r['custom_name'] = entry['custom_name']
        r['memo'] = entry.get('memo', '')

    # 3種に分類
    buy, uptrend, recent_gc = [], [], []
    for r in all_results:
        st = r.get('signal_type', '')
        if st == 'buy':
            buy.append(r)
        elif st == 'uptrend':
            uptrend.append(r)
        elif _is_recent_gc(r.get('last_gc'), months=6):
            recent_gc.append(r)

    # 買い/上昇トレンドはセクター順→ティッカー順にソート
    def sector_key(r):
        return (r.get('sector', ''), r.get('ticker', ''))
    buy.sort(key=sector_key)
    uptrend.sort(key=sector_key)

    # 直近GCは新しい順にソート
    def gc_key(r):
        return r.get('last_gc') or ''
    recent_gc.sort(key=gc_key, reverse=True)

    results = {'buy': buy, 'uptrend': uptrend, 'recent_gc': recent_gc}
    buy_cnt = len(buy) + len(uptrend)
    print(f'[discovery:{market}] 完了: 買い/上昇={buy_cnt}, 直近GC={len(recent_gc)} / {total}銘柄')

    updated_at = time.time()
    with _disc_lock:
        _disc_state[market].update({
            'status': 'done', 'results': results,
            'updated_at': updated_at, 'done': total,
        })


@app.route('/api/discovery', methods=['GET', 'POST'])
def discovery():
    """発掘スキャン API (market=jp|us)"""
    market = request.args.get('market', 'jp')
    if market not in ('jp', 'us'):
        return jsonify({'error': 'market は jp か us を指定してください'}), 400

    if request.method == 'POST':
        with _disc_lock:
            state = dict(_disc_state[market])
        if state['status'] == 'running':
            return jsonify({'status': 'running'})
        with _disc_lock:
            _disc_state[market] = _empty_disc_state()
            _disc_state[market]['status'] = 'running'
        t = threading.Thread(target=_run_discovery_thread, args=(market,), daemon=True)
        t.start()
        return jsonify({'status': 'running'})

    # GET: 現在の状態を返す
    with _disc_lock:
        state = dict(_disc_state[market])

    if state['status'] == 'done':
        res = state.get('results') or {}
        return jsonify({
            'status':     'done',
            'buy':        res.get('buy',       []),
            'uptrend':    res.get('uptrend',    []),
            'recent_gc':  res.get('recent_gc',  []),
            'total':      state.get('total',     0),
            'updated_at': state.get('updated_at',0),
        })
    if state['status'] == 'running':
        return jsonify({
            'status':       'running',
            'total':        state.get('total',        0),
            'done':         state.get('done',         0),
            'current':      state.get('current',      ''),
            'current_name': state.get('current_name', ''),
            'start_time':   state.get('start_time',   0),
        })
    return jsonify({'status': 'idle'})


def _run_scan_thread():
    start_time = time.time()
    with _scan_lock:
        _scan_state.update({'phase': 'prices', 'start_time': start_time,
                            'done': 0, 'total': 0, 'current': '', 'current_name': '',
                            'earn_done': 0, 'earn_total': 0,
                            'earn_current': '', 'earn_current_name': ''})

    all_stocks, _ = load_scan_list()
    total = len(all_stocks)
    with _scan_lock:
        _scan_state['total'] = total

    results = []
    _counter = [0]   # mutable counter shared across worker threads

    def safe(t):
        name = STOCK_NAMES.get(t, t)
        with _scan_lock:
            _scan_state['current']      = t
            _scan_state['current_name'] = name
        try:
            r = scan_stock_data(t)
        except Exception:
            r = None
        with _scan_lock:
            _counter[0] += 1
            _scan_state['done'] = _counter[0]
        return r

    with ThreadPoolExecutor(max_workers=12) as ex:
        for data in ex.map(safe, all_stocks):
            if data:
                results.append(data)

    # メタデータ（memo/custom_name）をスキャン結果に反映
    meta = load_metadata()
    for r in results:
        entry = meta.get(r['ticker'], {})
        if entry.get('custom_name'):
            r['custom_name'] = entry['custom_name']
        r['memo'] = entry.get('memo', '')

    # ── フェーズ1完了: 価格/MACDデータを先に保存 ──────────────────────────────
    prices_at = time.time()
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': results, 'updated_at': prices_at})
    with _scan_lock:
        _scan_state.update({'status': 'done', 'phase': 'prices_done',
                            'results': list(results), 'updated_at': prices_at,
                            'done': total})

    # ── フェーズ2: 決算日を順次取得 ───────────────────────────────────────────
    with _scan_lock:
        _scan_state['phase'] = 'cooldown'
    time.sleep(90)

    EARN_RANK = {'買い': 0, '上昇トレンド': 1, '様子見': 2, '下降トレンド': 3, '売り': 4}
    earnings_targets = sorted(results, key=lambda r: EARN_RANK.get(r.get('signal', ''), 5))[:60]
    earn_total = len(earnings_targets)
    print(f'[scan] 決算日取得対象: {earn_total}/{len(results)}銘柄')

    with _scan_lock:
        _scan_state.update({'phase': 'earnings', 'earn_total': earn_total,
                            'earn_done': 0, 'earn_current': '', 'earn_current_name': ''})

    earnings_ok = 0
    for i, r in enumerate(earnings_targets):
        with _scan_lock:
            _scan_state['earn_done']         = i
            _scan_state['earn_current']      = r['ticker']
            _scan_state['earn_current_name'] = STOCK_NAMES.get(r['ticker'], r['ticker'])
        try:
            e = fetch_next_earnings(normalize_ticker(r['ticker']))
            r['next_earnings'] = e
            if e:
                earnings_ok += 1
        except Exception:
            pass
        gc.collect()
        _malloc_trim()
        time.sleep(2)

    print(f'[scan] 決算日取得: {earnings_ok}/{earn_total}件')
    updated_at = time.time()
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': results, 'updated_at': updated_at})
    with _scan_lock:
        _scan_state.update({'status': 'done', 'phase': 'done',
                            'results': results, 'updated_at': updated_at,
                            'earn_done': earn_total})


@app.route('/api/scan')
def scan_signals():
    """スキャン状態を返す（バックグラウンドで実行してポーリング方式）"""
    force = request.args.get('force', 'false') == 'true'

    # Supabaseキャッシュ確認
    if not force and SUPABASE_URL and SUPABASE_KEY:
        cached = _sb_load('scan_cache')
        if cached and time.time() - cached.get('updated_at', 0) < SCAN_CACHE_TTL:
            with _scan_lock:
                _scan_state.update({'status': 'done',
                                    'results': cached['results'],
                                    'updated_at': cached['updated_at']})
            return jsonify({'status': 'done',
                            'results': cached['results'],
                            'updated_at': cached['updated_at']})

    # メモリ上の状態確認
    with _scan_lock:
        state = dict(_scan_state)

    if state['status'] == 'done' and not force:
        return jsonify({'status': 'done',
                        'results': state['results'],
                        'updated_at': state['updated_at']})

    # バックグラウンドスレッド起動（まだ動いていない場合）
    if state['status'] != 'running':
        with _scan_lock:
            _scan_state['status'] = 'running'
        # force scan時はSupabaseキャッシュを即座に無効化（古い結果が表示されないよう）
        if force and SUPABASE_URL and SUPABASE_KEY:
            _sb_save('scan_cache', {'results': [], 'updated_at': 0})
        t = threading.Thread(target=_run_scan_thread, daemon=True)
        t.start()

    return jsonify({
        'status':           'running',
        'phase':            state.get('phase', ''),
        'total':            state.get('total', 0),
        'done':             state.get('done', 0),
        'current':          state.get('current', ''),
        'current_name':     state.get('current_name', ''),
        'earn_total':       state.get('earn_total', 0),
        'earn_done':        state.get('earn_done', 0),
        'earn_current':     state.get('earn_current', ''),
        'earn_current_name':state.get('earn_current_name', ''),
        'start_time':       state.get('start_time', 0),
    })


@app.route('/api/scan-list', methods=['GET'])
def get_scan_list():
    all_stocks, custom = load_scan_list()
    auto_stocks, auto_updated_at = load_auto_scan_list()
    return jsonify({
        'default_count':   len(SCAN_STOCKS),
        'custom':          custom,
        'auto':            auto_stocks,
        'auto_updated_at': auto_updated_at,
        'total':           len(all_stocks),
    })


@app.route('/api/auto-scan-list/refresh', methods=['POST'])
def refresh_auto_scan_list():
    """値上がり上位を取得してスキャンリストを自動更新"""
    # 除外セット：ウォッチリスト + デフォルト + 手動追加
    wl = load_watchlist()
    wl_set = set(wl)
    default_set = set(display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS)
    _, custom = load_scan_list()
    custom_set = set(custom)

    gainers = fetch_top_gainers(limit=150)

    # フィルタリング（ウォッチリスト・デフォルト・手動追加は除外）
    filtered = [
        t for t in gainers
        if t not in wl_set and t not in default_set and t not in custom_set
    ][:100]

    save_auto_scan_list(filtered)
    _invalidate_scan_cache()

    auto_stocks, auto_updated_at = load_auto_scan_list()
    all_stocks, _ = load_scan_list()
    return jsonify({
        'success':         True,
        'auto':            filtered,
        'auto_updated_at': auto_updated_at,
        'count':           len(filtered),
        'total':           len(all_stocks),
    })


@app.route('/api/scan-list', methods=['POST'])
def add_to_scan_list():
    body = request.get_json(silent=True) or {}
    raw  = body.get('ticker', '').strip().upper()
    if not raw:
        return jsonify({'error': 'ティッカーを入力してください'}), 400

    disp = display_ticker(normalize_ticker(raw))
    default_disps = [display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS]
    _, custom = load_scan_list()

    if disp in default_disps or disp in custom:
        return jsonify({'error': 'すでにスキャンリストに含まれています'}), 400

    try:
        h = yf.Ticker(normalize_ticker(disp)).history(period='5d', interval='1d')
        if h.empty:
            return jsonify({'error': f'銘柄が見つかりません: {raw}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    custom.append(disp)
    save_scan_list(custom)
    _invalidate_scan_cache()

    return jsonify({'success': True, 'custom': custom, 'total': len(SCAN_STOCKS) + len(custom)})


@app.route('/api/scan-list/<ticker>', methods=['DELETE'])
def remove_from_scan_list(ticker):
    disp = display_ticker(normalize_ticker(ticker.upper()))
    _, custom = load_scan_list()
    if disp not in custom:
        return jsonify({'error': 'ユーザー追加銘柄にしか削除できません'}), 400
    custom.remove(disp)
    save_scan_list(custom)
    _invalidate_scan_cache()
    return jsonify({'success': True, 'custom': custom, 'total': len(SCAN_STOCKS) + len(custom)})


@app.route('/api/search')
def search_ticker():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'results': []})
    try:
        res = http_requests.get(
            'https://query1.finance.yahoo.com/v1/finance/search',
            params={
                'q': q,
                'quotesCount': 8,
                'newsCount': 0,
                'enableFuzzyQuery': 'false',
                'quotesQueryId': 'tss_match_phrase_query',
            },
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
            timeout=5
        )
        quotes = res.json().get('quotes', [])
        results = [
            {
                'symbol':   item.get('symbol', ''),
                'name':     item.get('longname') or item.get('shortname') or item.get('symbol', ''),
                'exchange': item.get('exchange', ''),
            }
            for item in quotes
            if item.get('quoteType') in ('EQUITY', 'ETF')
        ]
        return jsonify({'results': results})
    except Exception as e:
        return jsonify({'results': [], 'error': str(e)})


@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        return jsonify(enrich_with_metadata(fetch_stock_data(ticker)))
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'データ取得エラー: {e}'}), 500



@app.route('/api/debug/earnings/<ticker>')
def debug_one_earnings(ticker):
    """一時デバッグ: calendar パース確認"""
    symbol = normalize_ticker(ticker)
    result = {'symbol': symbol}
    try:
        stock = yf.Ticker(symbol)
        cal = stock.calendar
        result['cal_type']  = str(type(cal))
        result['cal_bool']  = bool(cal) if cal is not None else None
        result['cal_keys']  = list(cal.keys()) if isinstance(cal, dict) else None
        dates = cal.get('Earnings Date', []) if isinstance(cal, dict) else []
        result['dates_raw'] = str(dates)
        today = datetime.now().date()
        result['today'] = str(today)
        parsed = []
        for d in (dates if isinstance(dates, list) else [dates]):
            try:
                if hasattr(d, 'strftime'):
                    parsed.append({'val': str(d), 'future': d > today})
                else:
                    ts = pd.Timestamp(d)
                    parsed.append({'val': str(ts), 'future': ts.date() > today})
            except Exception as pe:
                parsed.append({'error': str(pe)})
        result['parsed_dates'] = parsed
        result['next_earnings'] = fetch_next_earnings(symbol)
    except Exception as e:
        result['error'] = str(e)
    return jsonify(result)


@app.route('/api/debug/scan-status')
def debug_scan_status():
    """一時デバッグ: スキャン進行フェーズとキャッシュ内の決算日を確認"""
    with _scan_lock:
        state = dict(_scan_state)

    earnings_sample = []
    earnings_count = 0
    if state.get('results'):
        for r in state['results'][:5]:
            earnings_sample.append({'ticker': r['ticker'], 'next_earnings': r.get('next_earnings')})
        earnings_count = sum(1 for r in state['results'] if r.get('next_earnings'))

    # Supabaseキャッシュも確認
    sb_earnings_count = 0
    sb_updated_at = None
    if SUPABASE_URL and SUPABASE_KEY:
        cached = _sb_load('scan_cache')
        if cached and cached.get('results'):
            sb_earnings_count = sum(1 for r in cached['results'] if r.get('next_earnings'))
            sb_updated_at = cached.get('updated_at')

    return jsonify({
        'scan_status':      state.get('status'),
        'scan_phase':       state.get('phase'),
        'memory_total':     len(state['results']) if state.get('results') else 0,
        'memory_earnings':  earnings_count,
        'memory_sample':    earnings_sample,
        'supabase_earnings': sb_earnings_count,
        'supabase_updated_at': sb_updated_at,
    })


# ── 起動時初期化 ──────────────────────────────────────────────────────────────
_ai_cache.update(_load_ai_comments())
print(f'[startup] AI解説キャッシュ読み込み: {len(_ai_cache)}件')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port, threaded=True)
