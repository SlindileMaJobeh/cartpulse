"""Static reference data for the simulated store (a fictional SA online shop)."""

# province -> (weight, cities)
PROVINCES = {
    "Gauteng": (34, ["Johannesburg", "Pretoria", "Soweto", "Centurion", "Midrand", "Benoni"]),
    "Western Cape": (17, ["Cape Town", "Stellenbosch", "Paarl", "George"]),
    "KwaZulu-Natal": (17, ["Durban", "Pietermaritzburg", "Umhlanga", "Newcastle"]),
    "Eastern Cape": (8, ["Gqeberha", "East London", "Mthatha"]),
    "Free State": (5, ["Bloemfontein", "Welkom"]),
    "Limpopo": (6, ["Polokwane", "Thohoyandou"]),
    "Mpumalanga": (6, ["Mbombela", "Emalahleni"]),
    "North West": (5, ["Rustenburg", "Mahikeng"]),
    "Northern Cape": (2, ["Kimberley", "Upington"]),
}

FIRST_NAMES = ["Thabo", "Lerato", "Sipho", "Naledi", "Johan", "Anika", "Pieter", "Zanele", "Kagiso", "Ayesha",
               "Mohammed", "Priya", "Lwazi", "Nomvula", "David", "Sarah", "Tshepo", "Busisiwe", "Ruan", "Chloe",
               "Themba", "Refilwe", "Kyle", "Megan", "Musa", "Palesa", "Ravi", "Fatima", "Bongani", "Ntombi"]
LAST_NAMES = ["Nkosi", "Dlamini", "van der Merwe", "Botha", "Naidoo", "Mokoena", "Pillay", "Khumalo", "Smith",
              "Mahlangu", "Pretorius", "Ndlovu", "Jacobs", "Molefe", "Govender", "Zulu", "Coetzee", "Sithole",
              "Adams", "Mthembu", "Petersen", "Maseko", "Fourie", "Baloyi"]

# category -> list of (product name, typical price in ZAR)
PRODUCTS = {
    "Electronics": [("Wireless Earbuds", 899), ("Bluetooth Speaker", 1299), ("Smartwatch", 3499),
                    ("65W USB-C Charger", 449), ("Power Bank 20000mAh", 599), ("Gaming Mouse", 699),
                    ("Mechanical Keyboard", 1499), ("27\" Monitor", 3999), ("Webcam 1080p", 799),
                    ("Wi-Fi Router", 1199), ("Solar Inverter Battery Pack", 8999)],
    "Home & Kitchen": [("Air Fryer 5L", 1499), ("Kettle 1.7L", 399), ("Coffee Plunger", 299),
                       ("Rechargeable LED Lantern", 349), ("Gas Braai Grid", 549), ("Bedding Set Queen", 899),
                       ("Cast Iron Potjie No.3", 799), ("Knife Set", 649), ("Storage Containers 10pc", 249)],
    "Fashion": [("Running Sneakers", 1199), ("Denim Jacket", 899), ("Cotton T-Shirt 3-Pack", 299),
                ("Leather Belt", 349), ("Winter Beanie", 149), ("Shweshwe Dress", 749), ("Rain Jacket", 999)],
    "Beauty": [("Shea Butter Body Lotion", 129), ("Sunscreen SPF50", 199), ("Hair Clippers", 599),
               ("Perfume 50ml", 899), ("Rooibos Face Mask", 179), ("Beard Oil", 159)],
    "Groceries": [("Rooibos Tea 80s", 59), ("Biltong 250g", 149), ("Coffee Beans 1kg", 329),
                  ("Peri-Peri Sauce", 49), ("Rusks 500g", 69), ("Olive Oil 1L", 179), ("Mielie Meal 10kg", 129)],
    "Books & Stationery": [("Notebook A5 3-Pack", 99), ("Fountain Pen", 249), ("Cookbook: Braai Classics", 349),
                           ("Novel: Paperback", 229), ("Kids' Colouring Set", 149)],
    "Sports & Outdoors": [("Yoga Mat", 349), ("Dumbbell Set 20kg", 1299), ("Camping Chair", 499),
                          ("Hiking Backpack 40L", 1099), ("Rugby Ball", 299), ("Cooler Box 40L", 899),
                          ("Bicycle Helmet", 599)],
}

DEVICES = {"mobile": 68, "desktop": 26, "tablet": 6}
TRAFFIC_SOURCES = {"organic_search": 34, "direct": 22, "social": 18, "paid_search": 14, "email": 7, "referral": 5}
# How likely a session from each source is to buy (multiplier on the base funnel).
SOURCE_INTENT = {"organic_search": 1.0, "direct": 1.3, "social": 0.6, "paid_search": 1.1, "email": 1.5, "referral": 0.9}

PAYMENT_METHODS = {"CARD": 70, "EFT": 20, "CASH_ON_DELIVERY": 10}
COURIERS = ["SwiftRoute", "KasiExpress", "CapeLink Couriers"]
# Typical delivery days by courier (mean); further provinces add a day.
COURIER_SPEED = {"SwiftRoute": 1.5, "KasiExpress": 2.2, "CapeLink Couriers": 2.9}
REMOTE_PROVINCES = {"Northern Cape", "Limpopo", "North West", "Eastern Cape"}

# South African public holidays 2026 (used to shape historic demand and as the
# offline fallback for the holidays REST API).
SA_HOLIDAYS_2026 = {
    "2026-01-01": "New Year's Day", "2026-03-21": "Human Rights Day", "2026-04-03": "Good Friday",
    "2026-04-06": "Family Day", "2026-04-27": "Freedom Day", "2026-05-01": "Workers' Day",
    "2026-06-16": "Youth Day", "2026-08-09": "National Women's Day", "2026-08-10": "National Women's Day (observed)",
    "2026-09-24": "Heritage Day", "2026-12-16": "Day of Reconciliation", "2026-12-25": "Christmas Day",
    "2026-12-26": "Day of Goodwill",
}
