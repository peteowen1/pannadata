"""
Competition metadata for Opta data catalog.

Maps Opta competition codes (from seasons.json) to human-readable metadata.
Unmapped codes get auto-generated names by humanizing underscored keys.
"""

# Mapping from seasons.json competition codes to metadata
COMPETITION_METADATA = {
    # Big 5 European Leagues
    "EPL": {"name": "English Premier League", "country": "England", "type": "league", "tier": 1},
    "La_Liga": {"name": "La Liga", "country": "Spain", "type": "league", "tier": 1},
    "Bundesliga": {"name": "Bundesliga", "country": "Germany", "type": "league", "tier": 1},
    "Serie_A": {"name": "Serie A", "country": "Italy", "type": "league", "tier": 1},
    "Ligue_1": {"name": "Ligue 1", "country": "France", "type": "league", "tier": 1},

    # Extended European Leagues
    "Eredivisie": {"name": "Eredivisie", "country": "Netherlands", "type": "league", "tier": 2},
    "Primeira_Liga": {"name": "Primeira Liga", "country": "Portugal", "type": "league", "tier": 2},
    "Super_Lig": {"name": "Super Lig", "country": "Turkey", "type": "league", "tier": 2},
    "Championship": {"name": "Championship", "country": "England", "type": "league", "tier": 2},
    "Scottish_Premiership": {"name": "Scottish Premiership", "country": "Scotland", "type": "league", "tier": 2},
    "Belgian_First_Division": {"name": "Belgian First Division", "country": "Belgium", "type": "league", "tier": 2},
    "Swiss_Super_League": {"name": "Swiss Super League", "country": "Switzerland", "type": "league", "tier": 3},
    "Austrian_Bundesliga": {"name": "Austrian Bundesliga", "country": "Austria", "type": "league", "tier": 3},
    "Danish_Superliga": {"name": "Danish Superliga", "country": "Denmark", "type": "league", "tier": 3},
    "Greek_Super_League": {"name": "Greek Super League", "country": "Greece", "type": "league", "tier": 3},
    "Croatian_HNL": {"name": "Croatian HNL", "country": "Croatia", "type": "league", "tier": 3},
    "Czech_Liga": {"name": "Czech Liga", "country": "Czech Republic", "type": "league", "tier": 3},
    "Romanian_Liga_I": {"name": "Romanian Liga I", "country": "Romania", "type": "league", "tier": 3},
    "Serbian_Super_Liga": {"name": "Serbian Super Liga", "country": "Serbia", "type": "league", "tier": 3},
    "Slovak_Liga": {"name": "Slovak Liga", "country": "Slovakia", "type": "league", "tier": 3},
    "Slovenian_Liga": {"name": "Slovenian Liga", "country": "Slovenia", "type": "league", "tier": 3},
    "NB_I": {"name": "NB I", "country": "Hungary", "type": "league", "tier": 3},
    "Ekstraklasa": {"name": "Ekstraklasa", "country": "Poland", "type": "league", "tier": 3},
    "Ukrainian_Premier_League": {"name": "Ukrainian Premier League", "country": "Ukraine", "type": "league", "tier": 3},
    "Ligat_Haal": {"name": "Ligat Ha'al", "country": "Israel", "type": "league", "tier": 3},
    "Cyprus_First": {"name": "Cyprus First Division", "country": "Cyprus", "type": "league", "tier": 4},
    "Gibraltar_Premier": {"name": "Gibraltar Premier", "country": "Gibraltar", "type": "league", "tier": 4},
    "Kosovo_Superliga": {"name": "Kosovo Superliga", "country": "Kosovo", "type": "league", "tier": 4},
    "Macedonian_First": {"name": "Macedonian First League", "country": "North Macedonia", "type": "league", "tier": 4},
    "Maltese_Premier": {"name": "Maltese Premier", "country": "Malta", "type": "league", "tier": 4},
    "Armenian_Premier": {"name": "Armenian Premier", "country": "Armenia", "type": "league", "tier": 4},
    "Azerbaijan_Premier": {"name": "Azerbaijan Premier", "country": "Azerbaijan", "type": "league", "tier": 4},
    "Bosnian_Premier": {"name": "Bosnian Premier", "country": "Bosnia", "type": "league", "tier": 4},
    "Bulgarian_First_League": {"name": "Bulgarian First League", "country": "Bulgaria", "type": "league", "tier": 4},
    "Kazakhstan_Premier": {"name": "Kazakhstan Premier", "country": "Kazakhstan", "type": "league", "tier": 4},
    "Irish_Premier": {"name": "Irish Premier Division", "country": "Ireland", "type": "league", "tier": 4},
    "Icelandic_Premier": {"name": "Icelandic Premier", "country": "Iceland", "type": "league", "tier": 4},

    # English lower divisions
    "League_One": {"name": "League One", "country": "England", "type": "league", "tier": 3},
    "League_Two": {"name": "League Two", "country": "England", "type": "league", "tier": 4},

    # Nordic leagues
    "Allsvenskan": {"name": "Allsvenskan", "country": "Sweden", "type": "league", "tier": 3},
    "Eliteserien": {"name": "Eliteserien", "country": "Norway", "type": "league", "tier": 3},
    "Veikkausliiga": {"name": "Veikkausliiga", "country": "Finland", "type": "league", "tier": 3},

    # African leagues
    "Botola_Pro": {"name": "Botola Pro", "country": "Morocco", "type": "league", "tier": 4},
    "Tunisian_Ligue_1": {"name": "Tunisian Ligue 1", "country": "Tunisia", "type": "league", "tier": 4},

    # Oceania / Americas / Middle East
    "A_League": {"name": "A-League Men", "country": "Australia", "type": "league", "tier": 3},
    "Brazilian_Serie_A": {"name": "Brazilian Serie A", "country": "Brazil", "type": "league", "tier": 2},
    "UAE_Pro_League": {"name": "UAE Pro League", "country": "UAE", "type": "league", "tier": 4},
    "NZ_National_League": {"name": "NZ National League", "country": "New Zealand", "type": "league", "tier": 4},

    # Women's leagues
    "WSL": {"name": "Women's Super League", "country": "England", "type": "league", "tier": 5},

    # UEFA Club Competitions
    "UCL": {"name": "UEFA Champions League", "country": "Europe", "type": "cup", "tier": 1},
    "UEL": {"name": "UEFA Europa League", "country": "Europe", "type": "cup", "tier": 2},
    "Conference_League": {"name": "UEFA Conference League", "country": "Europe", "type": "cup", "tier": 3},
    "UEFA_Super_Cup": {"name": "UEFA Super Cup", "country": "Europe", "type": "cup", "tier": 4},
    "Club_World_Cup": {"name": "FIFA Club World Cup", "country": "International", "type": "cup", "tier": 3},
    "FIFA_Intercontinental_Cup": {"name": "FIFA Intercontinental Cup", "country": "International", "type": "cup", "tier": 3},

    # African Club Competitions
    "CAF_CL": {"name": "CAF Champions League", "country": "Africa", "type": "cup", "tier": 3},
    "CAF_Confederation_Cup": {"name": "CAF Confederation Cup", "country": "Africa", "type": "cup", "tier": 4},

    # Other club cups
    "OFC_Champions_League": {"name": "OFC Champions League", "country": "Oceania", "type": "cup", "tier": 4},
    "Gulf_Champions_League": {"name": "Gulf Champions League", "country": "Middle East", "type": "cup", "tier": 4},

    # Domestic cups
    "FA_Cup": {"name": "FA Cup", "country": "England", "type": "domestic_cup", "tier": 2},
    "League_Cup": {"name": "League Cup", "country": "England", "type": "domestic_cup", "tier": 3},
    "DFB_Pokal": {"name": "DFB-Pokal", "country": "Germany", "type": "domestic_cup", "tier": 2},
    "Copa_del_Rey": {"name": "Copa del Rey", "country": "Spain", "type": "domestic_cup", "tier": 2},
    "Coppa_Italia": {"name": "Coppa Italia", "country": "Italy", "type": "domestic_cup", "tier": 2},
    "Coupe_de_France": {"name": "Coupe de France", "country": "France", "type": "domestic_cup", "tier": 2},
    "Taca_de_Portugal": {"name": "Taca de Portugal", "country": "Portugal", "type": "domestic_cup", "tier": 3},
    "Taca_da_Liga": {"name": "Taca da Liga", "country": "Portugal", "type": "domestic_cup", "tier": 4},
    "KNVB_Beker": {"name": "KNVB Beker", "country": "Netherlands", "type": "domestic_cup", "tier": 3},
    "Scottish_Cup": {"name": "Scottish Cup", "country": "Scotland", "type": "domestic_cup", "tier": 3},
    "Scottish_League_Cup": {"name": "Scottish League Cup", "country": "Scotland", "type": "domestic_cup", "tier": 4},
    "Turkish_Cup": {"name": "Turkish Cup", "country": "Turkey", "type": "domestic_cup", "tier": 3},
    "Austrian_Cup": {"name": "Austrian Cup", "country": "Austria", "type": "domestic_cup", "tier": 4},
    "Belgian_Cup": {"name": "Belgian Cup", "country": "Belgium", "type": "domestic_cup", "tier": 3},
    "Bulgarian_Cup": {"name": "Bulgarian Cup", "country": "Bulgaria", "type": "domestic_cup", "tier": 4},
    "Croatian_Cup": {"name": "Croatian Cup", "country": "Croatia", "type": "domestic_cup", "tier": 4},
    "Czech_Cup": {"name": "Czech Cup", "country": "Czech Republic", "type": "domestic_cup", "tier": 4},
    "Cupa_Romaniei": {"name": "Cupa Romaniei", "country": "Romania", "type": "domestic_cup", "tier": 4},
    "DBU_Pokalen": {"name": "DBU Pokalen", "country": "Denmark", "type": "domestic_cup", "tier": 4},
    "Greek_Cup": {"name": "Greek Cup", "country": "Greece", "type": "domestic_cup", "tier": 4},
    "Israeli_Cup": {"name": "Israeli Cup", "country": "Israel", "type": "domestic_cup", "tier": 4},
    "Magyar_Kupa": {"name": "Magyar Kupa", "country": "Hungary", "type": "domestic_cup", "tier": 4},
    "Moroccan_Cup": {"name": "Moroccan Cup", "country": "Morocco", "type": "domestic_cup", "tier": 4},
    "NZ_Chatham_Cup": {"name": "NZ Chatham Cup", "country": "New Zealand", "type": "domestic_cup", "tier": 4},
    "Polish_Cup": {"name": "Polish Cup", "country": "Poland", "type": "domestic_cup", "tier": 4},
    "Schweizer_Pokal": {"name": "Schweizer Pokal", "country": "Switzerland", "type": "domestic_cup", "tier": 4},
    "Serbian_Cup": {"name": "Serbian Cup", "country": "Serbia", "type": "domestic_cup", "tier": 4},
    "Slovak_Cup": {"name": "Slovak Cup", "country": "Slovakia", "type": "domestic_cup", "tier": 4},
    "Slovenian_Cup": {"name": "Slovenian Cup", "country": "Slovenia", "type": "domestic_cup", "tier": 4},
    "Suomen_Cup": {"name": "Suomen Cup", "country": "Finland", "type": "domestic_cup", "tier": 4},
    "Svenska_Cupen": {"name": "Svenska Cupen", "country": "Sweden", "type": "domestic_cup", "tier": 4},
    "Tunisian_Cup": {"name": "Tunisian Cup", "country": "Tunisia", "type": "domestic_cup", "tier": 4},
    "Tunisian_Super_Cup": {"name": "Tunisian Super Cup", "country": "Tunisia", "type": "domestic_cup", "tier": 4},
    "Ukrainian_Cup": {"name": "Ukrainian Cup", "country": "Ukraine", "type": "domestic_cup", "tier": 4},
    "Azerbaijan_Cup": {"name": "Azerbaijan Cup", "country": "Azerbaijan", "type": "domestic_cup", "tier": 4},
    "UAE_League_Cup": {"name": "UAE League Cup", "country": "UAE", "type": "domestic_cup", "tier": 4},
    "UAE_Presidents_Cup": {"name": "UAE President's Cup", "country": "UAE", "type": "domestic_cup", "tier": 4},

    # Super cups / shields
    "Supercopa": {"name": "Supercopa de Espana", "country": "Spain", "type": "domestic_cup", "tier": 4},
    "Trophee_Champions": {"name": "Trophee des Champions", "country": "France", "type": "domestic_cup", "tier": 4},

    # International Competitions
    "World_Cup": {"name": "FIFA World Cup", "country": "International", "type": "international", "tier": 1},
    "UEFA_Euros": {"name": "UEFA European Championship", "country": "Europe", "type": "international", "tier": 1},
    "Copa_America": {"name": "Copa America", "country": "South America", "type": "international", "tier": 2},
    "AFCON": {"name": "Africa Cup of Nations", "country": "Africa", "type": "international", "tier": 2},
    "CONCACAF_Gold_Cup": {"name": "CONCACAF Gold Cup", "country": "North America", "type": "international", "tier": 2},
    "UEFA_Nations_League": {"name": "UEFA Nations League", "country": "Europe", "type": "international", "tier": 3},
    "UEFA_Euro_Qualifiers": {"name": "UEFA Euro Qualifiers", "country": "Europe", "type": "international", "tier": 3},
    "UEFA_WC_Qualifiers": {"name": "UEFA World Cup Qualifiers", "country": "Europe", "type": "international", "tier": 3},
}

# Panna league code → Opta competition code (mirrors OPTA_LEAGUES in R)
PANNA_ALIASES = {
    "ENG": "EPL",
    "ESP": "La_Liga",
    "GER": "Bundesliga",
    "ITA": "Serie_A",
    "FRA": "Ligue_1",
    "NED": "Eredivisie",
    "POR": "Primeira_Liga",
    "TUR": "Super_Lig",
    "ENG2": "Championship",
    "SCO": "Scottish_Premiership",
    "UECL": "Conference_League",
    "WC": "World_Cup",
    "EURO": "UEFA_Euros",
}


def get_competition_metadata(code):
    """Get metadata for a competition code, with fallback to humanized name."""
    if code in COMPETITION_METADATA:
        return COMPETITION_METADATA[code]
    return {
        "name": code.replace("_", " "),
        "country": "Unknown",
        "type": "unknown",
        "tier": 99,
    }
