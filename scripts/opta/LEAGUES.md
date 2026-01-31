# Opta League Reference

Complete reference for all leagues available in the Opta scraper.

**Last updated:** 2026-01-31
**Total leagues:** 106
**Scraped:** 106 leagues (100%)

## Data Quality Tiers

| Tier | Description | Match Events | Shot Coords | Example |
|------|-------------|--------------|-------------|---------|
| **Full** | Complete event-level data | Yes | Yes | EPL, La Liga, UCL |
| **Partial** | Recent seasons have events, older don't | 2015+ only | 2015+ only | Championship, Eredivisie |
| **Basic** | Lineups and basic stats only | No | No | Bulgarian Cup, Slovak Cup |

---

## Tier 1: Big 5 European Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Premier League | `EPL` | `2kwbbcootiqqgmrzs6o5inle5` | 114 | 1901-2026 | Full |
| La Liga | `La_Liga` | `34pl8szyvrbwcmfkuocjm3r6t` | 33 | 1993-2026 | Full |
| Bundesliga | `Bundesliga` | `6by3h89i2eykc341oz7lv1ddd` | 63 | 1963-2026 | Full |
| Serie A | `Serie_A` | `1r097lpxe0xn03ihb7wi98kao` | 34 | 1992-2026 | Full |
| Ligue 1 | `Ligue_1` | `dm5ka0os1e3dxcp3vh05kmp33` | 32 | 1994-2026 | Full |

**Scrape status:** ✓ Scraped

---

## Tier 2: Major International Tournaments

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| FIFA World Cup | `World_Cup` | `70excpe1synn9kadnbppahdn7` | 23 | 1930-2026 | Full |
| UEFA Euros | `UEFA_Euros` | `8tddm56zbasf57jkkay4kbf11` | 18 | 1960-2028 | Full |
| UEFA Nations League | `UEFA_Nations_League` | `595nsvo7ykvoe690b1e4u5n56` | 4 | 2018-2025 | Full |
| Copa America | `Copa_America` | `45db8orh1qttbsqq9hqapmbit` | 13 | 1995-2028 | Full |
| UEFA WC Qualifiers | `UEFA_WC_Qualifiers` | `39q1hq42hxjfylxb7xpe9bvf9` | 21 | 1934-2026 | Partial |
| UEFA Euro Qualifiers | `UEFA_Euro_Qualifiers` | `gfskxsdituog2kqp9yiu7bzi` | 18 | 1960-2024 | Partial |
| CONCACAF Gold Cup | `CONCACAF_Gold_Cup` | `f51991ex45qhp1p3iu74u4d4e` | 19 | 1991-2027 | Partial |
| Africa Cup of Nations | `AFCON` | `68zplepppndhl8bfdvgy9vgu1` | 16 | 1998-2027 | Partial |

**Scrape status:** ✓ Scraped

---

## Tier 3: Major Club Competitions

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| UEFA Champions League | `UCL` | `4oogyu6o156iphvdvphwpck10` | 26 | 2000-2026 | Full |
| UEFA Europa League | `UEL` | `4c1nfi2j1m731hcay25fcgndq` | 26 | 2000-2026 | Full |
| Conference League | `Conference_League` | `c7b8o53flg36wbuevfzy3lb10` | 5 | 2021-2026 | Full |
| FIFA Club World Cup | `Club_World_Cup` | `dc4k1xh2984zbypbnunk7ncic` | 2 | 2025-2029 | Full |
| UEFA Super Cup | `UEFA_Super_Cup` | `a0f4gtru0oyxmpvty4thc5qkc` | 29 | 1998-2027 | Partial |

**Scrape status:** ✓ Scraped

---

## Tier 4: Big 5 Domestic Cups

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| FA Cup | `FA_Cup` | `2hj3286pqov1g1g59k2t2qcgm` | 22 | 2004-2026 | Partial |
| EFL Cup | `League_Cup` | `725gd73msyt08xm76v7gkxj7u` | 22 | 2004-2026 | Partial |
| Copa del Rey | `Copa_del_Rey` | `apdwh753fupxheygs8seahh7x` | 22 | 2004-2026 | Partial |
| Supercopa de Espana | `Supercopa` | `sd8z02fe455z2fjvlxvxh0zo` | 23 | 2004-2027 | Partial |
| DFB Pokal | `DFB_Pokal` | `486rhdgz7yc0sygziht7hje65` | 22 | 2004-2026 | Partial |
| Coppa Italia | `Coppa_Italia` | `6694fff47wqxl10lrd9tb91f8` | 22 | 2004-2026 | Partial |
| Coupe de France | `Coupe_de_France` | `3n9mk5b2mxmq831wfmv6pu86i` | 26 | 2000-2026 | Partial |
| Trophee des Champions | `Trophee_Champions` | `1nsu863daf68kns4l7ou69orf` | 22 | 2005-2027 | Partial |

**Scrape status:** ✓ Scraped

---

## Tier 5: Secondary European Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| EFL Championship | `Championship` | `7ntvbsyq31jnzoqoa8850b9b8` | 33 | 1993-2026 | Partial |
| EFL League One | `League_One` | `3frp1zxrqulrlrnk503n6l4l` | 33 | 1993-2026 | Partial |
| EFL League Two | `League_Two` | `bgen5kjer2ytfp7lo9949t72g` | 33 | 1993-2026 | Partial |
| WSL (Women's) | `WSL` | `6vq8j5p3av14nr3iuyi4okhjt` | 16 | 2011-2026 | Partial |
| Primeira Liga | `Primeira_Liga` | `8yi6ejjd1zudcqtbn07haahg6` | 32 | 1994-2026 | Partial |
| Eredivisie | `Eredivisie` | `akmkihra9ruad09ljapsm84b3` | 33 | 1993-2026 | Partial |
| Scottish Premiership | `Scottish_Premiership` | `e21cf135btr8t3upw0vl6n6x0` | 32 | 1994-2026 | Partial |
| Super Lig | `Super_Lig` | `482ofyysbdbeoxauk19yg7tdt` | 68 | 1959-2026 | Partial |
| Belgian First Division | `Belgian_First_Division` | `4zwgbb66rif2spcoeeol2motx` | 67 | 1959-2026 | Partial |
| Swiss Super League | `Swiss_Super_League` | `e0lck99w8meo9qoalfrxgo33o` | 32 | 1994-2026 | Partial |
| Austrian Bundesliga | `Austrian_Bundesliga` | `5c96g1zm7vo5ons9c42uy2w3r` | 33 | 1993-2026 | Partial |
| Greek Super League | `Greek_Super_League` | `c0r21rtokgnbtc0o2rldjmkxu` | 32 | 1994-2026 | Partial |

**Scrape status:** ✓ Scraped

---

## Tier 6: Secondary European Domestic Cups

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Taca de Portugal | `Taca_de_Portugal` | `5jd0k2txwnq69frs79eulba8j` | 22 | 2004-2026 | Basic |
| Taca da Liga | `Taca_da_Liga` | `bqvy41un7sf86rbse9tv810x7` | 19 | 2008-2027 | Basic |
| KNVB Beker | `KNVB_Beker` | `cbdbziaqczfuyuwqsylqi26zd` | 22 | 2004-2026 | Basic |
| Scottish Cup | `Scottish_Cup` | `8kt53kt3mfo29gldhkl05u25b` | 22 | 2004-2026 | Basic |
| Scottish League Cup | `Scottish_League_Cup` | `eog6knrkfei68si736fpquyzc` | 23 | 2004-2027 | Basic |
| Turkish Cup | `Turkish_Cup` | `7af85xa75vozt2l4hzi6ryts7` | 64 | 1962-2026 | Basic |
| Belgian Cup | `Belgian_Cup` | `1qt9bfl6dhydf4tpano6n1p7s` | 22 | 2004-2026 | Basic |
| Schweizer Pokal | `Schweizer_Pokal` | `8cit3whr514nnd4zkaovsnqn` | 22 | 2004-2026 | Basic |
| Austrian Cup | `Austrian_Cup` | `1ncmha8yglhyyhg6gtaujymqf` | 21 | 2004-2026 | Basic |
| Greek Cup | `Greek_Cup` | `10x5pvhifwo4y7hs3fz9hf245` | 22 | 2004-2026 | Basic |

**Scrape status:** ✓ Scraped

---

## Tier 7: Americas Top Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| MLS | `MLS` | `287tckirbfj9nb2ar2k9r60vn` | - | - | - |
| Liga MX | `Liga_MX` | `5vwz4siguym0udhj3cr4l2sz3` | - | - | - |
| Brazilian Serie A | `Brazilian_Serie_A` | `scf9p4y91yjvqvg5jndxzhxj` | 29 | 1998-2026 | Partial |
| Argentine Liga | `Argentine_Liga_Profesional` | `ecpu6zdp8s0l2zwrx0zprpqzl` | - | - | - |
| Copa Libertadores | `CONMEBOL_Libertadores` | `86rw2b2ml7rydq74bng6pzwbo` | - | - | - |
| Copa do Brasil | `Copa_do_Brasil` | `16bnz0wt7mzzrn92p2pza2k9n` | - | - | - |

**Scrape status:** ✓ Brazilian Serie A scraped; MLS, Liga MX, Copa Libertadores not in seasons.json (no API access)

---

## Tier 8: African Competitions

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| CAF Champions League | `CAF_CL` | `cse5oqqt2pzfcy8uz6yz3tkbj` | 22 | 2005-2026 | Basic |
| CAF Confederation Cup | `CAF_Confederation_Cup` | `bx57cmq1edfq53ckfk791supi` | 20 | 2007-2026 | Basic |
| Egyptian Premier | `Egyptian_Premier_League` | `12ixvd3k8vfqf10qbfcqitgzo` | - | - | - |
| South African PSL | `South_African_PSL` | `xvddvdgd2g40p82e3ztylmrqn` | - | - | - |
| Botola Pro (Morocco) | `Botola_Pro` | `1eruend45vd20g9hbrpiggs5u` | 22 | 2004-2026 | Basic |
| Moroccan Cup | `Moroccan_Cup` | `d1d1wnseo0ao8ojqtpxbirh2b` | 17 | 2009-2026 | Basic |
| Tunisian Ligue 1 | `Tunisian_Ligue_1` | `f4jc2cc5nq7flaoptpi5ua4k4` | 22 | 2004-2026 | Basic |
| Tunisian Cup | `Tunisian_Cup` | `138n7rt9ngbmktlhtwfeefqqp` | 17 | 2008-2026 | Basic |
| Tunisian Super Cup | `Tunisian_Super_Cup` | `8wmp2ym78qoluhmxpfe0o73mc` | 7 | 2020-2027 | Basic |

**Scrape status:** ✓ All scraped (CAF CL, CAF Confed Cup, Botola Pro, Moroccan Cup, Tunisian leagues)

---

## Tier 9: Asian & Oceania Competitions

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| J1 League | `J1_League` | `ctms5njkdpwkl1lnopx1d0lxp` | - | - | - |
| K League 1 | `K_League_1` | `xoxb38hl9k1l9e0yw6xm8qe` | - | - | - |
| Saudi League | `Saudi_League` | `blmkbxq7l6dufmuzqv831y6w9` | - | - | - |
| UAE Pro League | `UAE_Pro_League` | `f39uq10c8xhg5e6rwwcf6lhgc` | 21 | 2005-2026 | Basic |
| UAE League Cup | `UAE_League_Cup` | `89v3ukjpui1gashsz3i1vphfa` | 18 | 2008-2026 | Basic |
| UAE Presidents Cup | `UAE_Presidents_Cup` | `2smaq6vx7pgwmkfkn15kp7ib` | 18 | 2008-2026 | Basic |
| Gulf Champions League | `Gulf_Champions_League` | `4gyhjrol8ycf1taamo21fvfh2` | 8 | 2010-2026 | Basic |
| AFC Champions League | `AFC_Champions_League_Elite` | `3v16kc92h6xfv10c2b19f5owj` | - | - | - |
| A-League (Australia) | `A_League` | `xwnjb1az11zffwty3m6vn8y6` | 21 | 2005-2026 | Partial |
| NZ National League | `NZ_National_League` | `1vyghvhuy6abu4htoemdi79bd` | 11 | 2016-2026 | Basic |
| OFC Champions League | `OFC_Champions_League` | `4y9msam43q5ddjdrhsvd7fo85` | 21 | 2005-2026 | Basic |
| NZ Chatham Cup | `NZ_Chatham_Cup` | `c9n0iioc66668md31jzkpmfmj` | 14 | 2012-2026 | Basic |
| FIFA Intercontinental Cup | `FIFA_Intercontinental_Cup` | `cmvff99i4w10udooqckzt8c2x` | 23 | 2000-2026 | Partial |

**Scrape status:** ✓ All available scraped (A-League, UAE, Gulf, NZ, OFC); J1, K League, Saudi, AFC CL not in seasons.json (no API access)

---

## Tier 10: Nordic Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Danish Superliga | `Danish_Superliga` | `29actv1ohj8r10kd9hu0jnb0n` | 33 | 1993-2026 | Partial |
| Eliteserien (Norway) | `Eliteserien` | `9ynnnx1qmkizq1o3qr3v0nsuk` | 33 | 1994-2026 | Partial |
| Allsvenskan (Sweden) | `Allsvenskan` | `b60nisd3qn427jm0hrg9kvmab` | 33 | 1994-2026 | Partial |
| Veikkausliiga (Finland) | `Veikkausliiga` | `dvstmwnvw0mt5p38twn9yttyb` | 33 | 1994-2026 | Basic |
| DBU Pokalen | `DBU_Pokalen` | `8ztsv3pzrsyq5w1r3a0nfk1y5` | 22 | 2004-2026 | Partial |
| Svenska Cupen | `Svenska_Cupen` | `d9eaigzyfnfiraqc3ius757tl` | 22 | 2004-2026 | Basic |
| Suomen Cup | `Suomen_Cup` | `6hlw7rhrpe9garwmfoxu4lebc` | 23 | 2004-2026 | Basic |

**Scrape status:** ✓ Scraped

---

## Tier 11: Eastern European Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Ukrainian Premier | `Ukrainian_Premier_League` | `6wubmo7di3kdpflluf6s8c7vs` | 23 | 2003-2026 | Basic |
| Ekstraklasa (Poland) | `Ekstraklasa` | `7hl0svs2hg225i2zud0g3xzp2` | 32 | 1994-2026 | Partial |
| Czech Liga | `Czech_Liga` | `bu1l7ckihyr0errxw61p0m05` | 33 | 1993-2026 | Partial |
| Romanian Liga I | `Romanian_Liga_I` | `89ovpy1rarewwzqvi30bfdr8b` | 31 | 1995-2026 | Basic |
| Serbian Super Liga | `Serbian_Super_Liga` | `3ww12jab49q8q8mk9avdwjqgk` | 20 | 2006-2026 | Basic |
| Croatian HNL | `Croatian_HNL` | `1b70m6qtxrp75b4vtk8hxh8c3` | 32 | 1994-2026 | Partial |
| NB I (Hungary) | `NB_I` | `47s2kt0e8m444ftqvsrqa3bvq` | 31 | 1995-2026 | Basic |
| Bulgarian First League | `Bulgarian_First_League` | `c0yqkbilbbg70ij2473xymmqv` | 31 | 1995-2026 | Basic |
| Slovak Liga | `Slovak_Liga` | `1mpjd0vbxbtu9zw89yj09xk3z` | 24 | 2002-2026 | Basic |
| Slovenian Liga | `Slovenian_Liga` | `7nmz249q89qg5ezcvzlheljji` | 31 | 1995-2026 | Basic |

**Scrape status:** ✓ Scraped

---

## Tier 12: Eastern European Cups

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Ukrainian Cup | `Ukrainian_Cup` | `2kuyfkulm5lsgjxynrgh3vz70` | 21 | 2004-2026 | Basic |
| Polish Cup | `Polish_Cup` | `b3ufcd24wfnnd5j98ped6irfu` | 22 | 2004-2026 | Basic |
| Czech Cup | `Czech_Cup` | `193wqkyb0v5jnsblhvd2ocmyo` | 22 | 2004-2026 | Basic |
| Cupa Romaniei | `Cupa_Romaniei` | `65q4uwm6ol1rkf5dp89m8omny` | 22 | 2004-2026 | Basic |
| Serbian Cup | `Serbian_Cup` | `29lni33vxqrl1tqhadrnfid6t` | 20 | 2006-2026 | Basic |
| Croatian Cup | `Croatian_Cup` | `3z6xfyd3ovi5x09orlo4rmskx` | 22 | 2004-2026 | Basic |
| Magyar Kupa | `Magyar_Kupa` | `chfah95whw2m0sbdq6cvfac7q` | 22 | 2004-2026 | Basic |
| Bulgarian Cup | `Bulgarian_Cup` | `22euhl6zy56cp651ipq99rooq` | 22 | 2004-2026 | Basic |
| Slovak Cup | `Slovak_Cup` | `ahl3vljaignq9ebaos4uqkrvo` | 22 | 2004-2026 | Basic |
| Slovenian Cup | `Slovenian_Cup` | `ggsjtgoapnah61wu939ni8js` | 22 | 2004-2026 | Basic |

**Scrape status:** ✓ Scraped

---

## Tier 13: Other European Leagues

| League | Code | Competition ID | Seasons | Coverage | Data |
|--------|------|----------------|---------|----------|------|
| Ligat Ha'Al (Israel) | `Ligat_Haal` | (in seasons.json) | 31 | 1995-2026 | Basic |
| Israeli Cup | `Israeli_Cup` | (in seasons.json) | 22 | 2004-2026 | Basic |
| Irish Premier | `Irish_Premier` | (in seasons.json) | 33 | 1994-2026 | Basic |
| Bosnian Premier | `Bosnian_Premier` | (in seasons.json) | 24 | 2002-2026 | Basic |
| Icelandic Premier | `Icelandic_Premier` | (in seasons.json) | 33 | 1994-2026 | Basic |
| Macedonian First | `Macedonian_First` | (in seasons.json) | 29 | 1997-2026 | Basic |
| Kosovo Superliga | `Kosovo_Superliga` | (in seasons.json) | 13 | 2013-2026 | Basic |
| Maltese Premier | `Maltese_Premier` | (in seasons.json) | 29 | 1997-2026 | Basic |
| Gibraltar Premier | `Gibraltar_Premier` | (in seasons.json) | 13 | 2013-2026 | Basic |
| Armenian Premier | `Armenian_Premier` | (in seasons.json) | 22 | 2004-2026 | Basic |
| Cyprus First | `Cyprus_First` | (in seasons.json) | 30 | 1996-2026 | Basic |
| Azerbaijan Premier | `Azerbaijan_Premier` | (in seasons.json) | 23 | 2003-2026 | Basic |
| Kazakhstan Premier | `Kazakhstan_Premier` | (in seasons.json) | 25 | 2002-2026 | Basic |
| Azerbaijan Cup | `Azerbaijan_Cup` | (in seasons.json) | 22 | 2004-2026 | Basic |

**Scrape status:** ✓ All scraped (Ligat Haal, Israeli Cup, Irish Premier, Bosnian, Icelandic, Macedonian, Kosovo, Maltese, Gibraltar, Armenian, Cyprus, Azerbaijan, Kazakhstan)

---

## Scrape Summary

### Completed (106 leagues - 100%)
- Big 5 European Leagues (5) - EPL, La Liga, Bundesliga, Serie A, Ligue 1
- Big 5 Domestic Cups (8) - FA Cup, Copa del Rey, DFB Pokal, etc.
- Major International Tournaments (8) - World Cup, Euros, Nations League, Copa America, WC/Euro Qualifiers
- Major Club Competitions (5) - UCL, UEL, Conference League, Club World Cup, Super Cup
- Secondary European Leagues (12) - Championship, Eredivisie, Primeira Liga, etc.
- Secondary European Cups (10) - KNVB Beker, Scottish Cup, Turkish Cup, etc.
- Nordic Leagues (7) - Danish, Norwegian, Swedish, Finnish leagues + cups
- Eastern European Leagues (10) - Ukrainian, Polish, Czech, Romanian, etc.
- Eastern European Cups (10) - All corresponding cups
- African Competitions (9) - CAF CL, Confederation Cup, Botola Pro, Tunisian leagues
- Middle East (4) - UAE Pro League, League Cup, Presidents Cup, Gulf Champions League
- Oceania (4) - A-League, NZ National League, OFC Champions League, NZ Chatham Cup
- Small European (14) - Bosnian, Icelandic, Macedonian, Kosovo, Malta, Gibraltar, Armenia, Cyprus, Azerbaijan, Kazakhstan

### Not Yet Discovered (~11 leagues with IDs but no seasons)
- Americas (6) - MLS, Liga MX, Argentine Liga, Copa Libertadores, Copa do Brasil
- Asian (5) - J1 League, K League 1, Saudi League, AFC Champions League Elite, Egyptian/South African leagues

These leagues have competition IDs in the scraper but seasons cannot be discovered via the tournament calendar API.

---

## Usage

```bash
# Scrape specific leagues
python scrape_big5.py --leagues EPL La_Liga --recent 5

# Scrape all available seasons
python scrape_big5.py --leagues Championship

# Force rescrape
python scrape_big5.py --leagues FA_Cup --force
```

## Notes

1. **Full data leagues** have detailed match events, shot coordinates, and pass data
2. **Partial data leagues** have events for recent seasons (~2015+) but not older ones
3. **Basic data leagues** only have lineups, goals, cards - no detailed match events
4. **404 errors on matchevent** endpoint indicate the league doesn't have event-level data for that season
5. **Season IDs** are stored in `seasons.json` - run `discover_seasons.py` to update
