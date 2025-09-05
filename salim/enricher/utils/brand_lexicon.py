BRAND_ALIASES = {
    # ——— Retail private labels / chains ———
    "קרפור": {"קרפור", "carrefour", "קרפור קלאסיק", "קרפור קלאסיק", "קרפור אוריגינל", "קרפור אוריג'ינל"},
    "שופרסל": {"שופרסל", "shufersal", "שופרסל גרין", "שופרסל green", "סלקט", "select"},
    "ויקטורי": {"ויקטורי", "victory"},
    "רמי לוי": {"רמי לוי", "rami levi", "רמי-לוי"},
    "טיב טעם": {"טיב טעם", "tiv taam", "tiv-taam"},
    "יינות ביתן": {"יינות ביתן", "yainot bitan"},
    "פרשמרקט": {"פרשמרקט", "freshmarket", "fresh market"},
    "מחסני השוק": {"מחסני השוק", "mahseney hashuk"},
    "מגה": {"מגה", "mega", "you", "יו"},
    "बीג शופרסל": {"big shufersal"},  # guard for odd OCR

    # ——— Israeli staples ———
    "תנובה": {"תנובה", "tnuva"},
    "שטראוס": {"שטראוס", "strauss"},
    "עלית": {"עלית", "elite"},
    "אוסם": {"אוסם", "אסם", "osem"},
    "סוגת": {"סוגת", "sugat"},
    "יטבתה": {"יטבתה", "yotvata"},
    "טרה": {"טרה", "tara"},
    "יד מרדכי": {"יד מרדכי", "yad mordechai"},
    "מחלבות גד": {"גד", "gád", "gad", "מחלבות גד"},
    "המשק": {"המשק", "hamashk"},
    "שטראוס מחלבות": {"שטראוס מחלבות", "strauss dairy"},
    "השחר העולה": {"השחר העולה", "השחר", "hashahar", "hashachar", "hashahar ha'ole", "hashachar ha'ole"},
    "טירת צבי": {"טירת צבי", "tirat zvi"},
    "זוגלובק": {"זוגלובק", "zoglowek", "soglowek", "זוגלוייק"},
    "יחיעם": {"יחיעם", "yechiam"},
    "מאמא עוף": {"מאמא עוף", "mama of", "mamaof"},
    "עוף טוב": {"עוף טוב", "of tov", "oftov"},
    "סנפרוסט": {"סנפרוסט", "sanfrost", "sunfrost"},
    "תפוגן": {"תפוגן", "tivall", "tapugen"},  # (historic/alt forms)
    "ויליגר": {"ויליגר", "williger", "viliger"},
    "דורות": {"דורות", "dorot"},
    "שטיבל": {"שטיבל", "shtibel", "stibel"},
    "מימון": {"מימון", "mimon", "תבליני מימון", "mimon spices"},
    "מיה": {"מיה", "maya", "מיה מזון", "maya foods"},
    "אחוה": {"אחוה", "achva", "achva halva"},
    "אל ארז": {"אל ארז", "el arz", "el-arez"},
    "הר ברכה": {"הר ברכה", "har bracha"},
    "תומר": {"תומר", "tomer"},
    "שמרית": {"שמרית", "shimrit"},
    "השף הלבן": {"השף הלבן", "hashf halavan", "hashef halavan"},
    "אחלה": {"אחלה", "achla"},
    "צבר": {"צבר", "tzabar", "sabar"},
    "מעדני מיקי": {"מעדני מיקי", "mikey salads", "miki"},
    "שלושת האופים": {"שלושת האופים", "3 האופים", "three bakers"},
    "אסם פרפקטו": {"פרפקטו", "osem perfecto", "perfecto"},
    "ריצפז": {"ריצפז", "ritzpaz"},
    "כיף": {"כיף", "kef", "careline kef"},  # כיף (body care)

    # ——— Dairy & yogurt ———
    "דנונה": {"דנונה", "danone"},
    "מולר": {"מולר", "muller", "müller"},
    "יופלה": {"יופלה", "yoplait"},
    "אקטימל": {"אקטימל", "actimel"},
    "אקטיביה": {"אקטיביה", "activia"},
    "טילון": {"טילון", "tilon"},  # Strauss ice-cream line
    "בן & ג'ריס": {"בן & ג'ריס", "בן אנד ג'ריס", "ben & jerry's", "ben and jerrys"},
    "מגנום": {"מגנום", "magnum"},
    "האגן דאז": {"האגן דאז", "häagen-dazs", "haagen dazs", "haagen-dazs"},

    # ——— Drinks ———
    "קוקה קולה": {"קוקה קולה", "coca cola", "coca-cola", "קולה", "coke"},
    "פפסי": {"פפסי", "pepsi"},
    "ספרייט": {"ספרייט", "sprite"},
    "פאנטה": {"פאנטה", "fanta"},
    "7אפ": {"7אפ", "7up", "seven up"},
    "טמפו": {"טמפו", "tempo"},  # also tissue company, but keep
    "קריסטל": {"קריסטל", "crystal"},
    "תפוזינה": {"תפוזינה", "tapuzina"},
    "שוופס": {"שוופס", "schweppes", "שוופ'ס"},
    "מי עדן": {"מי עדן", "may eden", "mey eden"},
    "נביעות": {"נביעות", "neviot"},
    "סאן פלגרינו": {"סן פלגרינו", "סאן פלגרינו", "san pellegrino"},
    "אוויאן": {"אוויאן", "evian"},
    "פרייה": {"פרייה", "peria", "perrier"},
    "בלו": {"בלו", "blue", "blue energy"},
    "מאסטר קפה": {"מאסטר קפה", "master cafe", "master coffee"},

    # ——— Coffee & tea ———
    "נסקפה": {"נסקפה", "nescafe", "טסטר צ'ויס", "taster's choice", "taster choice"},
    "עלית קפה": {"עלית קפה", "elite coffee"},
    "לאוואצה": {"לאוואצה", "lavazza"},
    "אילי": {"אילי", "illy"},
    "יגובס": {"יגובס", "jacobs"},
    "סגפרדו": {"סגפרדו", "segafredo"},
    "טוויינינגס": {"טוויינינגס", "twinings"},
    "ליפטון": {"ליפטון", "lipton"},
    "טטלי": {"טטלי", "tetley"},

    # ——— Snacks, sweets & spreads ———
    "נוטלה": {"נוטלה", "nutella"},
    "קינדר": {"קינדר", "kinder"},
    "לואקר": {"לואקר", "loacker", "loacker"},
    "לינדט": {"לינדט", "lindt"},
    "טובלרון": {"טובלרון", "toblerone"},
    "מילקה": {"מילקה", "milka"},
    "מרס": {"מרס", "mars"},
    "סניקרס": {"סניקרס", "snickers"},
    "אם אנד אם": {"אם אנד אם", "m&m", "m and m", "m&ms", "m&m's"},
    "אורביט": {"אורביט", "orbit", "orbit gum"},
    "מנטוס": {"מנטוס", "mentos"},
    "וורטרס אוריג'ינל": {"וורטרס", "werther's", "werthers original"},
    "לוטוס": {"לוטוס", "lotus", "biscoff"},
    "דוריטוס": {"דוריטוס", "doritos"},
    "פרינגלס": {"פרינגלס", "pringles"},
    "צ'יטוס": {"צ'יטוס", "cheetos"},
    "לייז": {"לייז", "lays", "walkers"},
    "במבה": {"במבה", "bamba"},
    "ביסלי": {"ביסלי", "bisli", "bissli"},

    # ——— Baking, pasta, rice, condiments ———
    "ברילה": {"ברילה", "barilla"},
    "דה צ'קו": {"דה צ'קו", "de cecco", "de-cecco"},
    "רומו": {"רומו", "rummo"},
    "מאסטר שף": {"מאסטר שף", "master chef"},
    "היינץ": {"היינץ", "heinz"},
    "הלמנס": {"הלמנס", "hellmann's", "hellmans"},
    "טבסקו": {"טבסקו", "tabasco"},
    "קיקומן": {"קיקומן", "kikkoman"},
    "ליה אנד פרינס": {"lea & perrins", "lea and perrins", "ליאה אנד פרינס"},
    "מוטי": {"מוטי", "mutti"},
    "פרגו": {"פרגו", "prego"},
    "דולמיו": {"דולמיו", "dolmio"},

    # ——— Cleaning & home ———
    "סנו": {"סנו", "sano", "סנו סושי", "sano sushi", "סנו פוליווקס", "polivax"},
    "ניקול": {"ניקול", "nicole"},
    "בדין": {"בדין", "badin", "badin baby", "בייבי בדין"},
    "אסטוניש": {"אסטוניש", "astonish"},
    "רייד": {"רייד", "raid"},
    "גלייד": {"גלייד", "glade"},
    "פיירי": {"פיירי", "fairy"},
    "אריאל": {"אריאל", "ariel"},
    "פיניש": {"פיניש", "finish"},
    "וניש": {"וניש", "vanish"},
    "פרסיל": {"פרסיל", "persil"},
    "לנור": {"לנור", "lenor"},
    "וילדה": {"וילדה", "vileda"},  # cleaning tools
    "סולתם": {"סולתם", "soltam"},
    "ארקוסטיל": {"ארקוסטיל", "arcosteel"},
    "נעמן": {"נעמן", "naaman"},
    "טפאל": {"טפאל", "tefal"},
    "פיירקס": {"פיירקס", "pyrex"},

    # ——— Oral & personal care ———
    "דאב": {"דאב", "dove"},
    "אקס": {"אקס", "axe"},
    "רקסונה": {"רקסונה", "rexona"},
    "ניוואה": {"ניוואה", "nivea"},
    "גרנייה": {"גרנייה", "garnier"},
    "לוריאל": {"לוריאל", "l'oreal", "loreal"},
    "מייבלין": {"מייבלין", "maybelline"},
    "פנטן": {"פנטן", "pantene"},
    "הד אנד שולדרס": {"head & shoulders", "head and shoulders", "הד אנד שולדרס"},
    "הרבל אסנסס": {"הרבל אסנסס", "herbal essences"},
    "ג'ילט": {"ג'ילט", "gillette"},
    "אלוויז": {"אלוויז", "always"},
    "אורל בי": {"אוראל בי", "oral-b", "oral b"},
    "קולגייט": {"קולגייט", "colgate"},
    "סנסודיין": {"סנסודיין", "sensodyne"},
    "אלמקס": {"אלמקס", "elmex"},
    "ויט": {"ויט", "veet"},
    "ג'ונסונס": {"ג'ונסונס", "johnson's", "johnsons", "johnson & johnson"},

    # ——— Baby care & formula ———
    "האגיס": {"האגיס", "huggies"},
    "פמפרס": {"פמפרס", "pampers"},
    "סימילאק": {"סימילאק", "similac"},
    "מטרנה": {"מטרנה", "materna"},

    # ——— Cereals ———
    "פיטנס": {"פיטנס", "fitness"},
    "צ'יריוס": {"צ'יריוס", "cheerios", "cheeri0s"},
    "קורנפלקס קלוגס": {"קלוגס", "kellogg's", "kelloggs", "kornaflaex"},
    "פוסט": {"פוסט", "post", "post cereals"},
    "פבלס": {"קוקו פבלס", "cocoa pebbles", "coco pebbles", "פרוטי פבלס", "fruity pebbles"},

    # ——— Alcohol & beer ———
    "גולדסטאר": {"גולדסטאר", "goldstar"},
    "מכבי": {"מכבי", "macabi", "maccabee"},
    "טובורג": {"טובורג", "tuborg"},
    "קרלסברג": {"קרלסברג", "carlsberg"},
    "הייניקן": {"הייניקן", "heineken"},
    "סטלה ארטואה": {"סטלה ארטואה", "stella artois"},
    "קורונה": {"קורונה", "corona"},
    "גינס": {"גינס", "guinness"},
    "מרפיס": {"מרפיס", "murphy's", "murphys"},
    "וויינשטפן": {"ויינשטפן", "weihenstephan", "weihenstephaner"},
    "פרנציסקנר": {"פרנציסקנר", "franziskaner"},
    "פאולנר": {"פאולנר", "paulaner"},
    "ארדינגר": {"ארדינגר", "erdinger"},
    "הוגרדן": {"הוגארדן", "hoegaarden"},
    "לף": {"לף", "leffe"},
    "ג'יימסון": {"ג'יימסון", "jameson"},
    "ג'ק דניאלס": {"ג'ק דניאלס", "jack daniels", "jd"},
    "אבסולוט": {"אבסולוט", "absolut"},
    "סמירנוף": {"סמירנוף", "smirnoff"},
    "גריי גוס": {"גריי גוס", "grey goose"},
    "רמי מרטן": {"רמי מרטן", "remy martin"},
    "לימונצ'לו די קאפרי": {"לימונצ'לו", "limoncello", "limoncello di capri"},
    "פסטיס": {"פסטיס", "pastis", "ricard"},

    # ——— Wine (Israel) ———
    "כרמל": {"כרמל", "carmel"},
    "יקבי רמת הגולן": {"יקבי רמת הגולן", "golan heights winery"},
    "ברקן": {"ברקן", "barkan"},
    "טפרברג": {"טפרברג", "teperberg", "teperburg"},
    "תשבי": {"תשבי", "tishbi"},
    "דלתון": {"דלתון", "dalton"},
    "רקאנטי": {"רקאנטי", "recanati"},
    "הרי גליל": {"הרי גליל", "galil mountain"},
    "בנימינה": {"בנימינה", "binyamina"},
    "סגל": {"סגל", "segal"},
    "ירדן": {"ירדן", "yarden"},
    "יקב היוצר": {"היוצר", "hayotzer", "yotzer"},
    "היינן": {"היינן", "heinän", "heinen"},  # seen as "היינן פרייבט" variants

    # ——— Pet ———
    "פריסקיז": {"פריסקיז", "friskies"},
    "וויסקאס": {"וויסקאס", "whiskas"},
    "פורינה": {"פורינה", "purina"},
    "פדיגרי": {"פדיגרי", "pedigree"},

    # ——— House & disposables ———
    "אורביטול": {"אורביטול", "orbitol"},
    "סנו סושי": {"סנו סושי", "sano sushi"},  # kept also under סנו
    "ספידה": {"ספידה", "spido", "speedo cleaning"},  # guard for OCR of “ספוגית”
    "וייט": {"ווייט", "white", "white line"},  # generic white brands in tissues (optional)

    # ——— Misc. brands seen in your sample ———
    "פילסבורי": {"פילסבורי", "pillsbury", "pilsbury"},
    "ריצפז": {"ריצפז", "ritzpaz"},
    "אוליביה": {"אוליביה", "olivia"},
    "דגש": {"דגש", "degash"},
    "קמיל בלו": {"קמיל בלו", "kamil blue", "camel blue"},  # common OCR miss
    "וילדה": {"וילדה", "vileda"},
    "נדיָה'ס": {"נדיא'ס", "נדיה'ס", "nadia's", "nadias"},  # Nadia's vegan
    "ויליגר": {"ויליגר", "williger"},
    "אסטוניש": {"אסטוניש", "astonish"},
    "וורטרס": {"וורטרס", "werther's", "werthers"},
    "שסטוביץ": {"שסטוביץ", "shastovitz", "shastowitz"},  # importer occasionally printed as brand
    "סולתם": {"סולתם", "soltam"},
    "סולתם-תבור": {"סולתם תבור", "soltam tavor"},
    "סולתם-ריביירה": {"סולתם ריביירה", "soltam riviera"},
    "סולתם-פרובנס": {"סולתם פרובנס", "soltam provence"},  # lines sometimes appear as brand
    "וילדה INOX": {"vileda inox", "וילדה inox"},

    # ——— Bakery & breads ———
    "ברמן": {"ברמן", "berman"},
    "אנג'ל": {"אנג'ל", "angel"},

    # ——— Foreign pantry often in IL ———
    "ברטולי": {"ברטולי", "bertolli"},
    "בארילה": {"בארילה", "barilla"},  # guard dup for OCR
    "קנור": {"קנור", "knorr"},
    "מאגי": {"מאגי", "maggi"},
    "היינץ": {"היינץ", "heinz"},  # repeat ok; set-union by loader
    "פסטה זארה": {"פסטה זארה", "pasta zara"},
    "סיסקו": {"סיסקו", "saco", "sacos"},  # placeholder for common import labels

    # ——— Health & cosmetics extras ———
    "ניוטרוג'ינה": {"ניוטרוג'ינה", "neutrogena"},
    "לנסינו": {"לנסינו", "lansinoh"},
    "אוון": {"אוון", "avène", "avene"},
    "לה רוש פוזה": {"לה רוש פוזה", "laroche posay", "la roche-posay"},
    "ביודרמה": {"ביודרמה", "bioderma"},
    "שווארצקופף": {"שווארצקופף", "schwarzkopf"},
    "פאנטין": {"פאנטין", "pantene"},  # alt OCR
    "קרליין": {"קרליין", "careline"},

    # ——— More Israeli pantry ———
    "תפוגן": {"תפוגן", "tivall"},  # again to reinforce
    "סוגת": {"סוגת", "sugat"},
    "קנור": {"קנור", "knorr"},  # reinforce
    "מיימון": {"מיימון", "maymon"},  # OCR variant of מימון
    "ינון": {"ינון", "yinon"},
    "ששון": {"ששון", "sason", "sasson"},
    "השומרון": {"השומרון", "hashomron"},

    # ——— Random frequent imports (food) ———
    "President": {"פרזידנט", "president", "président"},
    "פילדלפיה": {"פילדלפיה", "philadelphia"},
    "ברילה": {"ברילה", "barilla"},  # reinforce
    "סניקרדודל": {"snickerdoodle", "סניקרדודל"},  # protective

    # ——— Drinks (Israeli lines) ———
    "יפאורה": {"יפאורה", "יפאורה-תבורי", "yafora", "tempo-yafora"},
    "תבורי": {"תבורי", "tabori"},  # historic

    # ——— Energy / soft brands often OCR’d ———
    "שוופס": {"שוופס", "schweppes"},
    "קולה": {"קולה", "cola"},  # generic fallback brand sometimes printed

    # ——— Safeties for OCR / punctuation variants ———
    "סנו פרש": {"סנו פרש", "sano fresh"},
    "סנו פוליוואקס": {"סנו פוליוואקס", "polivax", "polywax"},
    "סנו פרש ענק": {"סנו פרש ענק", "sano fresh giant"},
}
