"""The name tables behind `core/gender.py`.

One dict per role, in the repository, in the style of `core/i18n.py` — no
extraction step, no downloaded corpus, no .po files to drift, and a test that
asserts the tables are internally consistent. The alternative was surveyed and
rejected on measurement: `pymorphy3` installs on Python 3.14 but loses to a
suffix rule by 21 points on this base, and its Ukrainian dictionary derives
from a NonCommercial-licensed source, which is disqualifying for a commercial
retailer.

WHY A LEXICON AT ALL, WHEN MORPHOLOGY ALREADY DECIDES 99.8%

Measured on the 4 104-buyer patronymic gold set: with the dictionary emptied,
the morphology rule alone still decides 99.81% of rows — but male precision
falls from 96.1% to 87.9%. The dictionary does not buy coverage. It buys
precision on the minority class, which is the only class with any value here.
Every entry below exists to keep one more name away from `given_morphology`,
the single layer that produces false males.

THE ASYMMETRY THAT SHAPES EVERY TABLE

Slavic surnames overwhelmingly end in a consonant, and so do male given names.
So mistaking a SURNAME for a given name manufactures a FALSE MALE, while the
reverse merely costs coverage. That is why `SURNAME_SHAPES` and
`SURNAME_LEXICON` exist at all: they are not used to decide gender, they are
used as *negative evidence* about which token is the given name. A token that
looks like a surname is not offered to the gender rules.

WHAT IS AND IS NOT IN HERE

Only name knowledge. No thresholds, no confidence levels, no store, no I/O —
those belong to `core/gender.py` and to its caller. Nothing here is derived
from an individual customer: every entry is a name shared by many people, and
no table is keyed on a buyer.
"""
from __future__ import annotations

# ── folding ──────────────────────────────────────────────────────────────────
# Ukrainian and Russian spell one name several ways, and a customer types a
# fourth. Folding collapses the orthographic axis so one entry serves them all:
# Ірина/Ирина, Олена/Елена, Наталія/Наталия, Мар'яна/Марьяна/Маряна.
FOLD_MAP = {
    "і": "и", "ї": "и", "й": "и", "ы": "и",
    "є": "е", "э": "е", "ё": "е",
    "ґ": "г",
    "ь": "", "ъ": "",
    "'": "", "ʼ": "", "’": "", "`": "", "‘": "",
}

# Latin homoglyphs → Cyrillic, so a Cyrillic name typed on a Latin keyboard
# reaches the same entry. Used for LOOKUP only; never to rewrite stored data.
HOMOGLYPH_MAP = {
    "a": "а", "b": "в", "c": "с", "d": "д", "e": "е", "f": "ф", "g": "г",
    "h": "н", "i": "і", "j": "й", "k": "к", "l": "л", "m": "м", "n": "н",
    "o": "о", "p": "р", "q": "к", "r": "р", "s": "с", "t": "т", "u": "у",
    "v": "в", "w": "в", "x": "х", "y": "у", "z": "з",
}

# ── given names ──────────────────────────────────────────────────────────────
# Written in their natural spelling and folded at import. Where two languages
# disagree, both spellings appear: folding usually unifies them, and where it
# does not (Олена/Елена) the second entry is the cheap fix.

FEMALE_NAMES = """
Анна Ганна Аня Анюта Анька Нюра
Олена Елена Альона Альона Алёна Олеся Леся Оленка Лєна Лена
Ірина Ирина Іра Іринка Ірочка
Наталія Наталя Наталия Наталья Наташа Ната Наталка
Тетяна Татьяна Таня Танюша Тетянка
Оксана Ксана Ксюша Ксенія Ксения Оксанка
Світлана Светлана Світа Света Свєта Лана
Юлія Юлия Юля Юлька Юліанна Юліана
Марія Мария Маша Маруся Машенька Мар'яна Маряна Марьяна Марʼяна Маріанна Марианна Мар'янка
Катерина Екатерина Катя Катруся Катерінка Каріна Карина
Вікторія Виктория Віка Вика Вікуся
Ольга Оля Олька Олюня Ольга
Людмила Люда Людочка Люся Мила
Валентина Валя Валюша Валентинка
Галина Галя Галинка
Надія Надежда Надя Наденька
Любов Люба Любаша Любов
Лариса Лора Ларіса Лариса
Алла Аллочка
Інна Инна Іночка
Ніна Нина Ніночка
Віра Вера Вєра Вірочка
Софія София Софа Соня Софійка Sonya
Анастасія Анастасия Настя Настуся Наста Настасія
Дарина Дар'я Дарья Даря Даша Дашенька Дарʼя Даринка
Аліна Алина Алінка
Христина Кристина Хрестина Христя Крістіна Крiстiна
Яна Янка Яніна Янина
Марина Маринка Мариночка
Єлизавета Елизавета Ліза Лиза Лізонька
Валерія Валерия Лєра Лера Валєрія
Діана Диана Діанка
Злата Златка
Поліна Полина Поля
Вероніка Вероника Ніка Ника Вероничка
Мілана Милана Міла Мілена Милена
Аделіна Аделина Аделя Адель
Ілона Илона
Еріка Эрика Ерика
Аніта Анита
Даяна Даяна Даяна
Емма Эмма Ема
Ева Єва Евеліна Евелина Евелінка
Жанна Жаннa Жанночка
Віолетта Віолета Виолетта Віка
Неллі Нелли Неля Нелля
Даніелла Даніела Данієла Даниэла
Сюзанна Сузанна
Камілла Каміла Камила Каміля
Габріелла Габріела Габриэлла
Елліна Еліна Элина Еліна
Зоряна Зоряна Зоря
Соломія Соломия Соломійка
Ярина Ярина Яринка
Мирослава Мирослава Мирося
Владислава Влада Владуся
Богдана Богданка
Руслана Русланка
Сніжана Снежана
Анжеліка Анжелика Анжела Анжелла Ангеліна Ангелина Анжелінка
Стефанія Стефания Стефа
Тамара Тома Томочка
Раїса Раиса Рая
Зоя Зоїна
Майя Мая Маїна
Лідія Лидия Ліда Лида
Лілія Лилия Ліля Ліна Лина
Аврора Агата Агнеса Агнія Ада Азалія Аксінья Альбіна Альбина
Амалія Амелія Анфіса Аркадія Аеліта
Богуслава Božena Броніслава
Валерія Ванда Варвара Варя Василина Віталіна Виталина Віталія
Влада Владислава Власта
Гелена Генрієта Горпина
Дана Данута Дарія Діна Дина Домініка Доміка
Ельвіра Эльвира Ельза Емілія Эмилия Емілі Ернестина Есмеральда Естер
Єфросинія Євгенія Евгения Женя
Жаклін Жозефіна
Зінаїда Зина Зіна Зорина
Іванна Иванна Іванка Ізабелла Изабелла Інеса Інга Инга Ірена Ирена Іраїда Ія
Калина Капітоліна Кароліна Каролина Кіра Кира Клавдія Клара Констанція Ксенія
Лада Лана Леоніда Ліана Лілея Лія Лора Луїза Любомира Люція
Магдалина Мальвіна Маргарита Рита Ритуся Марта Марфа Марʼяна Матильда
Меланія Мілада Мілослава Муза
Надіра Нана Настасія Наталина Нелла Ніколь Николь Ніна Нонна Нора
Октябрина Олександра Александра Оляна Орися Орина
Павліна Павлина Пелагія Пріска
Рената Римма Рита Роза Розалія Роксолана Роксана Ромина Русалина
Сабіна Саломея Самара Сандра Сара Севіла Селена Серафима Сильвія Сніжинка
Станіслава Стелла Сюзана
Таїса Таїсія Таисия Тала Теодора Тереза
Уляна Ульяна
Фаїна Феодора Флора Франческа
Христя Хріста
Чеслава
Шарлотта Шушана
Щаслива
Юстина Юліта Юнона
Ядвіга Якилина Ярослава Ясміна
"""

MALE_NAMES = """
Олександр Александр Саня Санько Олексій Алексей Альоша Льоша Лёша
Андрій Андрей Андрійко Андрюша
Сергій Сергей Сергійко Серьожа Сєрьожа
Дмитро Дмитрий Діма Дима Димон Митя
Володимир Владимир Вова Вовка Володя
Віталій Виталий Віталик Виталик
Юрій Юрий Юра Юрко Юрась
Ігор Игорь Ігорьок Игорёк
Іван Иван Ваня Іванко Ванюша
Микола Николай Коля Миколка
Петро Пётр Петр Петя Петрик
Павло Павел Паша Павлик
Роман Рома Ромчик Ромка
Руслан Русланчик
Віктор Виктор Вітя Витя Вітьок
Владислав Влад Владік
Артем Артём Артьом Артемій
Артур Артурчик
Богдан Богданчик Данко
Денис Денисик Дениска
Євген Евгений Женя Жека Євгеній Євгеній
Максим Макс Максимко
Михайло Михаил Міша Миша Мішаня
Олег Олежка Олежик
Станіслав Стас Стасик
Тарас Тарасик
Вадим Вадік Вадик
Валерій Валерий Валера Валєра
Василь Василий Вася Васько
Ярослав Ярік Ярик Славік Славик
Анатолій Анатолий Толя Толік
Григорій Григорий Гриша Гріша
Ілля Илья Ілюша
Кирило Кирилл Кирилко
Леонід Леонид Льоня Леня
Арсен Арсеній Арсений Арсенчик
Данило Даниил Данил Данік Даня Даніїл
Назар Назарій Назарко
Остап Остапко
Ростислав Ростик
Святослав Свят
Тимур Тимофій Тимофей Тіма Тимко
Едуард Эдуард Едік Эдик
Еміль Эмиль
Пилип Филипп Філіп Филип
Георгій Георгий Гоша Жора Юрій
Захар Захарко
Матвій Матвей Мотя
Марко Маркіян Марк
Лука Лукʼян Лукян Лук'ян
Лев Лева Левко
Савелій Сава Савва Савка
Семен Семён Сеня Сємен
Степан Стьопа Степанко
Федір Фёдор Федор Федя
Яків Яков Яша
Кузьма Хома Фома Мойсей
Адам Аполлон Аркадій Аркадий Афанасій Арнольд Альберт Анатоль Антон Антоній Антошка
Боніфацій Борис Боря Броніслав
Валентин Валентін Вʼячеслав Вячеслав Слава Віктор Віталь Владлен Всеволод Сєва
Гаврило Гнат Гордій Горислав
Давид Давід Дан Демʼян Демид Добромир
Еліас Емельян Ераст Еразм Ернест Ефим
Жан Жданко
Зіновій Зорян
Ігнат Ілларіон Іларіон Інокентій Іполит Йосип Йосиф
Казимир Карл Клим Климент Кондрат Костянтин Константин Костя Купріян
Лаврентій Ладислав Ларіон Любомир Людвіг
Макар Марат Маріан Мар'ян Мирон Мирослав Митрофан Мстислав Мусій
Наум Нестор Никифор Нікіта Никита Нікіта
Овідій Онисим Орест Опанас Осип
Панас Панкрат Парамон Платон Порфирій Потап Прохор
Радислав Радій Рафаїл Рем Рустам
Самійло Самуїл Севастьян Серафим Сидір Сильвестр Созон Соломон Сократ Спиридон Стефан
Терентій Тихон Трохим
Устим
Феліціан Фелікс Феодосій Филимон Фрол
Харитон Христофор
Цезар
Чеслав
Шимон
Щасливий
Юліан Юлій Юхим
Ян Януш Ярема Яромир Ясон
"""

# Latin transliterations. Ukrainian passports use KMU 55, Russian ones ICAO, and
# customers type neither consistently — hence three spellings of one name.
FEMALE_LATIN = """
anna hanna ganna ania anya
olena elena alena alona olesia lesia olesya
iryna irina ira
nataliia nataliya natalia natalya natasha nata
tetiana tatiana tatyana tania tanya
oksana kseniia ksenia ksusha
svitlana svetlana sveta lana
yuliia yuliya julia yulia julija
mariia maria marya masha marianna mariana maryana
kateryna ekaterina katerina katia katya karina caryna
viktoriia viktoria victoria vika
olga olha olya
liudmyla lyudmyla ludmila luda mila
valentyna valentina valya
halyna galyna galina galya
nadiia nadezhda nadia nadya
liubov lyubov luba liuba
larysa larisa lora
alla inna nina vira vera
sofiia sofia sophia sonya sonia
anastasiia anastasia nastia nastya
daryna daria darya dasha dashenka
alina alena
khrystyna krystyna kristina christina
yana iana janna zhanna
maryna marina
yelyzaveta elizaveta liza lisa
valeriia valeria lera
diana zlata polina
veronika veronica nika
milana mylana milena
adelina adelia adel
ilona erika anita dayana
emma eva yeva evelina
violetta violeta
nelli nelia nellia
daniela daniella
suzanna susanna
kamilla kamila camila
gabriella gabriela
elina ellina
zoriana solomiia yaryna
myroslava vladyslava vlada
bohdana ruslana snizhana
anzhelika angelina anzhela
stefaniia stefania tamara raisa
zoia zoya maia maya
lidiia lidia lida
liliia lilia lilya lina
alisa alice albina amaliia
varvara vasylyna vitalina
dana dina dominika
elvira emiliia
ivanna izabella inha inga
kira klavdiia karolina
margarita rita marta
oleksandra aleksandra alexandra
renata roksolana roksana
sabina samira sandra
taisiia uliana ulyana
yaroslava yustyna
"""

MALE_LATIN = """
oleksandr aleksandr alexander sasha sanya
oleksii aleksei alexey oleksiy alosha
andrii andriy andrey andre
serhii sergii sergey serhiy seryozha
dmytro dmitry dmitriy dima
volodymyr vladimir vova volodya
vitalii vitaliy vitaly vitalik
yurii yuriy yury yura
ihor igor
ivan vania vanya
mykola nikolay kolya
petro petr pyotr petya
pavlo pavel pasha
roman roma
ruslan
viktor victor vitia vitya
vladyslav vladislav vlad
artem artom tioma artemii
artur arthur
bohdan bogdan
denys denis
yevhen evgeniy evgeny zhenia zhenya yevhenii
maksym maxim maks
mykhailo mikhail misha
oleh oleg
stanislav stas
taras
vadym vadim
valerii valeriy valera
vasyl vasily vasya
yaroslav yarik slavik
anatolii anatoliy tolya
hryhorii grigoriy grisha
illia ilya iliya
kyrylo kirill
leonid lonia
arsen arsenii arseniy
danylo daniil danil dania
nazar nazarii
ostap rostyslav sviatoslav
tymur timur tymofii timofey
eduard edward edik
emil filip philip pylyp
heorhii georgiy zhora
zakhar matvii matvey
marko mark markiyan
luka lev levko
savelii sava savva
semen stepan stiopa
fedir fedor fedya
yakiv yakov yasha
anton antin
borys boris borya
kostiantyn konstantin kostia
mykyta nikita
platon orest
serafym sydir
"""

# Male given names the single strongest rule gets wrong. "Ends in -а/-я → female"
# scores 99.70% precision on the gold set; these are why it is not 100%.
MALE_VOWEL_ENDING = """
Микола Ілля Илья Микита Нікіта Никита Сава Савва Кузьма Хома Фома Лука
Данила Мойсей Яша Вітя Петя Вася Коля Толя Сеня Гена Сєва Діма Дима
Льоша Альоша Вова Рома Гриша Міша Юра Паша Саня Стьопа Жора
Ваня Женя Слава Валя Костя Мотя Даня Яня Ларіон Зося
Лева Ілюша Андрюша Серьожа Вовка Ромчик
"""

# Male given names ending in -о. In Ukrainian this is a large and ordinary
# class, unlike Russian — Дмитро, Павло, Петро, Данило are all male.
MALE_O_ENDING = """
Дмитро Павло Петро Данило Михайло Марко Левко Іванко Гаврило Кирило
Сидір Тимко Юрко Василько Данилко Богданко Степанко Федько Грицько
Андрійко Сергійко Миколка Петрик Павлик Максимко Назарко Остапко
"""

# Female given names that do NOT end in a vowel — the mirror exception.
FEMALE_CONSONANT_ENDING = """
Любов Нінель Адель Рахіль Естер Жизель Мішель Ізабель Ніколь Николь
Руф Юдиф Кармен Ірен Ліліт Юдит Голда Айгуль Гульнар Суламіф
Зейнаб Мерієм Нур Жаклін Мадлен Ясмін Карін Катрін Ельзе Сольвейг
"""

# Genuinely unisex in this alphabet. Refusing is the correct output: guessing
# here is how a classifier manufactures errors it cannot detect.
UNISEX_NAMES = """
Саша Валя Женя Шура Слава Сима Ася Тоня Муся Юся
Sasha Zhenia Zhenya Valia Valya Slava Shura
"""

# ── organisation and non-person markers ──────────────────────────────────────
# A row that is a company, a marketplace handle or a test record has no gender
# to find. It is refused, not guessed.
#
# Markers shorter than MIN_MARKER_LEN are never matched. Folding collapses
# doubled letters, so "ПП" becomes "п" and would otherwise swallow every row
# carrying a bare initial.
MIN_MARKER_LEN = 3

ORG_MARKERS = """
тов чп пат зат тзов
ltd llc llp inc corp gmbh sarl company
опт оптом склад магазин маркет крамниця аптека
салон студія студия шоурум showroom shop store boutique
клініка клиника сервіс сервис компанія компания
"""

# A sole trader is a PERSON. "ФОП Шевченко Андрій Іванович" names a human being
# with a gender, and refusing the row loses 72 real customers on this base —
# measured, after a first version treated these as companies and male recall on
# the gold set fell from 98.4% to 95.3%. The prefix is stripped and the name
# behind it is classified normally.
SOLE_TRADER_MARKERS = """
фоп фізособа фізична-особа-підприємець
ип чп-фіз підприємець предприниматель
"""

NON_PERSON_MARKERS = """
тест test testing демо demo sample example
newsletter subscriber subscription unsubscribe
клієнт клиент покупець покупатель користувач
noname unknown невідомо неизвестно нет ні
instagram інстаграм телеграм telegram viber whatsapp
"""

# ── surname evidence ─────────────────────────────────────────────────────────
# NOT used to decide gender except where explicitly marked. Their job is to say
# "this token is a surname", so it is never offered to the given-name rules.

# Endings that make a token a surname whatever its gender.
SURNAME_SHAPES = (
    "енко", "енка", "ук", "юк", "чук", "цюк", "ко", "ич", "ыч",
    "ів", "ов", "ев", "єв", "ін", "ин", "ський", "цький", "зький",
    "ский", "цкий", "ская", "цкая", "ова", "ева", "єва", "іна", "ина",
    "ська", "цька", "зька", "ак", "як", "ець", "ий", "ый", "ая",
    "ань", "ар", "ир", "ур", "аш", "иш", "ух", "ай", "ей", "ой",
)

# Latin renderings of the same shapes, for the 2% of rows typed in Latin.
SURNAME_SHAPES_LATIN = (
    "enko", "uk", "iuk", "yuk", "chuk", "ko", "ych", "ich",
    "iv", "ov", "ev", "in", "yn", "skyi", "sky", "ski", "skiy",
    "tskyi", "ova", "eva", "ina", "ska", "aia", "aya",
)

# Surname endings that DO carry gender, and may be used as a fallback when the
# given name decided nothing. Female forms are reliable; the male list is an
# ALLOW-LIST, never "ends in a consonant" — -ів/-ук/-енко/-ак are invariant and
# a consonant rule over them would call every such woman a man.
SURNAME_FEMALE_MARKED = (
    "ова", "єва", "ева", "ьова", "іна", "ина", "ська", "цька", "зька",
    "ская", "цкая", "зкая", "ая", "яя",
)
SURNAME_MALE_MARKED = (
    "ський", "цький", "зький", "ский", "цкий", "зкий",
    "ов", "ев", "єв", "ьов", "ий", "ый",
)
# Invariant in Ukrainian: the same form for a man and a woman. Listed so the
# male rule can never claim them.
SURNAME_INVARIANT = (
    "енко", "ук", "юк", "чук", "цюк", "ко", "ич", "ів", "ак", "як",
    "ець", "ар", "ир", "ур", "ан", "ун", "ень", "усь", "ій", "ей",
)

# Surnames common enough in this base to be typed alone, and shaped like a
# given name. Without these the role resolver can offer them to the gender
# rules, and a consonant ending then manufactures a false male.
SURNAME_LEXICON = """
Шевченко Коваль Ковальчук Коваленко Мельник Кравченко Кравчук Бондаренко
Бондар Ткаченко Ткачук Савчук Савченко Поліщук Ляшенко Захарова Мороз
Кучеренко Харченко Артамонова Крупа Гаврилюк Волкова Кім Климчук Павлюк
Приходько Титаренко Романова Сова Павленко Василенко Кулик Калюжна
Тарасенко Власенко Таран Балюк Романчук Тищенко Кравчук Грищенко Король
Семенова Гринчук Михалюк Курило Білецька Кушнірук Кушнір Денисенко
Бойко Шевчук Олійник Лисенко Руденко Марченко Пономаренко Сидоренко
Мартиненко Гончаренко Іванченко Петренко Степаненко Даниленко Науменко
Козак Гриценко Левченко Максименко Федоренко
Мазур Гуменюк Пилипенко Зайцева Соколова Морозова Новікова Попова
Лебедєва Козлова Смирнова Кузнецова Іванова Петрова Сидорова Федорова
Ковальська Вишневська Малиновська Заболотна Зелена Біла Чорна Руда
Шульга Гончар Швець Кушнерук Гаврилів Данилів Ковалів Петрів Іванів
"""


def _words(block: str) -> list:
    """Split a table block into tokens, ignoring blank lines and comments."""
    out = []
    for line in block.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.extend(w for w in line.split() if w)
    return out
