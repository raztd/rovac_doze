from sqlite3 import Error, connect
from pandas import read_csv, read_excel, DataFrame

excel_file = "vaccinare-covid19-grupe-risc-01-23.03.2021.xlsx"
# path to folder where the excel file is located
# & where .csv & .sqlite3 fieles will be saved
working_dir = "./"
'''
script to analyze "transparenta covid-19" [0] data
prerequirements: python 3.7+, pandas, openpyxl
[0] https://data.gov.ro/dataset/transparenta-covid

by Răzvan T Duca

bitcoin donations: bc1q5f5km4x2etmylthjt87jn8j09gtwx4vyjewwhg

contact: razvan.t.duca@protonmail.ch

-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: OpenPGP.js v4.10.8
Comment: https://openpgpjs.org

xsBNBFpABjIBCADK/+BWrVn7Qgqu96ThKmaKN762cRJSwy9jGZ4Y9ODgu+0B
G10g0PsS+sm5az6XmgdWlcPNUih6fgCR9p+rAxpHAa1fHOuDZLulH2PRlBhN
5BALNM644wiHhzkjVxCewHuaW6eyu5ktZml0WSML/6R3La1a8PM31voQlgsP
4ELTwjLl0xZ3c3nbfrFmO2Q8HMSQQrzWB7sc78pUm56tzGKd9BVsSxGG3FuJ
AS3oYhT9DlqXGteU6v0gle6S2ut3WeWbeTaZWBu4VV5Uidm91brZbtzGEVU1
+nNJGULVoXkjiCtOu8LaYd0Xg55NKf8KIFjm4UhjWQQlb2MdBN3JhbSJABEB
AAHNOXJhenZhbi50LmR1Y2FAcHJvdG9ubWFpbC5jaCA8cmF6dmFuLnQuZHVj
YUBwcm90b25tYWlsLmNoPsLAfwQQAQgAKQUCWkAGMwYLCQcIAwIJECwrXYgU
eBrdBBUICgIDFgIBAhkBAhsDAh4BAAoJECwrXYgUeBrdzfMH/RELh+IppI9t
2pFRTXQ4/5+WbEff4eVvNEK5A5fm0CFZdo2z1qDCJF58dBFIVYnkXkhJ9jgm
qX47WdqORmGR5brEVIzbqsAQGnCoZFAe8rO/n1aj981lOJYrHNTi3lKqEsum
rY28vS3eUEVn7AvXQKcFbpz/ZBtam5Fh4XsqgVQtz3dKuAMNpUTWPwHS4lTz
P+UxnJsxTf5jJeUCzvB3/PO4ZPNYj7TWA85BF0Ryj9bauDu2yk7Rtwpo4FQZ
c8cqg6Kwj+fmi4iVijkyC/5kUIO9971dueCpY2CY8nMY3Sy+RB5g58eF+GiD
y9z5oyUlBFuj6ITWjTAzta67RmzhIpTOwE0EWkAGMgEIANTP2y++J+fqptWc
2++mngsRyPFl6a5RwGgXGliVenh3/oAQuOEQ3vB73X7PVpNDdKgvSSeTGStv
YRZWPUtmc/BzG+1neBV+3PKrTIahQZsUU/Xc3BPRxK6XlRu6h8IcbQQOpG1R
5UjQ+El1uEtrkS4rdTsxHhcu74iNcHfVI5/uebjYgwZl+RQpni6Wb5COVI7p
8YmBO1c5u+fWPUh4GizJ4QUt7BYgm//raZqCln1vqEhkSu0BctF7EeuuU4jF
AKaa1fBAc7yrFa2qQ78QEQ04al8k73lTDZkD4TLoWuPOdfZ7gFXyNQKkNe6d
0DeO/zC4Ea1+XVaHYlqgRDlqF4UAEQEAAcLAaQQYAQgAEwUCWkAGMwkQLCtd
iBR4Gt0CGwwACgkQLCtdiBR4Gt1Ohgf/SjEqJKRw2hqRoNol9pbcHBN71PzH
weaV+koqNfEx2MAYrgsD+9Wptl3E1WRKF7lDxUPb7G57ypvKmlvVeMYs+0md
sUxf/C191kK0oaXIeU02TyowrGTn1b9U+MFQOPklZ9aFu8NghAZALYriyiGL
UwJsdRfRTlno02UeCNjtbLVd/74Cp7RU14mVqtc2oLHbe47bnRStpLBM9Yhu
2Nmy4+U8EiD9VC1tJZlbIn9MEoY4N2NVoUJOHgJwoYOIlsmBB4UbJ1Ad0vMw
5xRqLOkVDgsmod2kvAmoSBXm9C1EvUQjYgy++NTv+EMVztVRjf5Q+DdpRy1i
OfL+E98rab3bYg==
=7VR1
-----END PGP PUBLIC KEY BLOCK-----
'''

COUNTY_COL = "Județ"
LOCALITY_COL = "Localitate"
CENTER_COL = "Nume centru"
DATE_COL = "Data vaccinării"
VACCINE_COL = "Produs"
DOSES_COL = "Doze administrate"
CATEGORY_COL = "Grupa de risc"

cats = {1: "Categoria I", 2: "Categoria a II-|Categoria a II a",
        3: "Categoria a III-|Categoria a III a"}


def df_filter(df, column, string):
    return df[df[column].str.contains(string, case=False, na=False)]


def unique_values_from_column(df, column):
    return df[column].dropna().unique().tolist()


def get_dates(df):
    # the dates in excel start with a ' character
    return [x.replace("'", "") for x in unique_values_from_column(df, DATE_COL)]


def df_sum(df, column):
    return int(df[column].sum())


def get_stats(df, county, locality, center, date, category):
    return [
        county,
        locality,
        center,
        date,
        category,
        df_sum(df, DOSES_COL),
        df_sum(df_filter(df, VACCINE_COL, "astra"), DOSES_COL),
        df_sum(df_filter(df, VACCINE_COL, "pfizer"), DOSES_COL),
        df_sum(df_filter(df, VACCINE_COL, "moderna"), DOSES_COL)]


def compute_stats(df):
    stats = []
    dates = get_dates(df)
    counties = unique_values_from_column(df, COUNTY_COL)
    for county in counties:
        print(county)
        df_county = df_filter(df, COUNTY_COL, county)
        localities = unique_values_from_column(df_county, LOCALITY_COL)
        for locality in localities:
            df_locality = df_filter(df_county, LOCALITY_COL, locality)
            centers = unique_values_from_column(df_locality, CENTER_COL)
            stats += all_centers(df_locality, county,
                                 locality, centers, cats, dates)
        locality = center = "toate"
        stats += all_cats(df_county, county, locality, center, cats, dates)
    county = locality = center = "toate"
    stats += all_cats(df, county, locality, center, cats, dates)
    return DataFrame(stats, columns=["judet", "localitate", "centru", "categorie_pers",
                                     "data", "total_doze", "astra", "pfizer", "moderna"])


def all_centers(df, county, locality, centers, categories, dates):
    lst = []
    for center in centers:
        df_center = df[df[CENTER_COL] == center]
        lst += all_cats(df_center, county, locality, center, categories, dates)
    lst += all_cats(df, county, locality, "toate", categories, dates)
    return lst


def all_cats(df, county, locality, center, categories, dates):
    lst = []
    for category in categories:
        df_cat = df_filter(df, CATEGORY_COL, cats[category])
        lst += all_dates(df_cat, county, locality, center, category, dates)
    lst += all_dates(df, county, locality, center, "toate", dates)
    return lst


def all_dates(df, county, locality, center, category, dates):
    lst = []
    for date in dates:
        df_date = df_filter(df, DATE_COL, date)
        lst.append(get_stats(df_date, county, locality, center, category, date))
    lst.append(get_stats(df, county, locality, center, category, "toate"))
    return lst


def get_data(filepath):
    return read_excel(filepath)
    # return read_csv(filepath)


def write_to_csv(df, filepath):
    df.to_csv(filepath, index=False)


class Db:
    def __init__(self, filepath):
        try:
            self.__con = connect(filepath)
        except Error as e:
            print(e)

    def save_df(self, df):
        df.to_sql("vaccine_jabs", self.__con, index=False)


def check_vaccine_types(df):
    new_df = df[~df[VACCINE_COL].str.contains(
        "astra|pfizer|moderna", case=False, na=False)]
    new_df = new_df[~new_df[VACCINE_COL].isnull()]
    if new_df.size == 0:
        print("OK!")
    else:
        print(new_df)
        raise ValueError("there is a new type of vaccine. please adjust the script")


def run():
    global stats
    print("Loading data, might take some time (up to 1 min)... ")
    df = get_data(f"{working_dir}{excel_file}")
    check_vaccine_types(df)
    print("Analyzing data... ")
    stats = compute_stats(df)
    print("Save stats to files")
    write_to_csv(stats, f"{working_dir}{excel_file}_2.csv")
    db = Db(f"{working_dir}{excel_file}_2.sqlite3")
    db.save_df(stats)


if __name__ == "__main__":
    run()
