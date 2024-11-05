import os
import sys
import shutil
import tempfile
import requests
import pandas as pd
import numpy as np
from time import sleep
from random import randint
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.error import HTTPError, URLError


def main():
    print("NPB Post Season Statistic Scraper")
    # Open the directory to store the scraped stat csv files
    relDir = os.path.dirname(__file__)
    statsDir = os.path.join(relDir, "stats")
    if not (os.path.exists(statsDir)):
        os.mkdir(statsDir)

    # Create year directory
    scrapeYear = get_scrape_year()
    yearDir = os.path.join(statsDir, scrapeYear)
    if not (os.path.exists(yearDir)):
        os.mkdir(yearDir)

    if get_user_choice("P") == "Y":
        get_playoff_stats(yearDir, "BP", scrapeYear)
        get_playoff_stats(yearDir, "PP", scrapeYear)
    postBatPlayerStats = PlayerData(statsDir, yearDir, "BP", scrapeYear)
    postPitchPlayerStats = PlayerData(statsDir, yearDir, "PP", scrapeYear)
    postBatPlayerStats.output_final()
    postPitchPlayerStats.output_final()


class Stats:
    def __init__(self, statsDir, yearDir, suffix, year):
        self.statsDir = statsDir
        self.suffix = suffix
        self.year = year
        self.yearDir = yearDir


class PlayerData(Stats):
    def __init__(self, statsDir, yearDir, suffix, year):
        """PlayerData new variables:
        df (pandas dataframe): Holds an entire NPB league's individual
        batting/pitching stats"""
        super().__init__(statsDir, yearDir, suffix, year)
        # Initialize data frame to store stats
        self.df = pd.read_csv(
            self.yearDir + "/" + year + "StatsRaw" + suffix + ".csv"
        )
        # Modify df for correct stats
        # self.post_season_merge()
        if self.suffix == "BP":
            self.org_bat()
        elif self.suffix == "PP":
            self.org_pitch()

    def __str__(self):
        """Outputs the Alt view of the associated dataframe (no HTML team or
        player names, no csv formatting, shows entire df instead of only
        Leaders)"""
        return self.df.to_string()

    def output_final(self):
        """Outputs final files for upload using the filtered and organized
        stat dataframes (NOTE: IP and PA drop constants are determined in this
        function) (CONVERSION OF writePlayerStats())

        Parameters: N/A

        Returns: N/A"""
        # Make dir that will store alt views of the dataframes
        altDir = os.path.join(self.yearDir, "alt")
        if not (os.path.exists(altDir)):
            os.mkdir(altDir)
        # Make dirs that will store files uploaded to yakyucosmo.com
        uploadDir = os.path.join(self.yearDir, "npb")
        if not (os.path.exists(uploadDir)):
            os.mkdir(uploadDir)

        # Print organized dataframe to file
        newCsvAlt = altDir + "/" + self.year + "AltView" + self.suffix + ".csv"
        self.df.to_string(newCsvAlt)
        # Make deep copy of original df to avoid HTML in df's team/player names
        finalDf = self.df.copy()
        # Convert player/team names to HTML that contains appropriate URLs
        if int(self.year) == datetime.now().year:
            finalDf = convert_player_to_html(finalDf, self.suffix, self.year)
        finalDf = convert_team_to_html(finalDf, "Abb")
        # Print final file with all players
        newCsvFinal = (
            uploadDir + "/" + self.year + "StatsFinal" + self.suffix + ".csv"
        )
        finalDf.to_csv(newCsvFinal, index=False)

        # AltView, Final file output
        if self.suffix == "PP":
            print(
                "An alternative view of the pitching results will be stored "
                "in: " + newCsvAlt
            )
            print(
                "The final organized pitching results will be stored in: "
                + newCsvFinal
            )
        elif self.suffix == "BP":
            print(
                "An alternative view of the batting results will be stored "
                "in: " + newCsvAlt
            )
            print(
                "The final organized batting results will be stored in: "
                + newCsvFinal
            )

    def org_pitch(self):
        """Organize the raw pitching stat csv and add new stats
        (CONVERSION OF pitchOrg())

        Parameters: N/A

        Returns: N/A"""
        # Some IP entries can be '+', replace with 0 for conversions and
        # calculations
        self.df["IP"] = self.df["IP"].astype(str).replace("+", "0")
        # Convert all NaN to 0 (as floats)
        if self.suffix == "PP":
            self.df.iloc[:, 11] = self.df.iloc[:, 11].fillna(0)
            self.df.iloc[:, 11] = self.df.iloc[:, 11].astype(float)
            # Combine the incorrectly split IP stat columns
            self.df["IP"] = self.df["IP"].astype(float)
            self.df["IP"] = self.df["IP"] + self.df.iloc[:, 11]
            # Drop unnamed column that held IP column decimals
            self.df.drop(self.df.columns[11], axis=1, inplace=True)
        # Combine duplicate player entries
        agg_functions = {
            "Pitcher": "first",
            "G": "sum",
            "W": "sum",
            "L": "sum",
            "SV": "sum",
            "HLD": "sum",
            "CG": "sum",
            "SHO": "sum",
            "PCT": "first",
            "BF": "sum",
            "IP": "sum",
            "H": "sum",
            "HR": "sum",
            "BB": "sum",
            "IBB": "sum",
            "HB": "sum",
            "SO": "sum",
            "WP": "sum",
            "BK": "sum",
            "R": "sum",
            "ER": "sum",
            "ERA": "first",
            "Team": "first",
        }
        self.df = self.df.groupby(self.df["Pitcher"], as_index=False).agg(
            agg_functions
        )
        # DEBUG
        # self.df.to_csv("test.csv",index=False)
        print(self.df.to_string())
        with open("output.txt", "w") as f:
            print(self.df.to_string(), file=f)

        # Translate player names TODO

        # Some ERA entries can be '----', replace with 0 and convert to float
        # for calculations
        self.df["ERA"] = self.df["ERA"].astype(str).replace("----", "inf")
        self.df["ERA"] = self.df["ERA"].astype(float)
        # Drop BK, and PCT columns
        self.df.drop(["BK", "PCT"], axis=1, inplace=True)
        # IP ".0 .1 .2" fix
        self.df["IP"] = convert_ip_column_in(self.df)

        # Counting stat column totals
        totalIP = self.df["IP"].sum()
        totalHR = self.df["HR"].sum()
        totalSO = self.df["SO"].sum()
        totalBB = self.df["BB"].sum()
        totalHB = self.df["HB"].sum()
        totalER = self.df["ER"].sum()
        totalBF = self.df["BF"].sum()
        totalERA = 9 * (totalER / totalIP)
        temp1 = 13 * totalHR
        temp2 = 3 * (totalBB + totalHB)
        temp3 = 2 * totalSO
        totalFIP = ((temp1 + temp2 - temp3) / totalIP) + select_fip_const(
            self.suffix, self.year
        )
        totalkwERA = round((4.80 - (10 * ((totalSO - totalBB) / totalBF))), 2)

        # Individual statistic calculations
        # Calculate kwERA
        self.df["kwERA"] = round(
            (4.80 - (10 * ((self.df["SO"] - self.df["BB"]) / self.df["BF"]))),
            2,
        )
        self.df = select_park_factor(self.df, self.suffix, self.year)
        tempERAP = 100 * ((totalERA * self.df["ParkF"]) / self.df["ERA"])
        self.df["ERA+"] = round(tempERAP, 0)
        self.df["ERA+"] = self.df["ERA+"].astype(str).replace("inf", "999")
        self.df["ERA+"] = self.df["ERA+"].astype(float)
        self.df["K%"] = round(self.df["SO"] / self.df["BF"], 3)
        self.df["BB%"] = round(self.df["BB"] / self.df["BF"], 3)
        self.df["K-BB%"] = round(self.df["K%"] - self.df["BB%"], 3)
        # Calculate FIP
        temp1 = 13 * self.df["HR"]
        temp2 = 3 * (self.df["BB"] + self.df["HB"])
        temp3 = 2 * self.df["SO"]
        self.df["FIP"] = round(
            ((temp1 + temp2 - temp3) / self.df["IP"])
            + select_fip_const(self.suffix, self.year),
            2,
        )
        # Calculate FIP-
        self.df["FIP-"] = round(
            (100 * (self.df["FIP"] / (totalFIP * self.df["ParkF"]))), 0
        )
        # Calculate WHIP
        self.df["WHIP"] = round(
            (self.df["BB"] + self.df["H"]) / self.df["IP"], 2
        )
        # Calculate HR%
        self.df["HR%"] = self.df["HR"] / self.df["BF"]
        # Calculate kwERA-
        self.df["kwERA-"] = round((100 * (self.df["kwERA"] / (totalkwERA))), 0)
        # Calculate Diff
        self.df["Diff"] = round((self.df["ERA"] - self.df["FIP"]), 2)

        # Data cleaning/reformatting
        # Remove temp Park Factor column
        self.df.drop("ParkF", axis=1, inplace=True)
        # "Mercedes Cristopher Crisostomo" name shortening to "Mercedes CC"
        self.df["Pitcher"] = (
            self.df["Pitcher"]
            .astype(str)
            .replace("Mercedes Cristopher Crisostomo", "Mercedes CC")
        )
        # Number formatting
        formatMapping = {
            "BB%": "{:.1%}",
            "K%": "{:.1%}",
            "K-BB%": "{:.1%}",
            "HR%": "{:.1%}",
            "Diff": "{:.2f}",
            "FIP": "{:.2f}",
            "WHIP": "{:.2f}",
            "kwERA": "{:.2f}",
            "ERA": "{:.2f}",
            "kwERA-": "{:.0f}",
            "ERA+": "{:.0f}",
            "FIP-": "{:.0f}",
        }
        for key, value in formatMapping.items():
            self.df[key] = self.df[key].apply(value.format)

        # Replace all infs in batting stat cols
        self.df["ERA"] = self.df["ERA"].astype(str)
        self.df["ERA"] = self.df["ERA"].str.replace("inf", "")
        self.df["FIP"] = self.df["FIP"].astype(str)
        self.df["FIP"] = self.df["FIP"].str.replace("inf", "")
        self.df["FIP-"] = self.df["FIP-"].astype(str)
        self.df["FIP-"] = self.df["FIP-"].str.replace("inf", "")
        self.df["WHIP"] = self.df["WHIP"].astype(str)
        self.df["WHIP"] = self.df["WHIP"].str.replace("inf", "")
        self.df["Diff"] = self.df["Diff"].astype(str)
        self.df["Diff"] = self.df["Diff"].str.replace("nan", "")
        # Changing .33 to .1 and .66 to .2 in the IP column
        self.df["IP"] = convert_ip_column_out(self.df)
        # Add "League" column
        self.df = select_league(self.df, self.suffix)
        # Column reordering
        self.df = self.df[
            [
                "Pitcher",
                "G",
                "W",
                "L",
                "SV",
                "HLD",
                "CG",
                "SHO",
                "BF",
                "IP",
                "H",
                "HR",
                "SO",
                "BB",
                "IBB",
                "HB",
                "WP",
                "R",
                "ER",
                "ERA",
                "FIP",
                "kwERA",
                "WHIP",
                "ERA+",
                "FIP-",
                "kwERA-",
                "Diff",
                "HR%",
                "K%",
                "BB%",
                "K-BB%",
                "Team",
                "League",
            ]
        ]

    def org_bat(self):
        """Organize the raw batting stat csv and add additional stats
        (CONVERSION OF batOrg())

        Parameters: N/A

        Returns: N/A"""
        # Combine duplicate player entries
        agg_functions = {
            "Player": "first",
            "G": "sum",
            "PA": "sum",
            "AB": "sum",
            "R": "sum",
            "H": "sum",
            "2B": "sum",
            "3B": "sum",
            "HR": "sum",
            "TB": "sum",
            "RBI": "sum",
            "SB": "sum",
            "CS": "sum",
            "SH": "sum",
            "SF": "sum",
            "BB": "sum",
            "IBB": "sum",
            "HP": "sum",
            "SO": "sum",
            "GDP": "sum",
            "AVG": "first",
            "SLG": "first",
            "OBP": "first",
            "Team": "first",
        }
        self.df = self.df.groupby(self.df["Player"], as_index=False).agg(
            agg_functions
        )
        # Recalculate AVG, SLG, OBP
        if self.suffix == "BP":
            self.df["AVG"] = self.df["H"] / self.df["AB"]
            self.df["SLG"] = (
                (self.df["H"] - self.df["2B"] - self.df["3B"] - self.df["HR"])
                + (2 * self.df["2B"])
                + (3 * self.df["3B"])
                + (4 * self.df["HR"])
            ) / self.df["AB"]
            self.df["OBP"] = (
                self.df["H"] + self.df["BB"] + self.df["HP"]
            ) / self.df["PA"]
        # DEBUG
        # self.df.to_csv("test.csv",index=False)
        print(self.df.to_string())
        with open("output.txt", "w") as f:
            print(self.df.to_string(), file=f)

        # Translate player names TODO

        # Unnecessary data removal
        # Remove all players if their PA is 0
        self.df = self.df.drop(self.df[self.df.PA == 0].index)
        # Drop last column TODO remove?
        # self.df.drop(
        #    self.df.columns[len(self.df.columns) - 1], axis=1, inplace=True
        # )

        # Counting stat column totals used in other calculations
        totalAB = self.df["AB"].sum()
        totalH = self.df["H"].sum()
        total2B = self.df["2B"].sum()
        total3B = self.df["3B"].sum()
        totalHR = self.df["HR"].sum()
        totalSF = self.df["SF"].sum()
        totalBB = self.df["BB"].sum()
        totalHP = self.df["HP"].sum()
        totalOBP = (totalH + totalBB + totalHP) / (
            totalAB + totalBB + totalHP + totalSF
        )
        totalSLG = (
            (totalH - total2B - total3B - totalHR)
            + (2 * total2B)
            + (3 * total3B)
            + (4 * totalHR)
        ) / totalAB

        # Individual statistic calculations
        # Calculate OPS
        self.df["OPS"] = round(self.df["SLG"] + self.df["OBP"], 3)
        # Calculate OPS+
        self.df = select_park_factor(self.df, self.suffix, self.year)
        self.df["OPS+"] = round(
            100
            * ((self.df["OBP"] / totalOBP) + (self.df["SLG"] / totalSLG) - 1),
            0,
        )
        self.df["OPS+"] = self.df["OPS+"] / self.df["ParkF"]
        # Calculate ISO
        self.df["ISO"] = round(self.df["SLG"] - self.df["AVG"], 3)
        # Calculate K%
        self.df["K%"] = round(self.df["SO"] / self.df["PA"], 3)
        # Calculate BB%
        self.df["BB%"] = round(self.df["BB"] / self.df["PA"], 3)
        # Calculate BB/K
        self.df["BB/K"] = round(self.df["BB"] / self.df["SO"], 2)
        # Calculate TTO%
        self.df["TTO%"] = (
            self.df["BB"] + self.df["SO"] + self.df["HR"]
        ) / self.df["PA"]
        self.df["TTO%"] = self.df["TTO%"].apply("{:.1%}".format)
        # Calculate BABIP
        numer = self.df["H"] - self.df["HR"]
        denom = self.df["AB"] - self.df["SO"] - self.df["HR"] + self.df["SF"]
        self.df["BABIP"] = round((numer / denom), 3)

        # Remove temp Park Factor column
        self.df.drop("ParkF", axis=1, inplace=True)
        # "Mercedes Cristopher Crisostomo" name shortening to "Mercedes CC"
        self.df["Player"] = (
            self.df["Player"]
            .astype(str)
            .replace("Mercedes Cristopher Crisostomo", "Mercedes CC")
        )
        # "Tysinger Brandon Taiga" name shortening to "Tysinger Brandon"
        self.df["Player"] = (
            self.df["Player"]
            .astype(str)
            .replace("Mercedes Cristopher Crisostomo", "Mercedes CC")
        )
        # Number formatting
        formatMapping = {
            "BB%": "{:.1%}",
            "K%": "{:.1%}",
            "OPS+": "{:.0f}",
            "AVG": "{:.3f}",
            "OBP": "{:.3f}",
            "SLG": "{:.3f}",
            "OPS": "{:.3f}",
            "ISO": "{:.3f}",
            "BABIP": "{:.3f}",
            "BB/K": "{:.2f}",
        }
        for key, value in formatMapping.items():
            self.df[key] = self.df[key].apply(value.format)
        # Replace all NaN in BB/K, wOBA and BABIP with ''
        self.df["BB/K"] = self.df["BB/K"].astype(str)
        self.df["BB/K"] = self.df["BB/K"].str.replace("nan", "")
        self.df["BABIP"] = self.df["BABIP"].astype(str)
        self.df["BABIP"] = self.df["BABIP"].str.replace("nan", "")
        # Replace BB/K infs with '1.00' (same format as MLB website)
        self.df["BB/K"] = self.df["BB/K"].str.replace("inf", "1.00")
        # Column reordering
        self.df = self.df[
            [
                "Player",
                "G",
                "PA",
                "AB",
                "R",
                "H",
                "2B",
                "3B",
                "HR",
                "TB",
                "RBI",
                "SB",
                "CS",
                "SH",
                "SF",
                "SO",
                "BB",
                "IBB",
                "HP",
                "GDP",
                "AVG",
                "OBP",
                "SLG",
                "OPS",
                "OPS+",
                "ISO",
                "BABIP",
                "TTO%",
                "K%",
                "BB%",
                "BB/K",
                "Team",
            ]
        ]

        # Add "League" column
        self.df = select_league(self.df, self.suffix)


def get_playoff_stats(yearDir, suffix, year):
    """The main stat scraping function that produces Raw stat files.
    Saving Raw stat files allows for scraping and stat organization to be
    independent of each other. No scraping should be needed to make changes to
    final files

    Parameters:
    yearDir (string): The directory that stores the raw, scraped NPB stats
    suffix (string): Determines header row of csv file and indicates the stats
    that the URLs point to:
    "BP" = post season batting stat URLs passed in
    "PP" = post season pitching stat URLs passed in
    year (string): The desired npb year to scrape

    Returns: N/A"""
    # Make output file
    outputFile = make_raw_player_file(yearDir, suffix, year)
    # Grab URLs to scrape
    urlArr = get_stat_urls(suffix, year)
    # Create header row
    if suffix == "BP":
        outputFile.write(
            "Player,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SB,CS,SH,SF,BB,"
            "IBB,HP,SO,GDP,AVG,SLG,OBP,Team,\n"
        )
    if suffix == "PP":
        outputFile.write(
            "Pitcher,G,W,L,SV,HLD,CG,SHO,PCT,BF,IP,,H,HR,BB,IBB,"
            "HB,SO,WP,BK,R,ER,ERA,Team,\n"
        )

    # Loop through all team stat pages in urlArr
    for url in urlArr:
        # Make GET request
        r = get_url(url)
        # Create the soup for parsing the html content
        soup = BeautifulSoup(r.content, "html.parser")
        # Since header row was created, skip to stat rows
        iterSoup = iter(soup.table)
        # Left handed pitcher/batter and switch hitter row skip
        next(iterSoup)
        # npb.jp header row skip
        next(iterSoup)

        # Extract table rows from npb.jp team stats
        for tableRow in iterSoup:
            # Skip first column for left handed batter/pitcher or switch hitter
            iterTable = iter(tableRow)
            next(iterTable)
            # Write output in csv file format
            for entry in iterTable:
                # Remove commas in first and last names
                entryText = entry.get_text()
                if entryText.find(","):
                    entryText = entryText.replace(",", "")
                # Write output in csv file format
                outputFile.write(entryText + ",")

            # Get team
            titleDiv = soup.find(id="stdivtitle")
            yearTitleStr = titleDiv.h1.get_text()
            # Correct team name formatting
            yearTitleStr = yearTitleStr.replace(year, "")
            if yearTitleStr.find("年度 阪神タイガース"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 阪神タイガース", "Hanshin Tigers"
                )
            if yearTitleStr.find("年度 千葉ロッテマリーンズ"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 千葉ロッテマリーンズ", "Lotte Marines"
                )
            if yearTitleStr.find("年度 福岡ソフトバンクホークス"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 福岡ソフトバンクホークス", "SoftBank Hawks"
                )
            if yearTitleStr.find("年度 北海道日本ハムファイターズ"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 北海道日本ハムファイターズ", "Nipponham Fighters"
                )
            if yearTitleStr.find("年度 読売ジャイアンツ"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 読売ジャイアンツ", "Yomiuri Giants"
                )
            if yearTitleStr.find("年度 横浜DeNAベイスターズ"):
                yearTitleStr = yearTitleStr.replace(
                    "年度 横浜DeNAベイスターズ", "DeNA BayStars"
                )
            yearTitleStr = yearTitleStr.lstrip()
            yearTitleStr = yearTitleStr.rstrip()
            # Append as last entry and move to next row
            outputFile.write(yearTitleStr + ",\n")

        # Close request
        r.close()
        # Pace requests to npb.jp to avoid excessive requests
        sleep(randint(3, 5))
    # After all URLs are scraped, close output file
    outputFile.close()


def get_stat_urls(suffix, year):
    """Creates arrays of the correct URLs for the individual stat scraping

    Parameters:
    suffix (string): The desired mode to run in (either farm or regular season)
    year (string): The desired npb year to scrape

    Returns:
    urlArrBaseB (array - string): Contains URLs to the team batting/pitching
    stat pages"""
    if suffix == "BP":
        # Team regular season individual batting stats
        urlArrBase = [
            # Hanshin Tigers RD1
            "https://npb.jp/bis/2024/stats/idb1s1_t.html",
            # YOKOHAMA DeNA BAYSTARS RD1
            "https://npb.jp/bis/2024/stats/idb1s1_db.html",
            # Chiba Lotte Marines RD1
            "https://npb.jp/bis/2024/stats/idb1s1_m.html",
            # Hokkaido Nippon-Ham Fighters RD1
            "https://npb.jp/bis/2024/stats/idb1s1_f.html",
            # Fukuoka SoftBank Hawks RD2
            "https://npb.jp/bis/2024/stats/idb1s2_h.html",
            # Yomiuri Giants RD2
            "https://npb.jp/bis/2024/stats/idb1s2_g.html",
            # Hokkaido Nippon-Ham Fighters RD2
            "https://npb.jp/bis/2024/stats/idb1s2_f.html",
            # YOKOHAMA DeNA BAYSTARS RD2
            "https://npb.jp/bis/2024/stats/idb1s2_db.html",
            # YOKOHAMA DeNA BAYSTARS Final
            "https://npb.jp/bis/2024/stats/idb1ns_db.html",
            # Fukuoka SoftBank Hawks Final
            "https://npb.jp/bis/2024/stats/idb1ns_h.html",
        ]
    elif suffix == "PP":
        # Team regular season individual pitching stats
        urlArrBase = [
            # Hanshin Tigers RD1
            "https://npb.jp/bis/2024/stats/idp1s1_t.html",
            # YOKOHAMA DeNA BAYSTARS RD1
            "https://npb.jp/bis/2024/stats/idp1s1_db.html",
            # Chiba Lotte Marines RD1
            "https://npb.jp/bis/2024/stats/idp1s1_m.html",
            # Hokkaido Nippon-Ham Fighters RD1
            "https://npb.jp/bis/2024/stats/idp1s1_f.html",
            # Fukuoka SoftBank Hawks RD2
            "https://npb.jp/bis/2024/stats/idp1s2_h.html",
            # Yomiuri Giants RD2
            "https://npb.jp/bis/2024/stats/idp1s2_g.html",
            # Hokkaido Nippon-Ham Fighters RD2
            "https://npb.jp/bis/2024/stats/idp1s2_f.html",
            # YOKOHAMA DeNA BAYSTARS RD2
            "https://npb.jp/bis/2024/stats/idp1s2_db.html",
            # YOKOHAMA DeNA BAYSTARS Final
            "https://npb.jp/bis/2024/stats/idp1ns_db.html",
            # Fukuoka SoftBank Hawks Final
            "https://npb.jp/bis/2024/stats/idp1ns_h.html",
        ]

    # Loop through each entry and change the year in the URL before returning
    for i, url in enumerate(urlArrBase):
        urlArrBase[i] = urlArrBase[i].replace("2024", year)
    return urlArrBase


def get_url(tryUrl):
    """Attempts a GET request from the passed in URL

    Parameters:
    tryUrl (string): The URL to attempt opening

    Returns:
    response (Response): The URL's response"""
    try:
        print("Connecting to: " + tryUrl)
        response = requests.get(tryUrl)
        response.raise_for_status()
    # Page doesn't exist (404 not found, 403 not authorized, etc)
    except HTTPError as hp:
        print(hp)
    # Bad URL
    except URLError as ue:
        print(ue)
    return response


def make_raw_player_file(writeDir, suffix, year):
    """Opens a file to hold all player stats inside a relative /stats/
    directory that is created before calling this function

    Parameters:
    writeDir (string): The directory that stores the scraped NPB stats
    suffix (string): Indicates the raw stat file to create:
    "BP" = post season batting stats
    "PP" = post season pitching stats
    year (string): The desired npb year to scrape

    Returns:
    newFile (file stream object): An opened file in /stats/ named
    "[Year][Stats][Suffix].csv"""
    # Open and return the file object in write mode
    newCsvName = writeDir + "/" + year + "StatsRaw" + suffix + ".csv"
    if suffix == "BP":
        print(
            "Raw post season batting results will be stored in: " + newCsvName
        )
    if suffix == "PP":
        print(
            "Raw post season pitching results will be stored in: " + newCsvName
        )
    newFile = open(newCsvName, "w")
    return newFile


def get_scrape_year(argsIn=None):
    """Checks passed in arguments or gets user input for NPB stat year to
    scrape

    Parameters:
    argsIn (string): If a command line argument is given, the year is checked
    for validity. Default (None) indicates to collect user input instead

    Returns:
    argsIn (string): The desired npb stat year to scrape"""
    # User input check
    if argsIn is None:
        # Infinite loop breaks when valid input obtained
        # Either valid year or exit signal entered
        while True:
            argsIn = input(
                "Enter a NPB year between 2020-"
                + str(datetime.now().year)
                + " or Q to quit: "
            )
            if argsIn == "Q":
                sys.exit("Exiting...")
            try:
                argsIn = int(argsIn)
            except ValueError:
                print("Input must be a number (Example: 2024)")
                continue
            # Bounds for scrapable years
            # Min year on npb.jp = 2008, but scraping is only tested until 2020
            if 2020 <= argsIn <= datetime.now().year:
                print(str(argsIn) + " entered. Continuing...")
                break
            else:
                print(
                    "Please enter a valid year (2020-"
                    + str(datetime.now().year)
                    + ")."
                )
    # Argument check
    else:
        try:
            argsIn = int(argsIn)
        except ValueError:
            print("Year argument must be a number (Example: 2024)")
            sys.exit("Exiting...")
        # Bounds for scrapable years
        # Min year on npb.jp is 2008, but scraping is only tested until 2020
        if 2020 <= argsIn <= datetime.now().year:
            pass
        else:
            print(
                "Please enter a valid year (2020-"
                + str(datetime.now().year)
                + ")."
            )
            sys.exit("Exiting...")

    # Return user input as a string
    return str(argsIn)


def get_user_choice(suffix):
    """Gets user input for whether or not to undergo scraping and whether to
    place relevant files in a zip

    Parameters:
    suffix (string): Indicates the option being asked about (post season
    scraping "P" or zip file creation "Z")

    Returns:
    userIn (string): Returns "Y" or "N" (if "Q" is chosen, program terminates)
    """
    # Loop ends for valid choice/exit
    while True:
        if suffix == "P":
            print(
                "Choose whether to pull new post season stats from "
                "npb.jp or only reorganize existing stat files.\nWARNING: "
                "EXISTING RAW STAT FILES MUST BE PRESENT TO SKIP SCRAPING."
            )
            userIn = input("Scrape regular season stats stats? (Y/N): ")
        elif suffix == "Z":
            userIn = input(
                "Output these stats in a zip file for manual "
                "upload? (Y/N): "
            )

        if userIn == "Q":
            sys.exit("Exiting...")
        elif userIn == "Y":
            print("Continuing...")
            break
        elif userIn == "N":
            print("Skipping...")
            break
        else:
            print(
                "Invalid input - enter (Y/N) to determine whether to scrape "
                "or (Q) to quit."
            )
            continue
    return userIn


def convert_ip_column_out(df):
    """In baseball, IP stats are traditionally represented using .1 (single
    inning pitched), .2 (2 innings pitched), and whole numbers. This function
    converts the decimals FROM thirds (.33 -> .1, .66 -> .2) for sake of
    presentation

    Parameters:
    df (pandas dataframe): A pitching stat dataframe with the "thirds"
    representation

    Returns:
    tempDf['IP'] (pandas dataframe column): An IP column converted back to the
    original IP representation"""
    # IP ".0 .1 .2" fix
    tempDf = pd.DataFrame(df["IP"])
    # Get the ".0 .3 .7" in the 'IP' column
    ipDecimals = tempDf["IP"] % 1
    # Make the original 'IP' column whole numbers
    tempDf["IP"] = tempDf["IP"] - ipDecimals
    # Convert IP decimals to thirds and re-add them to the whole numbers
    ipDecimals = (ipDecimals / 0.3333333333) / 10
    df["IP"] = tempDf["IP"] + ipDecimals
    # Entries with .3 are invalid: add 1 and remove the decimals
    x = tempDf["IP"] + ipDecimals
    condlist = [((x % 1) < 0.29), ((x % 1) >= 0.29)]
    choicelist = [x, (x - (x % 1)) + 1]
    tempDf["IP"] = np.select(condlist, choicelist)
    tempDf["IP"] = tempDf["IP"].apply("{:.1f}".format)
    tempDf["IP"] = tempDf["IP"].astype(float)
    return tempDf["IP"]


def convert_ip_column_in(df):
    """Converts the decimals in the IP column TO thirds (.1 -> .33, .2 -> .66)
    for stat calculations

    Parameters:
    df (pandas dataframe): A pitching stat dataframe with the traditional
    .1/.2 IP representation

    Returns:
    tempDf['IP'] (pandas dataframe column): An IP column converted for stat
    calculations"""
    tempDf = pd.DataFrame(df["IP"])
    # Get the ".0 .1 .2" in the 'IP' column
    ipDecimals = tempDf["IP"] % 1
    # Make the original 'IP' column whole numbers
    tempDf["IP"] = tempDf["IP"] - ipDecimals
    # Multiply IP decimals by .3333333333 and readd them to the whole numbers
    ipDecimals = (ipDecimals * 10) * 0.3333333333
    tempDf["IP"] = tempDf["IP"] + ipDecimals
    return tempDf["IP"]


def select_park_factor(df, suffix, year):
    """Selects the correct park factor depending on the NPB year and team

    Parameters:
    df (pandas dataframe): The dataframe to add the park factor column to
    suffix (string): Indicates whether to use farm or reg season park factors
    year (string): The year of park factors to pull

    Returns:
    df (pandas dataframe): The pandas dataframe with the new temp park factor
    column"""
    # Check for the park factor file, if nothing is there tell user and return
    relDir = os.path.dirname(__file__)
    pfFile = relDir + "/input/parkFactors.csv"
    if not (os.path.exists(pfFile)):
        print(
            "\nERROR: No park factor file found, calculations using park "
            "factors will be inaccurate...\nProvide a valid parkFactors.csv "
            "file in the /input/ directory to fix this.\n"
        )
        df["ParkF"] = np.nan
        return df

    pfDf = pd.read_csv(pfFile)
    # Drop all rows that are not the df's year
    pfDf = pfDf.drop(pfDf[pfDf.Year.astype(str) != year].index)
    # Drop all rows that do not match the df's league
    if suffix == "BP" or suffix == "PP":
        pfSuffix = "NPB"
    else:
        pfSuffix = "Farm"
    pfDf = pfDf.drop(pfDf[pfDf.League != pfSuffix].index)
    # Drop remaining unneeded cols before merge
    pfDf.drop(["Year", "League"], axis=1, inplace=True)
    # Modifying all park factors for calculations
    pfDf["ParkF"] = (pfDf["ParkF"] + 1) / 2
    df = df.merge(pfDf, on="Team")
    # For team files, league avg calculations have park factor as 1.000
    df.loc[df.Team == "League Average", "ParkF"] = 1.000

    return df


def select_fip_const(suffix, year):
    """Chooses FIP constant for 2020-2024 reg and farm years

    Parameters:
    suffix (string): Indicates whether to use farm or reg season FIP constants
    year (string): The year of FIP constants to pull

    Returns:
    fipConst (float): The correct FIP const according to year and farm/NPB reg
    season"""
    # Check for the player link file, if nothing is there tell user and return
    relDir = os.path.dirname(__file__)
    fipFile = relDir + "/input/fipConst.csv"
    if not (os.path.exists(fipFile)):
        print(
            "\nERROR: No FIP constant file found, calculations using FIP will "
            "be inaccurate...\nProvide a valid fipConst.csv file in the "
            "/input/ directory to fix this.\n"
        )
        fipConst = np.nan
        return fipConst

    fipDf = pd.read_csv(fipFile)
    # Drop all rows that are not the df's year
    fipDf = fipDf.drop(fipDf[fipDf.Year.astype(str) != year].index)
    # Drop all rows that do not match the df's league
    if suffix == "BP" or suffix == "PP":
        fipSuffix = "NPB"
    else:
        fipSuffix = "Farm"
    fipDf = fipDf.drop(fipDf[fipDf.League != fipSuffix].index)
    # Return FIP for that year and league
    fipConst = fipDf.at[fipDf.index[-1], "FIP"]
    return fipConst


def select_league(df, suffix):
    """Adds a "League" column based on the team

    Parameters:
    df (pandas dataframe): A team or player dataframe

    Returns:
    df (pandas dataframe): The dataframe with the correct "League" column added
    """
    if suffix == "BP" or suffix == "PP":
        # Contains all 2020-2024 reg baseball team names and leagues
        leagueDict = {
            "Hanshin Tigers": "CL",
            "Hiroshima Carp": "CL",
            "DeNA BayStars": "CL",
            "Yomiuri Giants": "CL",
            "Yakult Swallows": "CL",
            "Chunichi Dragons": "CL",
            "ORIX Buffaloes": "PL",
            "Lotte Marines": "PL",
            "SoftBank Hawks": "PL",
            "Rakuten Eagles": "PL",
            "Seibu Lions": "PL",
            "Nipponham Fighters": "PL",
        }

    for team in leagueDict:
        df.loc[df.Team == team, "League"] = leagueDict[team]
    return df


def convert_player_to_html(df, suffix, year):
    """The WordPress tables associated with this project accepts HTML code, so
    this function formats player names into <a> tags with links to the player's
    npb.jp pages. Used after stats are calculated but before any csv output

    Parameters:
    df (pandas dataframe): Any final stat dataframe
    suffix (string): Indicates the data in param df
        "BR" = reg season batting stat URLs passed in
        "PR" = reg season pitching stat URLs passed in
        "BF" = farm batting stat URLs passed in
        "PF" = farm pitching stat URLs passed in
    year (string): Indicates the stat year for df

    Returns:
    df (pandas dataframe): The final stat dataframe with valid HTML in the
    player/pitcher columns
    """
    relDir = os.path.dirname(__file__)
    playerLinkFile = relDir + "/input/playerUrls.csv"
    if not (os.path.exists(playerLinkFile)):
        print(
            "\nERROR: No player link file found, table entries will not "
            "have links...\nProvide a playerUrls.csv file in the /input/ "
            "directory to fix this.\n"
        )
        return df

    # Read in csv that contains player name and their personal page link
    linkDf = pd.read_csv(playerLinkFile)
    # Create new HTML code column
    linkDf["Link"] = linkDf.apply(build_html, axis=1)
    # Create dict of Player Name:Complete HTML tag
    playerDict = dict(linkDf.values)

    # Replace all team entries with HTML that leads to their pages
    if suffix == "PP":
        convertCol = "Pitcher"
    else:
        convertCol = "Player"
    df[convertCol] = (
        df[convertCol]
        .map(playerDict)
        .infer_objects()
        .fillna(df[convertCol])
        .astype(str)
    )

    # Check for the player link fix file
    playerLinkFixFile = relDir + "/input/playerUrlsFix.csv"
    if os.path.exists(playerLinkFixFile):
        fixDf = pd.read_csv(playerLinkFixFile)
        # Check year and suffix, fix if needed
        if int(year) in fixDf.Year.values and suffix in fixDf.Suffix.values:
            # Create dict of Player Name:Complete HTML tag
            fixDict = dict(zip(fixDf["Original"], fixDf["Corrected"]))
            df[convertCol] = (
                df[convertCol]
                .map(fixDict)
                .infer_objects()
                .fillna(df[convertCol])
                .astype(str)
            )

    return df


def convert_team_to_html(df, mode):
    """Formats the team names to include links to their npb.jp pages

    Parameters:
    df (pandas dataframe): A dataframe containing entries with NPB teams
    mode (string): Indicates whether to preserve full team names (pass in
    "Full") or also insert short names in the <a> tags (pass in "Abb")

    Returns:
    df (pandas dataframe): The dataframe with correct links and abbrieviations
    inserted as <a> tags"""
    # Check for the team link file, if missing, tell user and return
    relDir = os.path.dirname(__file__)
    teamLinkFile = relDir + "/input/teamUrls.csv"
    if not (os.path.exists(teamLinkFile)):
        print(
            "\nWARNING: No team link file found, table entries will not have "
            "links...\nProvide a teamUrls.csv file in the /input/ directory to"
            " fix this to fix this.\n"
        )
        return df

    linkDf = pd.read_csv(teamLinkFile)
    if mode == "Full":
        # Update Link col to have <a> tags
        linkDf["Link"] = linkDf.apply(build_html, axis=1)
    elif mode == "Abb":
        # Contains 2020-2024 reg/farm baseball team abbrieviations
        abbrDict = {
            "Hanshin Tigers": "Hanshin",
            "Hiroshima Carp": "Hiroshima",
            "DeNA BayStars": "DeNA",
            "Yomiuri Giants": "Yomiuri",
            "Yakult Swallows": "Yakult",
            "Chunichi Dragons": "Chunichi",
            "ORIX Buffaloes": "ORIX",
            "Lotte Marines": "Lotte",
            "SoftBank Hawks": "SoftBank",
            "Rakuten Eagles": "Rakuten",
            "Seibu Lions": "Seibu",
            "Nipponham Fighters": "Nipponham",
            "Oisix Albirex": "Oisix",
            "HAYATE Ventures": "HAYATE",
        }
        # Create temp col to have abbrieviations
        linkDf["Temp"] = (
            linkDf["Team"]
            .map(abbrDict)
            .infer_objects()
            .fillna(linkDf["Team"])
            .astype(str)
        )
        # Swap full name col with abb col to create HTML tags with abb names
        linkDf["Team"], linkDf["Temp"] = linkDf["Temp"], linkDf["Team"]
        linkDf["Link"] = linkDf.apply(build_html, axis=1)
        # Swap full name col back to original spot and delete temp col
        linkDf["Temp"], linkDf["Team"] = linkDf["Team"], linkDf["Temp"]
        linkDf = linkDf.drop("Temp", axis=1)
        # Add new, unlinked farm team abbrieviations to dataframe
        newRow = {"Team": "Oisix Albirex", "Link": "Oisix"}
        linkDf = linkDf._append(newRow, ignore_index=True)
        newRow = {"Team": "HAYATE Ventures", "Link": "HAYATE"}
        linkDf = linkDf._append(newRow, ignore_index=True)

    # Create dict of Team Name:Complete HTML tag and convert
    teamDict = dict(linkDf.values)
    for column in df:
        df[column] = (
            df[column]
            .map(teamDict)
            .infer_objects()
            .fillna(df[column])
            .astype(str)
        )

    return df


def build_html(row):
    """Insert the link and text in a <a> tag, returns the tag as a string"""
    htmlLine = "<a href=" "{0}" ">{1}</a>".format(row["Link"], row.iloc[0])
    return htmlLine


if __name__ == "__main__":
    main()
