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


class Stats:
    def __init__(self, statsDir, yearDir, suffix, year):
        self.statsDir = statsDir
        self.suffix = suffix
        self.year = year
        self.yearDir = yearDir


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


if __name__ == "__main__":
    main()
