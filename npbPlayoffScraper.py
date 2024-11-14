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
    postBatTeamStats = TeamData(
        postBatPlayerStats.df, statsDir, yearDir, "BP", scrapeYear
    )
    postPitchTeamStats = TeamData(
        postPitchPlayerStats.df, statsDir, yearDir, "PP", scrapeYear
    )
    postBatPlayerStats.output_final()
    postPitchPlayerStats.output_final()
    postBatTeamStats.output_final()
    postPitchTeamStats.output_final()

    # Asking user to make an upload zip for manual uploads
    # TODO: Remove choice and auto output zips?
    zipYN = get_user_choice("Z")
    if zipYN == "Y":
        make_zip(yearDir, scrapeYear)
    input("Press Enter to exit. ")


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

        # Translate player names TODO

        # Some ERA entries can be '----', replace with 0 and convert to float
        # for calculations
        self.df["ERA"] = self.df["ERA"].astype(str).replace("----", "inf")
        self.df["ERA"] = self.df["ERA"].astype(float)
        # Drop BK, HLD, and PCT columns
        self.df.drop(["BK", "PCT", "HLD"], axis=1, inplace=True)
        # IP ".0 .1 .2" fix
        self.df["IP"] = convert_ip_column_in(self.df)
        # Recalculate ERA
        self.df["ERA"] = (9 * self.df["ER"]) / self.df["IP"]

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

        # Translate player names TODO

        # Unnecessary data removal
        # Remove all players if their PA is 0
        self.df = self.df.drop(self.df[self.df.PA == 0].index)

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


class TeamData(Stats):
    def __init__(self, playerDf, statsDir, yearDir, suffix, year):
        """TeamData new variables:
        playerDf (pandas dataframe): Holds an entire NPB league's individual
        batting/pitching stats"""
        super().__init__(statsDir, yearDir, suffix, year)
        self.playerDf = playerDf.copy()
        # Initialize df for teams stats
        if self.suffix == "BP":
            self.org_team_bat()
        elif self.suffix == "PP":
            self.org_team_pitch()

    def __str__(self):
        """Outputs the Alt view of the associated dataframe (no HTML
        team or player names, no csv formatting, shows entire df instead of
        only Leaders)"""
        return self.df.to_string()

    def output_final(self):
        """Outputs final files for upload using the team stat dataframes
        (CONVERSION OF writeTeamStats())

        Parameters: N/A

        Returns: N/A"""
        # Fix NaNs in League col
        self.df["League"] = self.df["League"].fillna("")
        # Make dir that will store alt views of the dataframes
        altDir = os.path.join(self.yearDir, "alt")
        if not (os.path.exists(altDir)):
            os.mkdir(altDir)
        # Make dirs that will store files uploaded to yakyucosmo.com
        uploadDir = self.yearDir
        if self.suffix == "PP" or self.suffix == "BP":
            uploadDir = os.path.join(self.yearDir, "npb")
            if not (os.path.exists(uploadDir)):
                os.mkdir(uploadDir)
        # Print organized dataframe to file
        newCsvAlt = altDir + "/" + self.year + "TeamAlt" + self.suffix + ".csv"
        self.df.to_string(newCsvAlt)
        # Make output copy to avoid modifying original df
        finalDf = self.df.copy()
        # Insert HTML code for team names
        finalDf = convert_team_to_html(finalDf, "Full")
        # Print output file for upload
        newCsvFinal = (
            uploadDir + "/" + self.year + "Team" + self.suffix + ".csv"
        )
        finalDf.to_csv(newCsvFinal, index=False)

        # Pitching TeamAlt and Team file location outputs
        if self.suffix == "PP" or self.suffix == "BP":
            print(
                "The final organized team pitching results will be stored "
                "in: " + newCsvFinal
            )
            print(
                "An alternative view of team pitching results will be stored "
                "in: " + newCsvAlt
            )

    def org_team_bat(self):
        """Outputs batting team stat files using the organized player stat
        dataframes (CONVERSION OF batTeamOrg())

        Parameters: N/A

        Returns: N/A"""
        # Initialize new row list with all possible teams
        rowArr = [
            "Hanshin Tigers",
            "Hiroshima Carp",
            "DeNA BayStars",
            "Yomiuri Giants",
            "Yakult Swallows",
            "Chunichi Dragons",
            "ORIX Buffaloes",
            "Lotte Marines",
            "SoftBank Hawks",
            "Rakuten Eagles",
            "Seibu Lions",
            "Nipponham Fighters",
        ]

        # Initialize a list and put team columns in first
        # REFACTOR (?)
        colArr = [
            "Team",
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
        ]
        teamBatList = []
        teamBatList.append(colArr)

        # Form team stat rows
        # REFACTOR (?)
        for row in rowArr:
            newTeamStat = [row]
            tempStatDf = self.playerDf[self.playerDf.Team == row]
            # Skip teams that didn't play (PA = 0)
            if tempStatDf["PA"].sum() == 0:
                continue
            newTeamStat.append(tempStatDf["PA"].sum())
            newTeamStat.append(tempStatDf["AB"].sum())
            newTeamStat.append(tempStatDf["R"].sum())
            newTeamStat.append(tempStatDf["H"].sum())
            newTeamStat.append(tempStatDf["2B"].sum())
            newTeamStat.append(tempStatDf["3B"].sum())
            newTeamStat.append(tempStatDf["HR"].sum())
            newTeamStat.append(tempStatDf["TB"].sum())
            newTeamStat.append(tempStatDf["RBI"].sum())
            newTeamStat.append(tempStatDf["SB"].sum())
            newTeamStat.append(tempStatDf["CS"].sum())
            newTeamStat.append(tempStatDf["SH"].sum())
            newTeamStat.append(tempStatDf["SF"].sum())
            newTeamStat.append(tempStatDf["SO"].sum())
            newTeamStat.append(tempStatDf["BB"].sum())
            newTeamStat.append(tempStatDf["IBB"].sum())
            newTeamStat.append(tempStatDf["HP"].sum())
            newTeamStat.append(tempStatDf["GDP"].sum())
            totalH = tempStatDf["H"].sum()
            total2B = tempStatDf["2B"].sum()
            total3B = tempStatDf["3B"].sum()
            totalHR = tempStatDf["HR"].sum()
            totalSF = tempStatDf["SF"].sum()
            totalBB = tempStatDf["BB"].sum()
            totalHP = tempStatDf["HP"].sum()
            totalAB = tempStatDf["AB"].sum()
            totalAVG = round((totalH / totalAB), 3)
            newTeamStat.append(totalAVG)
            totalOBP = round(
                (
                    (totalH + totalBB + totalHP)
                    / (totalAB + totalBB + totalHP + totalSF)
                ),
                3,
            )
            newTeamStat.append(totalOBP)
            tempSLG1 = totalH - total2B - total3B - totalHR
            tempSLG2 = (2 * total2B) + (3 * total3B) + (4 * totalHR)
            totalSLG = round((((tempSLG1 + tempSLG2) / totalAB)), 3)
            newTeamStat.append(totalSLG)
            totalOPS = round((totalOBP + totalSLG), 3)
            newTeamStat.append(totalOPS)
            teamBatList.append(newTeamStat)

        teamConst = 12
        # Getting league stat totals (last row to be appended to the dataframe)
        newTeamStat = ["League Average"]
        newTeamStat.append(round(self.playerDf["PA"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["AB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["R"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["H"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["2B"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["3B"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["HR"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["TB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["RBI"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["CS"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SH"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SF"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SO"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["BB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["IBB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["HP"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["GDP"].sum() / teamConst, 0))
        totalH = self.playerDf["H"].sum()
        total2B = self.playerDf["2B"].sum()
        total3B = self.playerDf["3B"].sum()
        totalHR = self.playerDf["HR"].sum()
        totalSF = self.playerDf["SF"].sum()
        totalBB = self.playerDf["BB"].sum()
        totalHP = self.playerDf["HP"].sum()
        totalAB = self.playerDf["AB"].sum()
        totalAVG = round((totalH / totalAB), 3)
        newTeamStat.append(totalAVG)
        totalOBP = round(
            (
                (totalH + totalBB + totalHP)
                / (totalAB + totalBB + totalHP + totalSF)
            ),
            3,
        )
        newTeamStat.append(totalOBP)
        totalSLG = round(
            (
                (
                    (
                        (totalH - total2B - total3B - totalHR)
                        + (2 * total2B)
                        + (3 * total3B)
                        + (4 * totalHR)
                    )
                    / totalAB
                )
            ),
            3,
        )
        newTeamStat.append(totalSLG)
        totalOPS = round((totalOBP + totalSLG), 3)
        newTeamStat.append(totalOPS)
        teamBatList.append(newTeamStat)

        # Initialize new team stat dataframe
        self.df = pd.DataFrame(teamBatList)
        # Import adds extra top row of numbers which misaligns columns
        # Replace with proper row and drop extra number row
        self.df.columns = self.df.iloc[0]
        self.df.drop(index=0, axis=1, inplace=True)
        # Create park factors for any remaining team stats
        self.df = select_park_factor(self.df, self.suffix, self.year)

        # Total OPS of the teams / total OPS of the league
        self.df["OPS+"] = round(
            100
            * ((self.df["OBP"] / totalOBP) + (self.df["SLG"] / totalSLG) - 1),
            0,
        )
        self.df["OPS+"] = self.df["OPS+"] / self.df["ParkF"]
        self.df["ISO"] = round(self.df["SLG"] - self.df["AVG"], 3)
        self.df["K%"] = round(self.df["SO"] / self.df["PA"], 3)
        self.df["BB%"] = round(self.df["BB"] / self.df["PA"], 3)
        self.df["BB/K"] = round(self.df["BB"] / self.df["SO"], 2)
        self.df["TTO%"] = (
            self.df["BB"] + self.df["SO"] + self.df["HR"]
        ) / self.df["PA"]
        self.df["TTO%"] = self.df["TTO%"].apply("{:.1%}".format)
        numer = self.df["H"] - self.df["HR"]
        denom = self.df["AB"] - self.df["SO"] - self.df["HR"] + self.df["SF"]
        self.df["BABIP"] = round((numer / denom), 3)

        # Remove temp Park Factor column
        self.df.drop("ParkF", axis=1, inplace=True)
        # Number formatting
        formatMapping = {
            "BB%": "{:.1%}",
            "K%": "{:.1%}",
            "AVG": "{:.3f}",
            "OBP": "{:.3f}",
            "SLG": "{:.3f}",
            "OPS": "{:.3f}",
            "ISO": "{:.3f}",
            "BABIP": "{:.3f}",
            "BB/K": "{:.2f}",
            "OPS+": "{:.0f}",
            "PA": "{:.0f}",
            "AB": "{:.0f}",
            "2B": "{:.0f}",
            "3B": "{:.0f}",
            "TB": "{:.0f}",
            "RBI": "{:.0f}",
            "SB": "{:.0f}",
            "CS": "{:.0f}",
            "SH": "{:.0f}",
            "SF": "{:.0f}",
            "HP": "{:.0f}",
            "GDP": "{:.0f}",
            "H": "{:.0f}",
            "HR": "{:.0f}",
            "SO": "{:.0f}",
            "BB": "{:.0f}",
            "IBB": "{:.0f}",
            "R": "{:.0f}",
        }
        for key, value in formatMapping.items():
            self.df[key] = self.df[key].apply(value.format)

        # Add "League" column
        self.df = select_league(self.df, self.suffix)

    def org_team_pitch(self):
        """Outputs pitching team stat files using the organized player stat
        dataframes (CONVERSION OF pitchTeamOrg())

        Parameters: N/A

        Returns: N/A"""
        # IP column ".1 .2 .3" calculation fix
        self.playerDf["IP"] = convert_ip_column_in(self.playerDf)
        # Initialize new row list with all possible teams
        rowArr = [
            "Hanshin Tigers",
            "Hiroshima Carp",
            "DeNA BayStars",
            "Yomiuri Giants",
            "Yakult Swallows",
            "Chunichi Dragons",
            "ORIX Buffaloes",
            "Lotte Marines",
            "SoftBank Hawks",
            "Rakuten Eagles",
            "Seibu Lions",
            "Nipponham Fighters",
        ]

        # Initialize a list and put columns in first
        teamPitList = [
            [
                "Team",
                "W",
                "L",
                "SV",
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
            ]
        ]

        # Form team stat rows and collect all COUNTING stats
        # TODO: REFACTOR (?)
        teamConst = 0
        for row in rowArr:
            newTeamStat = [row]
            tempStatDf = self.playerDf[self.playerDf.Team == row]
            if tempStatDf["IP"].sum() == 0:
                continue
            newTeamStat.append(tempStatDf["W"].sum())
            newTeamStat.append(tempStatDf["L"].sum())
            newTeamStat.append(tempStatDf["SV"].sum())
            newTeamStat.append(tempStatDf["CG"].sum())
            newTeamStat.append(tempStatDf["SHO"].sum())
            newTeamStat.append(tempStatDf["BF"].sum())
            newTeamStat.append(tempStatDf["IP"].sum())
            newTeamStat.append(tempStatDf["H"].sum())
            newTeamStat.append(tempStatDf["HR"].sum())
            newTeamStat.append(tempStatDf["SO"].sum())
            newTeamStat.append(tempStatDf["BB"].sum())
            newTeamStat.append(tempStatDf["IBB"].sum())
            newTeamStat.append(tempStatDf["HB"].sum())
            newTeamStat.append(tempStatDf["WP"].sum())
            newTeamStat.append(tempStatDf["R"].sum())
            newTeamStat.append(tempStatDf["ER"].sum())
            teamPitList.append(newTeamStat)
            teamConst = teamConst + 1

        # Getting league stat averages for rate stats (last row to be appended)
        newTeamStat = ["League Average"]
        newTeamStat.append(round(self.playerDf["W"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["L"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SV"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["CG"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SHO"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["BF"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["IP"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["H"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["HR"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["SO"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["BB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["IBB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["HB"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["WP"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["R"].sum() / teamConst, 0))
        newTeamStat.append(round(self.playerDf["ER"].sum() / teamConst, 0))
        teamPitList.append(newTeamStat)

        # League totals that are needed for other calculations
        totalIP = self.playerDf["IP"].sum()
        totalHR = self.playerDf["HR"].sum()
        totalSO = self.playerDf["SO"].sum()
        totalBB = self.playerDf["BB"].sum()
        totalHB = self.playerDf["HB"].sum()
        totalER = self.playerDf["ER"].sum()
        totalBF = self.playerDf["BF"].sum()
        totalERA = 9 * (totalER / totalIP)

        # Initialize new team stat dataframe
        self.df = pd.DataFrame(teamPitList)
        # Import adds extra top row of numbers which misaligns columns,
        # replace with proper row and drop extra number row
        self.df.columns = self.df.iloc[0]
        self.df.drop(index=0, axis=1, inplace=True)
        # Create park factor col to use for any remaining team stats
        self.df = select_park_factor(self.df, self.suffix, self.year)
        # League totals have park factor as 1.000
        self.df["ParkF"] = self.df["ParkF"].replace(0.000, 1.000)

        # Calculations for RATE stats
        self.df["ERA"] = round(9 * (self.df["ER"] / self.df["IP"]), 2)
        self.df["ERA+"] = 100 * (totalERA * self.df["ParkF"]) / self.df["ERA"]
        self.df["kwERA"] = round(
            (4.80 - (10 * ((self.df["SO"] - self.df["BB"]) / self.df["BF"]))),
            2,
        )
        totalkwERA = round((4.80 - (10 * ((totalSO - totalBB) / totalBF))), 2)
        self.df["K%"] = round(self.df["SO"] / self.df["BF"], 3)
        self.df["BB%"] = round(self.df["BB"] / self.df["BF"], 3)
        self.df["K-BB%"] = round(self.df["K%"] - self.df["BB%"], 3)
        temp1 = 13 * self.df["HR"]
        temp2 = 3 * (self.df["BB"] + self.df["HB"])
        temp3 = 2 * self.df["SO"]
        self.df["FIP"] = round(
            ((temp1 + temp2 - temp3) / self.df["IP"])
            + select_fip_const(self.suffix, self.year),
            2,
        )
        temp1 = 13 * totalHR
        temp2 = 3 * (totalBB + totalHB)
        temp3 = 2 * totalSO
        totalFIP = ((temp1 + temp2 - temp3) / totalIP) + select_fip_const(
            self.suffix, self.year
        )
        self.df["FIP-"] = round(
            (100 * (self.df["FIP"] / (totalFIP * self.df["ParkF"]))), 0
        )
        self.df["WHIP"] = round(
            (self.df["BB"] + self.df["H"]) / self.df["IP"], 2
        )
        self.df["Diff"] = self.df["ERA"] - self.df["FIP"]
        self.df["HR%"] = self.df["HR"] / self.df["BF"]
        self.df["kwERA-"] = round((100 * (self.df["kwERA"] / totalkwERA)), 0)

        # Remove temp Park Factor column
        self.df.drop("ParkF", axis=1, inplace=True)
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
            "kwERA-": "{:.1f}",
            "ERA+": "{:.0f}",
            "FIP-": "{:.0f}",
            "W": "{:.0f}",
            "L": "{:.0f}",
            "SV": "{:.0f}",
            "CG": "{:.0f}",
            "SHO": "{:.0f}",
            "BF": "{:.0f}",
            "H": "{:.0f}",
            "HR": "{:.0f}",
            "SO": "{:.0f}",
            "BB": "{:.0f}",
            "IBB": "{:.0f}",
            "HB": "{:.0f}",
            "WP": "{:.0f}",
            "R": "{:.0f}",
            "ER": "{:.0f}",
        }
        for key, value in formatMapping.items():
            self.df[key] = self.df[key].apply(value.format)
        # Changing .33 to .1 and .66 to .2 in the IP column
        self.df["IP"] = convert_ip_column_out(self.df)
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
    # Drop entries with incorrect year and suffix
    # Check for the player link file, if nothing is there tell user and return
    relDir = os.path.dirname(__file__)
    playoffUrlFile = relDir + "/input/playoffUrls.csv"
    if not (os.path.exists(playoffUrlFile)):
        print(
            "\nERROR: No playoff URL file found, no links to scrape...\n"
            "Provide a valid playoffUrls.csv file in the /input/ directory to "
            "fix this.\n"
        )
        urlArrBase = np.nan
        return urlArrBase

    urlDf = pd.read_csv(playoffUrlFile)
    # Drop all rows that are not the df's year
    urlDf = urlDf.drop(urlDf[urlDf.Year.astype(str) != year].index)
    # Drop all rows that do not match the requested stats (batting/pitching)
    urlDf = urlDf.drop(urlDf[urlDf.Suffix != suffix].index)
    # Return URL arr for that year and stat type
    urlArrBase = urlDf["Link"]
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
    pfSuffix = "NPB"
    pfDf = pfDf.drop(pfDf[pfDf.League != pfSuffix].index)
    # Drop remaining unneeded cols before merge
    pfDf.drop(["Year", "League"], axis=1, inplace=True)
    # Modifying all park factors for calculations
    pfDf["ParkF"] = (pfDf["ParkF"] + 1) / 2
    df = df.merge(pfDf, on="Team", how="left")
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
    # TODO: FIP const for playoffs, choose between reg or farm
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

    # TODO: insert translation code here?

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


def make_zip(yearDir, year):
    """Groups a year's farm and npb directories in to a single zip for
    uploading/sending

    Parameters:
    yearDir (string): The directory that stores the raw, scraped NPB stats
    year (string): The year of npb stats to group together

    Returns: N/A"""
    tempDir = os.path.join(yearDir, "/stats/temp")
    tempDir = tempfile.mkdtemp()
    # Gather relevant dir to put into temp
    shutil.copytree(
        yearDir + "/npb", tempDir + "/stats/npb", dirs_exist_ok=True
    )
    # Create upload zip
    outputFilename = yearDir + "/" + year + "upload"
    dirName = tempDir
    shutil.make_archive(outputFilename, "zip", dirName)
    shutil.rmtree(tempDir)
    # Output name of upload zip
    print("Upload zip can be found at: " + outputFilename + ".zip")


if __name__ == "__main__":
    main()
