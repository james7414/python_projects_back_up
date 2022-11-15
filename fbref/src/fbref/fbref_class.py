"""Script used to help fetch data grabbed from FBref site"""

import pandas as pd
import numpy as np

from src.config.fbref_config import (
    LEAGUE_TABLE_COLUMNS,
)

from src.utility.functions import (
    flatten_cols,
    rename_unnamed_columns,
)


class FBref:
    """FBref class used to fetch data from FBref website"""

    def get_fbref_league_team_data(self, season_name, league_id, league_name):
        """Function used to grab league tables for a specific league e.g
        Premier league"""
        year1, year2 = season_name.split("_")

        league_data = pd.read_html(
            f"https://fbref.com/en/comps/{league_id}/{year1}-{year2}/"
            + f"{year1}-{year2}-{league_name}-Stats"
        )

        for table in league_data:
            if isinstance(table.columns, pd.MultiIndex):
                table = flatten_cols(table)
                table = rename_unnamed_columns(table)

        league_team_dict = {}

        league_team_dict["league_table"] = league_data[0]
        league_team_dict["league_table_home_away"] = league_data[1]
        league_team_dict["standard_stats"] = league_data[2]
        league_team_dict["standard_stats_opp"] = league_data[3]
        league_team_dict["goalkeeping"] = league_data[4]
        league_team_dict["goalkeeping_opp"] = league_data[5]
        league_team_dict["ad_goalkeeping"] = league_data[6]
        league_team_dict["ad_goalkeeping_opp"] = league_data[7]
        league_team_dict["shooting"] = league_data[8]
        league_team_dict["shooting_opp"] = league_data[9]
        league_team_dict["passing"] = league_data[10]
        league_team_dict["passing_opp"] = league_data[11]
        league_team_dict["pass_types"] = league_data[12]
        league_team_dict["pass_types_opp"] = league_data[13]
        league_team_dict["goal_shot_creation"] = league_data[14]
        league_team_dict["goal_shot_creation_opp"] = league_data[15]
        league_team_dict["defensive_action"] = league_data[16]
        league_team_dict["defensive_action_opp"] = league_data[17]
        league_team_dict["possession"] = league_data[18]
        league_team_dict["possession_opp"] = league_data[19]
        league_team_dict["playing_time"] = league_data[20]
        league_team_dict["playing_time_opp"] = league_data[21]
        league_team_dict["miscellaneous"] = league_data[22]
        league_team_dict["miscellaneous_opp"] = league_data[23]

        return league_team_dict

    def get_fbref_fixtures_and_results(
        self, season_name, league_id, league_name
    ):
        """Function used to grab fixtures and results table for a specific
        league e.g Premier league"""
        year1, year2 = season_name.split("_")

        fixtures_data = pd.read_html(
            f"https://fbref.com/en/comps/{league_id}/{year1}-{year2}/schedule/"
            + f"{year1}-{year2}-{league_name}-Scores-and-Fixtures"
        )
        fixtures_df = fixtures_data[0]

        fixtures_df["home_score"] = fixtures_df["Score"].apply(
            lambda x: x.split("–")[0] if isinstance(x, str) else np.nan
        )
        fixtures_df["away_score"] = fixtures_df["Score"].apply(
            lambda x: x.split("–")[1] if isinstance(x, str) else np.nan
        )
        return fixtures_df

    def get_league_table(
        self, season_name, league_id, league_name, league_team_dict=None
    ):
        """Function used to grab league table of the season"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        # grab league data table
        league_table = league_team_dict["league_table"]
        league_table_home_away = league_team_dict["league_table_home_away"]

        # merge table
        league_table_df = league_table.merge(
            league_table_home_away, on=["Squad", "Rk"]
        )[LEAGUE_TABLE_COLUMNS]
        return league_table_df

    def get_passing_table(
        self,
        season_name,
        league_id,
        league_name,
        league_team_dict=None,
        opponent=False,
    ):
        """Function used to grab passing data"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        if opponent:
            opponent_data = "_opp"
        else:
            opponent_data = ""

        # grab passing data table
        passing = league_team_dict[f"passing{opponent_data}"]
        pass_types = league_team_dict[f"pass_types{opponent_data}"]
        possession = league_team_dict[f"possession{opponent_data}"]

        passing_table_df = passing.merge(
            pass_types, on=["Squad", "# Pl", "90s"]
        ).merge(possession, on=["Squad", "# Pl", "90s"])
        return passing_table_df

    def get_goalkeeping_table(
        self,
        season_name,
        league_id,
        league_name,
        league_team_dict=None,
        opponent=False,
    ):
        """Function used to grab goalkeeping data"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        if opponent:
            opponent_data = "_opp"
        else:
            opponent_data = ""

        # grab passing data table
        goalkeeping = league_team_dict[f"goalkeeping{opponent_data}"]
        ad_goalkeeping = league_team_dict[f"ad_goalkeeping{opponent_data}"]

        goalkeeping_df = goalkeeping.merge(ad_goalkeeping, on=["Squad", "# Pl"])

        return goalkeeping_df

    def get_attacking_table(
        self,
        season_name,
        league_id,
        league_name,
        league_team_dict=None,
        opponent=False,
    ):
        """Function used to grab attacking data"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        if opponent:
            opponent_data = "_opp"
        else:
            opponent_data = ""

        standard_stats = league_team_dict[f"standard_stats{opponent_data}"]
        shooting = league_team_dict[f"shooting{opponent_data}"]
        goal_shot_creation = league_team_dict[
            f"goal_shot_creation{opponent_data}"
        ]

        attacking_table = standard_stats.merge(
            shooting, on=["Squad", "# Pl"]
        ).merge(goal_shot_creation, on=["Squad", "# Pl", "90s"])

        return attacking_table

    def get_defensive_table(
        self,
        season_name,
        league_id,
        league_name,
        league_team_dict=None,
        opponent=False,
    ):
        """Function used to grab defensive data"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        if opponent:
            opponent_data = "_opp"
        else:
            opponent_data = ""

        # grab passing data table
        defensive_action = league_team_dict[f"defensive_action{opponent_data}"]
        miscellaneous = league_team_dict[f"miscellaneous{opponent_data}"]

        defensive_table = defensive_action.merge(
            miscellaneous, on=["Squad", "# Pl", "90s"]
        )

        return defensive_table

    def get_playing_time_table(
        self,
        season_name,
        league_id,
        league_name,
        league_team_dict=None,
        opponent=False,
    ):
        """Function used to grabbing playing time table"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        if opponent:
            opponent_data = "_opp"
        else:
            opponent_data = ""

        playing_time_df = league_team_dict[f"playing_time{opponent_data}"]

        return playing_time_df

    def get_team_data_dict(
        self, season_name, league_id, league_name, league_team_dict=None
    ):
        """Function used to grab all team data for an example season"""
        # fetch league data
        if league_team_dict is None:
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
        else:
            pass

        team_data_dict = {}
        opponent_data_dict = {}

        # league table
        league_table_df = self.get_league_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
        )

        # team data
        team_data_dict["attacking"] = self.get_attacking_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=False,
        )
        team_data_dict["defense"] = self.get_defensive_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=False,
        )
        team_data_dict["passing"] = self.get_passing_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=False,
        )
        team_data_dict["goalkeeping"] = self.get_goalkeeping_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=False,
        )
        team_data_dict["playing_time"] = self.get_playing_time_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=False,
        )

        # opponent data
        opponent_data_dict["attacking"] = self.get_attacking_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=True,
        )
        opponent_data_dict["defense"] = self.get_defensive_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=True,
        )
        opponent_data_dict["passing"] = self.get_passing_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=True,
        )
        opponent_data_dict["goalkeeping"] = self.get_goalkeeping_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=True,
        )
        opponent_data_dict["playing_time"] = self.get_playing_time_table(
            season_name,
            league_id,
            league_name,
            league_team_dict=league_team_dict,
            opponent=True,
        )

        season_dict = {
            "league_table": league_table_df,
            "team_data": team_data_dict,
            "opponent_data": opponent_data_dict,
        }

        return season_dict

    def get_seasons_dict(self, season_name_list, league_id, league_name):
        """Function used to get multiple seasons worth of data for a chosen competition"""
        seasons_dict = {}
        for season_name in season_name_list:
            season_dict = {}
            # team season statistics
            league_team_dict = self.get_fbref_league_team_data(
                season_name, league_id, league_name
            )
            team_data_dict = self.get_team_data_dict(
                season_name, league_id, league_name, league_team_dict
            )
            # fixtures and results
            fixtures_df = self.get_fbref_fixtures_and_results(
                season_name, league_id, league_name
            )

            # add season name
            fixtures_df["season_name"] = season_name

            season_dict["data"] = team_data_dict
            season_dict["fixtures"] = fixtures_df

            seasons_dict[season_name] = season_dict
        return seasons_dict
