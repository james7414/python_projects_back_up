"""Fetch script used to grab fbref data used for analaysis
"""
import pandas as pd
from src.etl.clean import (
    clean_attacking_table_df,
    clean_defense_table_df,
    clean_passing_table_df,
    clean_goalkeeping_table_df,
    clean_playing_time_table_df,
)
from src.config.fbref_config import (
    ATTACKING_COMPARISON_COLUMNS,
    DEFENSE_COMPARISON_COLUMNS,
    PASSING_COMPARISON_COLUMNS,
    PLAYING_COMPARISON_COLUMNS,
    GOALKEEPING_COMPARISON_COLUMNS,
)


def get_category_data_across_seasons(
    seasons_dict, data_category, opponent_data=False
):
    """Function used to create a dataframe of seasons worth of data with data side by side

    Args:
        seasons_dict (dict): Dictionary of data from fbref for multiple seasons
        data_category (str): Type of data from fbref.
        opponent_data (bool, optional): Whether we want extract. Defaults to False.

    Raises:
        Exception: Given if data_category is not one of 'attacking', 'defense', 'passing',
        'goalkeeping', 'playing_time'

    Returns:
        seasons_df (pandas.DataFrame) : df for a certain data category in the format to compare
                                        data across seasons.
    """
    if opponent_data:
        data_type = "opponent_data"
    else:
        data_type = "team_data"

    # clean each seasons data
    seasons_category_data_dict = {}
    for season_name in seasons_dict.keys():
        category_data_dict = {}
        category_df = seasons_dict[season_name]["data"][data_type][
            data_category
        ]
        if data_category == "attacking":
            cleaned_category_df = clean_attacking_table_df(category_df)
        elif data_category == "defense":
            cleaned_category_df = clean_defense_table_df(category_df)
        elif data_category == "passing":
            cleaned_category_df = clean_passing_table_df(category_df)
        elif data_category == "goalkeeping":
            cleaned_category_df = clean_goalkeeping_table_df(category_df)
        elif data_category == "playing_time":
            cleaned_category_df = clean_playing_time_table_df(category_df)
        else:
            raise Exception("Invalid data category.")

        cleaned_category_df = cleaned_category_df.assign(
            season_name=season_name
        )
        category_data_dict[data_type] = cleaned_category_df
        seasons_category_data_dict[season_name] = category_data_dict

    stats_data_type_df_list = [
        season_data_dict[data_type]
        for season_data_dict in seasons_category_data_dict.values()
    ]

    seasons_df = pd.concat(stats_data_type_df_list)
    # transpose data
    seasons_df = seasons_df.set_index(["Squad", "season_name"])
    # sort columns based by team
    seasons_df = seasons_df.sort_index(axis=0)

    return seasons_df


def get_seasons_comparison_dict(
    seasons_dict,
    data_category_list=[
        "attacking",
        "defense",
        "passing",
        "goalkeeping",
        "playing_time",
    ],
):
    """Function used to create dictionary for season comparisons"""
    season_comparison_dict = {}
    team_data_dict = {}
    oppoenet_data_dict = {}

    for data_category in data_category_list:
        team_comparison_df = get_category_data_across_seasons(
            seasons_dict=seasons_dict,
            data_category=data_category,
            opponent_data=False,
        )
        team_opponent_comparison_df = get_category_data_across_seasons(
            seasons_dict=seasons_dict,
            data_category=data_category,
            opponent_data=True,
        )
        team_data_dict[data_category] = team_comparison_df
        oppoenet_data_dict[data_category] = team_opponent_comparison_df

    season_comparison_dict["team_data"] = team_data_dict
    season_comparison_dict["opponent_data"] = oppoenet_data_dict
    return season_comparison_dict


def get_data_category_season_comparison_df(
    season_comparison_dict,
    data_category,
    opponent_data=False,
    filter_columns=True,
):
    """Function used to grab season comparison df for specified data category.

    Args:
        season_comparison_dict (dict): Dictionary of season data for data categories from fbref.
        data_category (str): Specified data category ie 'attacking', 'defense', 'passing',
                                'goalkeeping', 'playing_time
        opponent_data (bool, optional): Whether we are grabbing team data or opponent data. Defaults to False.
        filter_columns (bool): Whether to filter season_comparison_df with pre-determined columns.

    Raises:
        Exception: Given if data_category is not one of 'attacking', 'defense', 'passing',
        'goalkeeping', 'playing_time'

    Returns:
        season_comparison_df (pandas.DataFrame): Dataframe of seasons data for specific data category.
    """
    if opponent_data:
        data_type = "opponent_data"
    else:
        data_type = "team_data"

    season_comparison_df = season_comparison_dict[data_type][data_category]

    if filter_columns:
        if data_category == "attacking":
            season_comparison_df = season_comparison_df[
                ATTACKING_COMPARISON_COLUMNS
            ]
        elif data_category == "defense":
            season_comparison_df = season_comparison_df[
                DEFENSE_COMPARISON_COLUMNS
            ]
        elif data_category == "passing":
            season_comparison_df = season_comparison_df[
                PASSING_COMPARISON_COLUMNS
            ]
        elif data_category == "goalkeeping":
            season_comparison_df = season_comparison_df[
                GOALKEEPING_COMPARISON_COLUMNS
            ]
        elif data_category == "playing_time":
            season_comparison_df = season_comparison_df[
                PLAYING_COMPARISON_COLUMNS
            ]
        else:
            raise Exception("Invalid data category.")
    return season_comparison_df


def get_ranking_df(data_category_df):
    """Function used to get the ranking of each stat for each team with respective to other teams of the league"""
    # filter out columns we don't want to rank
    stats_columns = [
        column
        for column in data_category_df.columns
        if column not in ["Squad", "season_name", "MP"]
    ]

    # create ranking df
    ranking_df = pd.concat(
        [
            data_category_df[column].rank(ascending=False)
            for column in stats_columns
        ],
        axis=1,
    )
    # rename columns
    ranking_df.columns = [column + "_rank" for column in ranking_df]

    return ranking_df
