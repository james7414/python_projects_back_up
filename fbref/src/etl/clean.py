""" Script used to help clean data extracted from fbref. """

import pandas as pd
from src.config.fbref_config import (
    FIXTURE_TABLE_COLUMNS,
    ATTACKING_RENAME_COL_DICT,
    ATTACKING_TOTAL_COLUMNS,
    DEFENSE_RENAME_COL_DICT,
    DEFENSE_TOTAL_COLUMNS,
    PASSING_RENAME_COL_DICT,
    PASSING_TOTAL_COLUMNS,
    GOALKEEPING_RENAME_COL_DICT,
    GOALKEEPING_TOTAL_COLUMNS,
    PLAYING_TIME_RENAME_COL_DICT,
    PLAYING_TIME_TOTAL_COLUMNS,
    ATTACKING_COMPARISON_COLUMNS,
    DEFENSE_COMPARISON_COLUMNS,
    PASSING_COMPARISON_COLUMNS,
    PLAYING_COMPARISON_COLUMNS,
    GOALKEEPING_COMPARISON_COLUMNS,
)


def clean_fb_ref_column_names(fbref_df):
    """Function used to clean column names for fbref dataframes.

    Args:
        fbref_df (pandas.DataFrame): fbref dataframe

    Returns:
        fbref_df (pandas.DataFrame): Renamed fbref dataframe
    """
    # set column names
    fbref_df.columns = [
        col_name.replace(" ", "_")
        .replace("/", "_per_")
        .replace("+", "_plus_")
        .replace("-", "_minus_")
        .replace("%", "_perc")
        for col_name in list(fbref_df.columns)
    ]
    return fbref_df


def clean_fixtures_df(fixtures_df):
    """Function used to clean fixtures data extracted from FBref"""
    cleaned_fixtures_df = (
        fixtures_df
        # change column types
        .astype(
            {
                "home_score": float,
                "away_score": float,
                "Venue": "category",
                "Referee": "category",
            }
        )
        # rename
        .rename(
            columns={
                "Wk": "week",
                "Day": "dow",
                "Date": "date",
                "Time": "time",
                "Home": "home_team",
                "xG": "xG_home",
                "xG.1": "xG_away",
                "Away": "away_team",
                "Attendance": "attendance",
                "Referee": "referee",
                "Notes": "notes",
            }
        )
        # set kick off column
        .assign(
            kickoff=pd.to_datetime(
                fixtures_df["Date"] + " " + fixtures_df["Time"]
            )
        )
        # filter for columns we want
        [FIXTURE_TABLE_COLUMNS]
    )
    return cleaned_fixtures_df


def clean_league_table_df(league_table_df):
    """Function used to clean league table data extracted from FBref"""
    cleaned_league_table_df = (
        league_table_df
        # change column types
        .astype(
            {
                "Squad": "category",
            }
        )
        # rename
        .rename(
            columns={
                "Rank": "Position",
                "Home_Pts/MP": "Home_Pts_Per_MP",
                "Away_Pts/MP": "Away_Pts_Per_MP",
            }
        )
        # Create new columns
        .assign(
            win_perc=lambda dfr: round(dfr.W / dfr.MP, 3),
            draw_perc=lambda dfr: round(dfr.D / dfr.MP, 3),
            loss_perc=lambda dfr: round(dfr.L / dfr.MP, 3),
            home_win_perc=lambda dfr: round(dfr.Home_W / dfr.Home_MP, 3),
            home_draw_perc=lambda dfr: round(dfr.Home_D / dfr.Home_MP, 3),
            home_loss_perc=lambda dfr: round(dfr.Home_L / dfr.Home_MP, 3),
            away_win_perc=lambda dfr: round(dfr.Away_W / dfr.Away_MP, 3),
            away_draw_perc=lambda dfr: round(dfr.Away_D / dfr.Away_MP, 3),
            away_loss_perc=lambda dfr: round(dfr.Away_L / dfr.Away_MP, 3),
            goals_per_game=lambda dfr: round(dfr.GF / dfr.MP, 3),
            goals_against_per_game=lambda dfr: round(dfr.GA / dfr.MP, 3),
            home_goals_per_game=lambda dfr: round(dfr.Home_GF / dfr.Home_MP, 3),
            home_goals_against_per_game=lambda dfr: round(
                dfr.Home_GA / dfr.Home_MP, 3
            ),
            away_goals_per_game=lambda dfr: round(dfr.Away_GF / dfr.Away_MP, 3),
            away_goals_against_per_game=lambda dfr: round(
                dfr.Away_GA / dfr.Away_MP, 3
            ),
        )
    )
    return cleaned_league_table_df


def clean_attacking_table_df(attacking_df):
    """Function used to clean attacking table data extracted from FBref

    Args:
        attacking_df (pandas.DataFrame): tabular data related to attacking data

    Returns:
        cleaned_attacking_df (pandas.DataFrame): cleaned attacking data
    """
    # set column names
    attacking_df = clean_fb_ref_column_names(attacking_df)

    # clean attacking df
    attacking_df = (
        attacking_df
        # change column types
        .astype(
            {
                "Squad": "category",
            }
        )
        # rename
        .rename(columns=ATTACKING_RENAME_COL_DICT).assign(
            goal_to_assist_ratio=lambda x: x["total_goals"] / x["total_assists"]
        )
    )

    # calculate per match columns
    for tot_col in ATTACKING_TOTAL_COLUMNS:
        attacking_df[f"{tot_col}_per_match"] = attacking_df.apply(
            lambda x: round(float(x[f"{tot_col}"]) / float(x["MP"]), 3), axis=1
        )

    # drop columns

    cleaned_attacking_df = attacking_df.drop(
        [
            "Playing_Time_Starts",
            "Playing_Time_Min",
            "Playing_Time_90s",
            "Performance_CrdY",
            "Performance_CrdR",
            "Per_90_Minutes_G_plus_A",
            "Per_90_Minutes_G_plus_A_minus_PK",
            "Expected_npxG_plus_xAG",
            "Per_90_Minutes_xG_plus_xAG",
            "Per_90_Minutes_npxG_plus_xAG",
            "90s",
            "Standard_Gls",
            "Expected_xG_y",
            "Expected_npxG_y",
        ],
        axis=1,
    )
    return cleaned_attacking_df


def clean_defense_table_df(defense_df):
    """Function used to clean defense table data extracted from FBref
    Args:
        defense_df (pandas.DataFrame): tabular data related to defensive data

    Returns:
        cleaned_defense_df (pandas.DataFrame): cleaned defense data
    """
    # set column names
    defense_df = clean_fb_ref_column_names(defense_df)

    # rename column names
    defense_df = defense_df.rename(columns=DEFENSE_RENAME_COL_DICT)

    # calculate per match columns
    for tot_col in DEFENSE_TOTAL_COLUMNS:
        defense_df[f"{tot_col}_per_match"] = defense_df.apply(
            lambda x: round(float(x[f"{tot_col}"]) / float(x["MP"]), 3), axis=1
        )

    # drop columns

    cleaned_defense_df = defense_df.drop(
        ["Performance_Int", "Performance_TklW"], axis=1
    )

    return cleaned_defense_df


def clean_passing_table_df(passing_df):
    """Function used to clean passing table data extracted from FBref
    Args:
        defense_df (pandas.DataFrame): tabular data related to passing data

    Returns:
        cleaned_defense_df (pandas.DataFrame): cleaned passing data
    """
    # set column names
    passing_df = clean_fb_ref_column_names(passing_df)

    passing_df = (
        passing_df
        # change type of
        .astype(
            {
                "Squad": "category",
            }
        )
        # rename columns
        .rename(columns=PASSING_RENAME_COL_DICT)
    )

    # calculate per match columns
    for tot_col in PASSING_TOTAL_COLUMNS:
        passing_df[f"{tot_col}_per_match"] = passing_df.apply(
            lambda x: round(float(x[f"{tot_col}"]) / float(x["MP"]), 3), axis=1
        )

    # drop columns

    cleaned_passing_df = passing_df.drop(
        ["Ast", "xAG", "xA", "A_minus_xAG", "Att", "Outcomes_Cmp"], axis=1
    )

    return cleaned_passing_df


def clean_goalkeeping_table_df(goalkeeping_df):
    """Function used to clean goalkeeping table data extracted from FBref
    Args:
        defense_df (pandas.DataFrame): tabular data related to goalkeeping data

    Returns:
        cleaned_defense_df (pandas.DataFrame): cleaned goalkeeping data
    """
    # set column names
    goalkeeping_df = clean_fb_ref_column_names(goalkeeping_df)

    goalkeeping_df = (
        goalkeeping_df
        # change type of
        .astype(
            {
                "Squad": "category",
            }
        )
        # rename columns
        .rename(columns=GOALKEEPING_RENAME_COL_DICT)
    )

    # calculate per match columns
    for tot_col in GOALKEEPING_TOTAL_COLUMNS:
        goalkeeping_df[f"{tot_col}_per_match"] = goalkeeping_df.apply(
            lambda x: round(float(x[f"{tot_col}"]) / float(x["MP"]), 3), axis=1
        )

    # drop columns

    cleaned_goalkeeping_df = goalkeeping_df.drop(
        [
            "Playing_Time_Starts",
            "Playing_Time_Min",
            "Playing_Time_90s",
            "Performance_W",
            "Performance_D",
            "Performance_L",
            "90s",
            "Goals_GA",
        ],
        axis=1,
    )

    return cleaned_goalkeeping_df


def clean_playing_time_table_df(playing_time_df):
    """Function used to clean playing_time table data extracted from FBref
    Args:
        playing_time_df (pandas.DataFrame): tabular data related to playing_time data

    Returns:
        cleaned_playing_time_df (pandas.DataFrame): cleaned playing_time data
    """
    # set column names
    playing_time_df = clean_fb_ref_column_names(playing_time_df)

    playing_time_df = (
        playing_time_df
        # change type of
        .astype(
            {
                "Squad": "category",
            }
        )
        # rename columns
        .rename(columns=PLAYING_TIME_RENAME_COL_DICT)
    )

    # calculate per match columns
    for tot_col in PLAYING_TIME_TOTAL_COLUMNS:
        playing_time_df[f"{tot_col}_per_match"] = playing_time_df.apply(
            lambda x: round(float(x[f"{tot_col}"]) / float(x["MP"]), 3), axis=1
        )

    # filter for certain columns

    cleaned_playing_time_df = playing_time_df[
        [
            "Squad",
            "no_of_players_used",
            "average_age",
            "MP",
            "no_of_subs_used",
            "minutes_per_sub",
            "no_of_subs_unused",
            "goals_scored_minus_against_per_90",
            "total_expect_goals",
            "total_expected_goals_against",
            "xg_minus_xga",
            "xg_minus_xga_per_90",
            "no_of_subs_used_per_match",
            "no_of_subs_unused_per_match",
            "total_expect_goals_per_match",
            "total_expected_goals_against_per_match",
        ]
    ]

    return cleaned_playing_time_df
