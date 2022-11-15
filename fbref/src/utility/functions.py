"""Script used to help with general functionality"""


def flatten_cols(df):
    """Function used to flatten multi indexed columns in panda dfs."""
    df.columns = ["_".join(x) for x in df.columns.to_flat_index()]
    return df


def rename_unnamed_columns(input_df):
    """Function used to clean colun names that were not multi-indexed"""
    col_names = list(input_df.columns)

    new_col_list = []
    for col in col_names:
        if "Unnamed" in col:
            level_index = col.split("_").index("level")
            new_col = "_".join(col.split("_")[level_index + 2 :])
            new_col_list.append(new_col)
        else:
            new_col = col
            new_col_list.append(new_col)
    input_df.columns = new_col_list
    return input_df
