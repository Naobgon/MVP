import re
import math
import pandas as pd

COLUMN_PATTERN = re.compile(r"\[([^\[\]]+)\]")


def apply_formula(df: pd.DataFrame, formula: str) -> pd.Series:
    def replace_column(match):
        col_name = match.group(1).strip()
        if col_name not in df.columns:
            raise ValueError(f"Column not found in formula: {col_name}")
        return f"df[{col_name!r}]"

    expression = COLUMN_PATTERN.sub(replace_column, formula)

    allowed_globals = {
        "__builtins__": {},
        "df": df,
        "pd": pd,
        "math": math,
        "round": round,
        "abs": abs,
        "min": min,
        "max": max,
    }

    try:
        result = eval(expression, allowed_globals, {})
        if not isinstance(result, pd.Series):
            if isinstance(result, (int, float)):
                return pd.Series([result] * len(df), index=df.index)
            raise ValueError("Formula must produce a column of values.")
        return result
    except ZeroDivisionError:
        raise ValueError("Division by zero in formula.")
    except Exception as e:
        raise ValueError(f"Invalid formula: {e}")