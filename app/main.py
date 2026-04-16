from pathlib import Path
import pandas as pd

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.csv_loader import list_csv_files, load_csv
from app.formula_engine import apply_formula
from app.storage import (
    init_db,
    get_all_views,
    get_view_by_id,
    get_view_by_slug,
    create_view,
    get_view_columns,
    add_view_column,
    update_view_column,
    delete_view_column,
    get_computed_columns,
    add_computed_column,
    delete_computed_column,
)

BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(title="CSV Showcase App")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
def startup():
    init_db()


def dataframe_to_display(df: pd.DataFrame):
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].round(2)
    return df.fillna("")


def build_view_dataframe(view_obj: dict) -> pd.DataFrame:
    df = load_csv(view_obj["file_name"]).copy()

    raw_columns = get_view_columns(view_obj["id"])
    computed_columns = get_computed_columns(view_obj["id"])

    for item in computed_columns:
        try:
            df[item["column_name"]] = apply_formula(df, item["formula"])
        except Exception as e:
            df[item["column_name"]] = f"ERROR: {e}"

    selected_data = {}
    visible_items = []

    for item in raw_columns:
        if item["is_visible"] and item["source_column_name"] in df.columns:
            visible_items.append({
                "sort_order": item["sort_order"],
                "display_name": item["display_name"],
                "source_name": item["source_column_name"],
            })

    for item in computed_columns:
        if item["is_visible"] and item["column_name"] in df.columns:
            visible_items.append({
                "sort_order": item["sort_order"],
                "display_name": item["column_name"],
                "source_name": item["column_name"],
            })

    visible_items.sort(key=lambda x: (x["sort_order"], x["display_name"]))

    for item in visible_items:
        selected_data[item["display_name"]] = df[item["source_name"]]

    result_df = pd.DataFrame(selected_data)
    return dataframe_to_display(result_df)


@app.get("/", response_class=HTMLResponse)
async def admin_home(request: Request):
    views = get_all_views()
    files = list_csv_files()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "views": views,
            "files": files,
        }
    )


@app.post("/views")
async def create_view_handler(
    name: str = Form(...),
    slug: str = Form(...),
    file_name: str = Form(...),
):
    name = name.strip()
    slug = slug.strip()
    file_name = file_name.strip()

    if not name or not slug or not file_name:
        raise HTTPException(status_code=400, detail="All fields are required")

    try:
        view_id = create_view(name, slug, file_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to create view: {e}")

    df = load_csv(file_name)
    for idx, col in enumerate(df.columns.tolist(), start=1):
        add_view_column(
            view_id=view_id,
            source_column_name=col,
            display_name=col,
            is_visible=1,
            sort_order=idx * 10,
        )

    view_obj = get_view_by_id(view_id)
    return RedirectResponse(url=f"/views/{view_obj['id']}", status_code=303)


@app.get("/views/{view_id}", response_class=HTMLResponse)
async def edit_view(request: Request, view_id: int):
    view_obj = get_view_by_id(view_id)
    if not view_obj:
        raise HTTPException(status_code=404, detail="View not found")

    raw_columns = get_view_columns(view_id)
    computed_columns = get_computed_columns(view_id)

    preview_columns = []
    preview_rows = []

    try:
        df_preview = build_view_dataframe(view_obj)
        preview_columns = df_preview.columns.tolist()
        preview_rows = df_preview.to_dict(orient="records")
    except Exception as e:
        preview_columns = ["Ошибка"]
        preview_rows = [{"Ошибка": str(e)}]

    return templates.TemplateResponse(
        request=request,
        name="view_edit.html",
        context={
            "view_obj": view_obj,
            "raw_columns": raw_columns,
            "computed_columns": computed_columns,
            "preview_columns": preview_columns,
            "preview_rows": preview_rows,
        }
    )


@app.post("/views/{view_id}/raw-columns/{column_id}")
async def update_raw_column(
    view_id: int,
    column_id: int,
    display_name: str = Form(...),
    is_visible: str = Form("0"),
    sort_order: int = Form(...),
):
    update_view_column(
        column_id=column_id,
        display_name=display_name.strip(),
        is_visible=1 if is_visible == "1" else 0,
        sort_order=sort_order,
    )
    return RedirectResponse(url=f"/views/{view_id}", status_code=303)


@app.post("/views/{view_id}/computed-columns")
async def create_computed_column_handler(
    view_id: int,
    column_name: str = Form(...),
    formula: str = Form(...),
    is_visible: str = Form("1"),
    sort_order: int = Form(1000),
):
    view_obj = get_view_by_id(view_id)
    if not view_obj:
        raise HTTPException(status_code=404, detail="View not found")

    df = load_csv(view_obj["file_name"]).copy()
    existing_computed = get_computed_columns(view_id)

    for item in existing_computed:
        df[item["column_name"]] = apply_formula(df, item["formula"])

    _ = apply_formula(df, formula)

    add_computed_column(
        view_id=view_id,
        column_name=column_name.strip(),
        formula=formula.strip(),
        is_visible=1 if is_visible == "1" else 0,
        sort_order=sort_order,
    )
    return RedirectResponse(url=f"/views/{view_id}", status_code=303)


@app.post("/views/{view_id}/computed-columns/{column_id}/delete")
async def delete_computed_column_handler(view_id: int, column_id: int):
    delete_computed_column(column_id)
    return RedirectResponse(url=f"/views/{view_id}", status_code=303)


@app.get("/view/{slug}", response_class=HTMLResponse)
async def public_view(request: Request, slug: str):
    view_obj = get_view_by_slug(slug)
    if not view_obj:
        raise HTTPException(status_code=404, detail="View not found")

    df = build_view_dataframe(view_obj)

    return templates.TemplateResponse(
        request=request,
        name="public_view.html",
        context={
            "view_obj": view_obj,
            "columns": df.columns.tolist(),
            "rows": df.to_dict(orient="records"),
        }
    )