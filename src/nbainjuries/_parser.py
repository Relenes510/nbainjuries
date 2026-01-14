from os import PathLike
import pandas as pd
import tabula
import PyPDF2
from io import BytesIO
import requests
from ._exceptions import URLRetrievalError, LocalRetrievalError
from ._util import __concat_injreppgs, _validate_headers, _pagect_localpdf, __clean_injrep

import tempfile
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def validate_injrepurl(filepath: str | PathLike, **kwargs) -> requests.Response:
    """
    Validate and retrieve injury report PDF from nba.com
    """
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session.mount("https://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf",
    }

    headers.update(kwargs.get("headers", {}))

    try:
        resp = session.get(filepath, headers=headers, stream=True, timeout=(5, 60))
        resp.raise_for_status()
        print(f"Validated {Path(filepath).stem}.")
        return resp
    except requests.exceptions.RequestException as e_gen:
        print(f"Failed validation - {Path(filepath).stem}.")
        raise URLRetrievalError(filepath, e_gen)



def extract_injrepurl(filepath: str | PathLike, area_headpg: list, cols_headpg: list,
                      area_otherpgs: list | None = None, cols_otherpgs: list | None = None,
                      **kwargs) -> pd.DataFrame:
    """
    Extract injury report from URL (nba.com) using local PDF buffering

    NOTICE:
    Tabula must NEVER receive a URL directly.
    NBA CDN intermittently stalls connections and Java has no timeout.
    Always download PDFs locally before parsing.
    """

    resp = validate_injrepurl(filepath, **kwargs)

    # -----------------------------
    # Save PDF locally (CRITICAL)
    # -----------------------------
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)

    try:
        pdf_reader = PyPDF2.PdfReader(str(tmp_path))
        pdf_numpgs = len(pdf_reader.pages)

        if area_otherpgs is None:
            area_otherpgs = area_headpg
        if cols_otherpgs is None:
            cols_otherpgs = cols_headpg

        # First page
        dfs_headpg = tabula.read_pdf(
            str(tmp_path),
            stream=True,
            area=area_headpg,
            columns=cols_headpg,
            pages=1
        )
        _validate_headers(dfs_headpg[0])

        # Following pages
        dfs_otherpgs = []
        if pdf_numpgs >= 2:
            dfs_otherpgs = tabula.read_pdf(
                str(tmp_path),
                stream=True,
                area=area_otherpgs,
                columns=cols_otherpgs,
                pages=f"2-{pdf_numpgs}",
                pandas_options={"header": None}
            )

        df_rawdata = __concat_injreppgs(
            dflist_headpg=dfs_headpg,
            dflist_otherpgs=dfs_otherpgs
        )
        df_cleandata = __clean_injrep(df_rawdata)
        return df_cleandata

    finally:
        # Always clean up temp file
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass



def extract_injreplocal(filepath: str | PathLike, area_headpg: list, cols_headpg: list,
                        area_otherpgs: list | None = None, cols_otherpgs: list | None = None) -> pd.DataFrame:
    try:
        pdf_numpgs = _pagect_localpdf(filepath)
    except (FileNotFoundError, PermissionError) as e_gen:
        raise LocalRetrievalError(filepath, e_gen)
        # archive FileNotFoundError(f'Could not open {str(filepath)} due to {e_gen}.')

    if area_otherpgs is None:
        area_otherpgs = area_headpg
    if cols_otherpgs is None:
        cols_otherpgs = cols_headpg

    # First page
    dfs_headpg = tabula.read_pdf(filepath, stream=True, area=area_headpg,
                                 columns=cols_headpg, pages=1)
    _validate_headers(dfs_headpg[0])
    # Following pgs
    dfs_otherpgs = []  # default to empty list if only single pg
    if pdf_numpgs >= 2:
        dfs_otherpgs = tabula.read_pdf(filepath, stream=True, area=area_otherpgs,
                                       columns=cols_otherpgs, pages='2-' + str(pdf_numpgs),
                                       pandas_options={'header': None})
        # default setting - pandas_options={'header': 'infer'} has been overridden with pandas_options={'header': None}
        # Check first row contents; no headers present --> good, headers present --> drop and set headers manually
    # Process and clean data
    df_rawdata = __concat_injreppgs(dflist_headpg=dfs_headpg, dflist_otherpgs=dfs_otherpgs)
    df_cleandata = __clean_injrep(df_rawdata)
    return df_cleandata

