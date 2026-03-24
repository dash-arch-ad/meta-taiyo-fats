import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, TypedDict

import gspread
import requests
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")
GRAPH_API_VERSION = "v25.0"
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
APP_SECRET_ENV = "APP_SECRET_JSON"
RE_MO_FIXED_FILTER_KEYWORD = "スポンジ_Reach"

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleCredsDict(TypedDict):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class SheetsMap(TypedDict, total=False):
    re_mo: str
    re_ad: str
    re_da: str
    re_gen: str
    re_age: str
    re_pla: str


class AppSecretJson(TypedDict, total=False):
    m_token: str
    m_act_id: str | int
    s_id: str | list[str]
    sheets: SheetsMap
    g_creds: GoogleCredsDict
    re_mo_filters: list[str]


@dataclass(frozen=True)
class ReportRule:
    secret_sheet_key: str
    level: str
    fields: str
    header: list[str]
    time_increment: str
    breakdowns: str | None = None


REPORT_RULES: dict[str, ReportRule] = {
    "re_mo": ReportRule(
        secret_sheet_key="re_mo",
        level="campaign",
        fields="campaign_name,reach",
        header=["Month", "Campaign name", "Reach"],
        time_increment="monthly",
    ),
    "re_ad": ReportRule(
        secret_sheet_key="re_ad",
        level="ad",
        fields="ad_name,adset_name,campaign_name,reach,actions",
        header=["Month", "Ad Name", "Adset Name", "Campaign name", "Reach", "Location searches"],
        time_increment="monthly",
    ),
    "re_da": ReportRule(
        secret_sheet_key="re_da",
        level="campaign",
        fields="campaign_name,reach",
        header=["Daily", "Campaign name", "Reach"],
        time_increment="1",
    ),
    "re_gen": ReportRule(
        secret_sheet_key="re_gen",
        level="campaign",
        fields="campaign_name,reach",
        header=["Month", "Gender", "Campaign name", "Reach"],
        time_increment="monthly",
        breakdowns="gender",
    ),
    "re_age": ReportRule(
        secret_sheet_key="re_age",
        level="campaign",
        fields="campaign_name,reach",
        header=["Month", "Age", "Campaign name", "Reach"],
        time_increment="monthly",
        breakdowns="age",
    ),
    "re_pla": ReportRule(
        secret_sheet_key="re_pla",
        level="campaign",
        fields="campaign_name,reach",
        header=["Month", "Platform", "Campaign name", "Reach"],
        time_increment="monthly",
        breakdowns="publisher_platform",
    ),
}


def main() -> None:
    print("Starting Meta monthly export...")

    config = load_secret_config()
    meta_token = config["m_token"]
    act_id = normalize_act_id(str(config["m_act_id"]))
    spreadsheet_id = normalize_spreadsheet_id(config["s_id"])
    sheet_name_map = config["sheets"]
    google_creds = config["g_creds"]
    re_mo_filters = normalize_re_mo_filters(config.get("re_mo_filters", []))

    print(f"Target Account: ******{act_id[-4:]}")

    spreadsheet = open_spreadsheet(spreadsheet_id, google_creds)
    session = build_session()

    since_date, until_date = build_target_range()
    print(f"Fetch Range (JST): {since_date} to {until_date}")

    for rule_key, rule in REPORT_RULES.items():
        target_sheet_name = sheet_name_map.get(rule.secret_sheet_key)
        if not target_sheet_name:
            print(f"Skip: sheets.{rule.secret_sheet_key} is not configured.")
            continue

        print(f"Processing: {rule_key} -> {target_sheet_name}")

        if rule_key == "re_mo":
            fixed_filtered_rows = fetch_re_mo_filtered_totals(
                session=session,
                act_id=act_id,
                access_token=meta_token,
                since_date=since_date,
                until_date=until_date,
                keywords=[RE_MO_FIXED_FILTER_KEYWORD],
            )
            raw_rows = fetch_insights(
                session=session,
                act_id=act_id,
                access_token=meta_token,
                rule=rule,
                since_date=since_date,
                until_date=until_date,
            )
            normal_rows = transform_rows(rule_key, raw_rows)
            output_rows = insert_re_mo_fixed_rows(normal_rows, fixed_filtered_rows)
        else:
            raw_rows = fetch_insights(
                session=session,
                act_id=act_id,
                access_token=meta_token,
                rule=rule,
                since_date=since_date,
                until_date=until_date,
            )
            output_rows = transform_rows(rule_key, raw_rows)

        write_sheet(
            spreadsheet=spreadsheet,
            worksheet_name=target_sheet_name,
            header=rule.header,
            rows=output_rows,
        )
        print(f"Write success: {target_sheet_name} ({len(output_rows)} rows)")

    print("Completed.")


def load_secret_config() -> AppSecretJson:
    secret_env = os.environ.get(APP_SECRET_ENV)
    if not secret_env:
        raise RuntimeError(f"{APP_SECRET_ENV} is not set.")

    try:
        config = json.loads(secret_env)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"{APP_SECRET_ENV} is not valid JSON: {e}") from e

    required_top_keys = ["m_token", "m_act_id", "s_id", "sheets", "g_creds"]
    missing = [k for k in required_top_keys if k not in config or config[k] in (None, "", {})]
    if missing:
        raise RuntimeError(f"Missing required keys in secret: {', '.join(missing)}")

    return config


def normalize_act_id(raw_act_id: str) -> str:
    cleaned = raw_act_id.replace("act_", "").replace("act=", "").replace("act", "").strip()
    if not cleaned:
        raise RuntimeError("m_act_id is empty.")
    return f"act_{cleaned}"


def normalize_spreadsheet_id(s_id: str | list[str]) -> str:
    if isinstance(s_id, list):
        for item in s_id:
            item = str(item).strip()
            if item:
                return item
        raise RuntimeError("s_id list does not contain a valid spreadsheet ID.")
    s_id = str(s_id).strip()
    if not s_id:
        raise RuntimeError("s_id is empty.")
    return s_id


def open_spreadsheet(spreadsheet_id: str, google_creds: GoogleCredsDict) -> gspread.Spreadsheet:
    credentials = Credentials.from_service_account_info(google_creds, scopes=GOOGLE_SCOPES)
    client = gspread.authorize(credentials)
    return client.open_by_key(spreadsheet_id)


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def first_day_of_previous_month(target: date) -> date:
    return (target.replace(day=1) - timedelta(days=1)).replace(day=1)


def build_target_range() -> tuple[date, date]:
    now_jst = datetime.now(JST)
    today_jst = now_jst.date()
    yesterday_jst = today_jst - timedelta(days=1)
    run_month_start = today_jst.replace(day=1)

    if today_jst.day == 1:
        # 月初は当月データが空になるため、前々月 + 前月の2か月分に寄せる
        since_date = first_day_of_previous_month(first_day_of_previous_month(run_month_start))
    else:
        # 通常日は、先月 + 当月途中までの2か月分
        since_date = first_day_of_previous_month(run_month_start)

    return since_date, yesterday_jst


def fetch_insights(
    session: requests.Session,
    act_id: str,
    access_token: str,
    rule: ReportRule,
    since_date: date,
    until_date: date,
) -> list[dict[str, Any]]:
    url = f"{GRAPH_BASE}/{act_id}/insights"
    params: dict[str, Any] | None = {
        "access_token": access_token,
        "level": rule.level,
        "fields": rule.fields,
        "time_increment": rule.time_increment,
        "time_range": json.dumps(
            {
                "since": since_date.strftime("%Y-%m-%d"),
                "until": until_date.strftime("%Y-%m-%d"),
            },
            separators=(",", ":"),
        ),
        "limit": 500,
    }

    if rule.breakdowns:
        params["breakdowns"] = rule.breakdowns

    all_rows: list[dict[str, Any]] = []

    while True:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()

        payload = response.json()
        if "error" in payload:
            message = payload["error"].get("message", "Unknown Meta API error")
            code = payload["error"].get("code", "")
            subcode = payload["error"].get("error_subcode", "")
            raise RuntimeError(f"Meta API error: {message} (code={code}, subcode={subcode})")

        data = payload.get("data", [])
        all_rows.extend(data)

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


def fetch_re_mo_filtered_totals(
    session: requests.Session,
    act_id: str,
    access_token: str,
    since_date: date,
    until_date: date,
    keywords: list[str],
) -> list[list[Any]]:
    rows: list[list[Any]] = []

    for keyword in keywords:
        url = f"{GRAPH_BASE}/{act_id}/insights"
        params: dict[str, Any] | None = {
            "access_token": access_token,
            "level": "account",
            "fields": "reach",
            "time_increment": "monthly",
            "time_range": json.dumps(
                {
                    "since": since_date.strftime("%Y-%m-%d"),
                    "until": until_date.strftime("%Y-%m-%d"),
                },
                separators=(",", ":"),
            ),
            "filtering": json.dumps(
                [
                    {
                        "field": "campaign.name",
                        "operator": "CONTAIN",
                        "value": keyword,
                    }
                ],
                separators=(",", ":"),
                ensure_ascii=False,
            ),
            "limit": 500,
        }

        while True:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()

            payload = response.json()
            if "error" in payload:
                message = payload["error"].get("message", "Unknown Meta API error")
                code = payload["error"].get("code", "")
                subcode = payload["error"].get("error_subcode", "")
                raise RuntimeError(f"Meta API error: {message} (code={code}, subcode={subcode})")

            data = payload.get("data", [])
            for item in data:
                month = normalize_month(item.get("date_start"))
                if not month:
                    continue
                rows.append(
                    [
                        month,
                        f"{keyword}合計",
                        to_int(item.get("reach")),
                    ]
                )

            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break

            url = next_url
            params = None

    rows.sort(key=lambda x: tuple(str(v) for v in x), reverse=True)
    return rows


def insert_re_mo_fixed_rows(
    normal_rows: list[list[Any]],
    fixed_rows: list[list[Any]],
) -> list[list[Any]]:
    insert_index = min(2, len(normal_rows))
    return normal_rows[:insert_index] + fixed_rows + normal_rows[insert_index:]


def transform_rows(rule_key: str, raw_rows: list[dict[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []

    for item in raw_rows:
        if rule_key == "re_mo":
            month = normalize_month(item.get("date_start"))
            if not month:
                continue
            rows.append(
                [
                    month,
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                ]
            )
        elif rule_key == "re_ad":
            month = normalize_month(item.get("date_start"))
            if not month:
                continue
            rows.append(
                [
                    month,
                    item.get("ad_name", ""),
                    item.get("adset_name", ""),
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                    extract_location_searches(item.get("actions", [])),
                ]
            )
        elif rule_key == "re_da":
            daily = normalize_daily(item.get("date_start"))
            if not daily:
                continue
            rows.append(
                [
                    daily,
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                ]
            )
        elif rule_key == "re_gen":
            month = normalize_month(item.get("date_start"))
            if not month:
                continue
            rows.append(
                [
                    month,
                    item.get("gender", ""),
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                ]
            )
        elif rule_key == "re_age":
            month = normalize_month(item.get("date_start"))
            if not month:
                continue
            rows.append(
                [
                    month,
                    item.get("age", ""),
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                ]
            )
        elif rule_key == "re_pla":
            month = normalize_month(item.get("date_start"))
            if not month:
                continue
            rows.append(
                [
                    month,
                    normalize_platform(item.get("publisher_platform", "")),
                    item.get("campaign_name", ""),
                    to_int(item.get("reach")),
                ]
            )
        else:
            raise RuntimeError(f"Unsupported rule key: {rule_key}")

    rows.sort(key=lambda x: tuple(str(v) for v in x), reverse=True)
    return rows


def normalize_month(date_start: Any) -> str:
    if not date_start:
        return ""
    date_str = str(date_start)
    if len(date_str) >= 7:
        return date_str[:7]
    return ""


def normalize_daily(date_start: Any) -> str:
    if not date_start:
        return ""
    date_str = str(date_start)
    if len(date_str) >= 10:
        return date_str[:10]
    return ""


def normalize_platform(value: Any) -> str:
    raw = str(value or "").strip()
    platform_map = {
        "facebook": "Facebook",
        "instagram": "Instagram",
        "messenger": "Messenger",
        "audience_network": "Audience Network",
        "oculus": "Oculus",
        "threads": "Threads",
    }
    return platform_map.get(raw, raw)


def normalize_re_mo_filters(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for value in values:
        keyword = str(value).strip()
        if not keyword or keyword in seen:
            continue
        normalized.append(keyword)
        seen.add(keyword)

    return normalized


def to_int(value: Any) -> int:
    if value in (None, "", "null"):
        return 0
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return 0


def extract_location_searches(actions: Any) -> int:
    if not isinstance(actions, list):
        return 0

    action_map: dict[str, int] = {}
    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = str(action.get("action_type", "")).strip()
        if not action_type:
            continue
        action_map[action_type] = to_int(action.get("value"))

    if "find_location_total" in action_map:
        return action_map["find_location_total"]

    return action_map.get("find_location_website", 0) + action_map.get("find_location_mobile_app", 0)


def get_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str,
    rows: int,
    cols: int,
) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=max(rows, 100),
            cols=max(cols, 10),
        )


def write_sheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str,
    header: list[str],
    rows: list[list[Any]],
) -> None:
    worksheet = get_or_create_worksheet(
        spreadsheet=spreadsheet,
        worksheet_name=worksheet_name,
        rows=len(rows) + 10,
        cols=len(header) + 2,
    )
    output = [header] + rows
    worksheet.clear()
    worksheet.update("A1", output, value_input_option="USER_ENTERED")


if __name__ == "__main__":
    main()
