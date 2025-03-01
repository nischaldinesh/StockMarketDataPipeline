import json
import datetime
from zoneinfo import ZoneInfo  

def lambda_handler(event, context):
    now = datetime.datetime.now(ZoneInfo("America/New_York"))
    year = now.year
    month = now.month
    week = now.isocalendar()[1]
    
    query_string_gain = (
        "WITH opening_prices AS ("
        "  SELECT ticker, price AS opening_price, "
        "         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated ASC) AS rn "
        "  FROM stock_data "
        "  WHERE price_type = 'opening' "
        f"    AND year = {year} AND month = {month} AND week = {week} "
        "), "
        "closing_prices AS ("
        "  SELECT ticker, price AS closing_price, "
        "         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated DESC) AS rn "
        "  FROM stock_data "
        "  WHERE price_type = 'closing' "
        f"    AND year = {year} AND month = {month} AND week = {week} "
        ") "
        "SELECT o.ticker, o.opening_price, c.closing_price, "
        "       round(((c.closing_price - o.opening_price) / o.opening_price) * 100, 2) AS weekly_gain "
        "FROM opening_prices o "
        "JOIN closing_prices c ON o.ticker = c.ticker "
        "WHERE o.rn = 1 AND c.rn = 1 "
        "  AND ((c.closing_price - o.opening_price) / o.opening_price) > 0 "
        "ORDER BY weekly_gain DESC "
        "LIMIT 5;"
    )
    
    query_string_loss = (
        "WITH opening_prices AS ("
        "  SELECT ticker, price AS opening_price, "
        "         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated ASC) AS rn "
        "  FROM stock_data "
        "  WHERE price_type = 'opening' "
        f"    AND year = {year} AND month = {month} AND week = {week} "
        "), "
        "closing_prices AS ("
        "  SELECT ticker, price AS closing_price, "
        "         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated DESC) AS rn "
        "  FROM stock_data "
        "  WHERE price_type = 'closing' "
        f"    AND year = {year} AND month = {month} AND week = {week} "
        ") "
        "SELECT o.ticker, o.opening_price, c.closing_price, "
        "       round(((c.closing_price - o.opening_price) / o.opening_price) * 100, 2) AS weekly_loss "
        "FROM opening_prices o "
        "JOIN closing_prices c ON o.ticker = c.ticker "
        "WHERE o.rn = 1 AND c.rn = 1 "
        "  AND ((c.closing_price - o.opening_price) / o.opening_price) < 0 "
        "ORDER BY weekly_loss ASC "
        "LIMIT 5;"
    )
    
    return {
        "queryStringGain": query_string_gain,
        "queryStringLoss": query_string_loss,
        "year": year,
        "month": month,
        "week": week
    }
