import yfinance as yf
import pymysql
import pandas as pd
from datetime import datetime


def fetch_data(symbol):
    """
    从 yfinance 获取自上市以来的所有历史数据。
    """
    df = yf.download(symbol, period="max")
    # df = yf.download(symbol, start='2020-01-01')

    if df.empty:
        raise ValueError(f"没有获取到 {symbol} 的历史数据。")

    df.reset_index(inplace=True)
    df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)

    df['dates'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').astype(int)

    if 'adj_close' not in df.columns:
        if 'close' in df.columns:
            print("警告：'adj_close' 列不存在，将使用 'close' 列代替。")
            df['adj_close'] = df['close']
        else:
            raise ValueError("DataFrame 中既没有 'adj_close' 也没有 'close' 列。")

    df.dropna(inplace=True)

    return df

def insert_data(df, symbol='QQQ', source='yfinance'):
    """
    将 DataFrame 中的数据批量插入到 MySQL 数据库。
    """
    if df.empty:
        print("没有数据需要插入。")
        return

    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='my-secret-pw',
        database='qqq',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    # 修复：在 VALUES 中增加一个 %s 占位符以匹配 10 个参数
    sql = """
    INSERT INTO stock_daily_finance
    (symbol, dates, open, high, low, close, adj_close, volume, source, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        high = VALUES(high),
        low = VALUES(low),
        `close` = VALUES(`close`),
        adj_close = VALUES(adj_close),
        volume = VALUES(volume),
        created = VALUES(created)
    """

    current_time = datetime.now()
    # 使用列表推导式更高效地构建数据列表
    data_to_insert = [
        (
            symbol,
            row['dates'].item(),
            row['open'].item(),
            row['high'].item(),
            row['low'].item(),
            row['close'].item(),
            row['adj_close'].item(),
            row['volume'].item(),
            source,
            current_time
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        print(f"✅ 成功插入/更新 {len(data_to_insert)} 条数据。")
    except Exception as e:
        print(f"❌ 批量插入失败，错误：{e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    try:
        df = fetch_data("QQQ")

        # 打印 DataFrame 的头尾，检查下载的数据是否完整
        print("---")
        print("下载的 DataFrame 头（前5行）:")
        print(df.head())
        print("\n下载的 DataFrame 尾（后5行）:")
        print(df.tail())
        print("---")

        insert_data(df, symbol="QQQ")
    except Exception as e:
        print(f"❌ 程序运行失败：{e}")