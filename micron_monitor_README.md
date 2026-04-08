# 美光近三個月新聞/社群爬蟲

這個腳本會抓：
- 新聞：Google News RSS
- 社群：Reddit + PTT(Stock) + Dcard(stock)

關鍵字預設為：`美光 OR Micron`

## 1) 安裝

```bash
pip install -r requirements.txt
```

## 2) 執行

```bash
python micron_monitor.py
```

## 3) 輸出

執行後會在 `data/` 產生三個檔案：
- `micron_news_時間戳.csv`
- `micron_social_時間戳.csv`
- `micron_combined_時間戳.json`

## 4) 可調整參數

在 `micron_monitor.py` 內可改：
- `DAYS_BACK`：預設 90 天
- `NEWS_LIMIT`：新聞筆數上限
- `SOCIAL_LIMIT`：社群筆數上限
- `PTT_PAGES`：PTT 回溯頁數
- `DCARD_PAGES`：Dcard 回溯頁數
- `NEWS_QUERY` / `SOCIAL_QUERY`：搜尋字串

## 5) 注意事項

- 網站可能變更結構或限制抓取頻率，若失敗可重試或降低頻率。
- 使用爬蟲請遵守目標網站服務條款與法規。
