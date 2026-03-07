# TDIV Monitor

Webowa aplikacja do monitorowania składników funduszu ETF **VanEck Morningstar Developed Markets Dividend Leaders (TDIV)**.

## Funkcje

- **Aktualne ceny** — pobierane na żywo z Yahoo Finance (yfinance)
- **Trend dzienny** — czy kurs zamknięcia ostatniej sesji był wzrostowy czy spadkowy
- **Trend 5-dniowy** — czy 5-dniowa średnia krocząca (MA5) rośnie czy spada
- **Live holdings** — lista składników pobierana automatycznie z VanEck przy każdym odświeżeniu
- **Fallback** — jeśli VanEck jest niedostępny, aplikacja korzysta z lokalnego pliku `.xlsx`
- **Cache** — dane rynkowe cachowane przez 15 minut
- **Wykresy cenowe** — kliknięcie wiersza otwiera interaktywny wykres z liniami ceny, MA5 i MA20
- **Wybór okresu** — 1M / 3M / 6M na wykresie
- **Filtrowanie** — po trendzie dziennym i MA5
- **Wyszukiwarka** — po nazwie spółki lub tickerze
- **Sortowanie** — po każdej kolumnie tabeli

## Wymagania

- Python 3.9+
- Biblioteki: `flask`, `yfinance`, `openpyxl`, `requests`, `pandas`

## Instalacja

```bash
git clone https://github.com/bormarek/tdiv-monitor.git
cd tdiv-monitor
pip install flask yfinance openpyxl requests pandas
```

## Uruchomienie

```bash
python3 app.py
```

Aplikacja dostępna pod adresem: [http://127.0.0.1:5001](http://127.0.0.1:5001)

## Struktura projektu

```
tdiv-monitor/
├── app.py                  # Backend Flask
├── templates/
│   └── index.html          # Interfejs webowy
└── README.md
```

## Źródła danych

| Dane | Źródło |
|---|---|
| Skład funduszu (holdings) | [VanEck – TDIV Holdings](https://www.vaneck.com/pl/pl/investments/dividend-etf/downloads/holdings/) |
| Ceny akcji | Yahoo Finance via [yfinance](https://github.com/ranaroussi/yfinance) |

## Licencja

MIT
