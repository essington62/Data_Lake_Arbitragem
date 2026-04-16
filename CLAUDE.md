# btc-data-lake

Data lake multi-exchange para estratégia de funding rate arbitrage delta-neutral.

## Arquitetura

```
collectors/ → normalizers/ → storage/
     ↓              ↓            ↓
  raw JSON      validate      parquet
  per exchange  + dedup     partitioned
```

Camadas:
- **collectors/**: Um arquivo por exchange, todos herdam `BaseCollector`. IO puro — sem lógica de negócio.
- **normalizers/**: `schema.py` (tipos), `transforms.py` (validação + dedup). Sem IO.
- **storage/**: `parquet_writer.py`. Escrita particionada Hive-style.

## Como adicionar nova exchange

1. Adicionar bloco em `conf/exchanges.yml` com endpoints, rate_limit, symbol_format
2. Adicionar symbol em `conf/instruments.yml` → `exchange_symbols`
3. Criar `src/collectors/<exchange>.py` implementando `collect_funding_rates()` e `collect_current_funding()`
4. Registrar em `src/collectors/__init__.py` → `COLLECTOR_MAP`

## Como adicionar novo instrumento

Só YAML: copiar bloco em `conf/instruments.yml` e preencher `exchange_symbols` para cada exchange.

## Comandos

```bash
# Coleta atual (funding rates em tempo real)
python scripts/collect_funding.py

# Backfill 30 dias histórico
python scripts/backfill_funding.py

# Ver spreads atuais
python scripts/show_spreads.py

# Testes
pytest tests/ -v
```

## Particionamento Parquet

```
data/normalized/funding_rates/exchange=binance/date=2026-04-16/data.parquet
data/raw/binance/funding_2026-04-16.parquet
```

## Relação com projetos irmãos

- **btc_AI** (AI.hab): usa CoinGlass API para sinais de regime/stress. Repositório separado.
- **btc-data-lake** (este): coleta direta das exchanges, sem intermediários.

Assessment completo das APIs: `prompts/assessment-data-lake-6-exchanges.md` no repo btc_AI.

## Restrições

- Apenas endpoints públicos (sem API keys nesta fase)
- Se exchange cair, demais continuam (falha isolada)
- Sem Kedro, Airflow ou frameworks pesados
- Timezone UTC obrigatório em todos os timestamps
