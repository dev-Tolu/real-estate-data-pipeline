import os
import json
import logging
import pandas as pd
import numpy as np
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoETS
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import redis
from typing import Dict, Any, Optional

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealEstateForecaster:
    def __init__(self):
        postgres_dsn = os.getenv('POSTGRES_DSN')

        engine_url = postgres_dsn
        if engine_url.startswith('postgresql://'):
            engine_url = engine_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
        self.engine = create_engine(engine_url)

        self.pg_conn = psycopg2.connect(dsn=postgres_dsn)

        self.redis_client = redis.Redis(
            host='redis',
            port=6379,
            password=os.getenv('REDIS_PASSWORD'),
            db=1,
            decode_responses=True
        )

        self.forecast_horizon = int(os.getenv('FORECAST_HORIZON', 90))
        self.confidence_interval = float(os.getenv('FORECAST_CONFIDENCE_INTERVAL', 0.95))

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_historical_data(self, zip_code: Optional[str] = None) -> pd.DataFrame:
        logger.info(f"Fetching historical data for zip_code: {zip_code or 'all'}")

        if zip_code:
            query = text("""
                SELECT
                    ph.listing_date  AS ds,
                    AVG(ph.price)    AS y
                FROM price_history ph
                JOIN properties p ON ph.property_id = p.property_id
                WHERE ph.price IS NOT NULL
                  AND p.zip_code = :zip_code
                GROUP BY ph.listing_date
                ORDER BY ph.listing_date
            """)
            params = {'zip_code': zip_code}
        else:
            query = text("""
                SELECT
                    ph.listing_date  AS ds,
                    AVG(ph.price)    AS y
                FROM price_history ph
                JOIN properties p ON ph.property_id = p.property_id
                WHERE ph.price IS NOT NULL
                GROUP BY ph.listing_date
                ORDER BY ph.listing_date
            """)
            params = {}

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            logger.warning(f"No data found for zip_code: {zip_code}")
            return pd.DataFrame()

        logger.info(f"Fetched {len(df)} historical records")
        return df

    # ------------------------------------------------------------------
    # Preparation
    # ------------------------------------------------------------------

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Preparing features...")
        df = df.copy()
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.sort_values('ds').reset_index(drop=True)
        df = df.dropna(subset=['ds', 'y'])
        # statsforecast requires a unique_id column
        df['unique_id'] = 'all'
        logger.info(f"Prepared {len(df)} records with features")
        return df

    # ------------------------------------------------------------------
    # Training + forecasting
    # ------------------------------------------------------------------

    def train_and_forecast(self, df: pd.DataFrame, zip_code: str) -> Optional[pd.DataFrame]:
        logger.info(f"Training model for zip_code: {zip_code}")

        if len(df) < 2: # minimum number of days for forecast: change from 2 to 10 for production
            logger.warning(
                f"Only {len(df)} data points for zip_code={zip_code} — "
                f"need at least 10 to train. Skipping."
            )
            return None

        freq = self._infer_frequency(df)
        logger.info(f"Inferred frequency: {freq}")

        level = [int(self.confidence_interval * 100)]  # e.g. [95]

        models = [
            AutoARIMA(season_length=1),
            # AutoETS(season_length=7),
        ]

        sf = StatsForecast(models=models, freq=freq, n_jobs=1)

        try:
            sf.fit(df[['unique_id', 'ds', 'y']])
            forecast = sf.predict(h=self.forecast_horizon, level=level)
        except Exception as e:
            logger.error(f"Model fitting failed: {e}", exc_info=True)
            return None

        # Blend the two models; fall back to yhat if CI columns are absent
        # lo_arima = f'AutoARIMA-lo-{level[0]}'
        # hi_arima = f'AutoARIMA-hi-{level[0]}'
        # lo_ets   = f'AutoETS-lo-{level[0]}'
        # hi_ets   = f'AutoETS-hi-{level[0]}'

        # forecast['yhat']       = (forecast['AutoARIMA'] + forecast['AutoETS']) / 2
        # forecast['yhat_lower'] = (forecast.get(lo_arima, forecast['yhat']) +
        #                           forecast.get(lo_ets,   forecast['yhat'])) / 2
        # forecast['yhat_upper'] = (forecast.get(hi_arima, forecast['yhat']) +
        #                           forecast.get(hi_ets,   forecast['yhat'])) / 2

        # For simplicity and to test prediction, currently using AutoARIMA's point forecasts and intervals only. 
        # Can expand to blending when we have more confidence in the models and more data.
        # TODO: Implement model blending     
        forecast['yhat']       = forecast['AutoARIMA']
        lo_col = f'AutoARIMA-lo-{level[0]}'
        hi_col = f'AutoARIMA-hi-{level[0]}'
        forecast['yhat_lower'] = forecast.get(lo_col, forecast['yhat'])
        forecast['yhat_upper'] = forecast.get(hi_col, forecast['yhat'])

        result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        result = result.reset_index(drop=True)

        logger.info(f"Generated {len(result)} forecast points")
        return result

    @staticmethod
    def _infer_frequency(df: pd.DataFrame) -> str:
        if len(df) < 2: # minimum number of days for forecast: change from 2 to 10 for production
            return 'D'
        deltas = df['ds'].diff().dropna().dt.days
        median_gap = deltas.median()
        if median_gap <= 1:
            return 'D'
        if median_gap <= 7:
            return 'W'
        return 'MS'

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_forecast_to_postgres(self, forecast_df: pd.DataFrame, zip_code: str):
        logger.info(f"Saving forecast for zip_code: {zip_code}")

        property_id = f"forecast_zip_{zip_code}"
        cursor = self.pg_conn.cursor()

        cursor.execute("""
            INSERT INTO properties (property_id, address, property_type, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (property_id) DO NOTHING
        """, (property_id, f'Market forecast — zip {zip_code}', 'market_aggregate', datetime.now()))

        values = [
            (
                property_id,
                row['ds'].date() if hasattr(row['ds'], 'date') else row['ds'],
                float(row['yhat']),
                float(row['yhat_lower']),
                float(row['yhat_upper']),
                'statsforecast_v1',
                datetime.now(),
            )
            for _, row in forecast_df.iterrows()
        ]

        if values:
            execute_values(cursor, """
                INSERT INTO price_forecasts (
                    property_id, forecast_date, predicted_price,
                    confidence_lower, confidence_upper, model_version, created_at
                ) VALUES %s
                ON CONFLICT (property_id, forecast_date) DO UPDATE SET
                    predicted_price  = EXCLUDED.predicted_price,
                    confidence_lower = EXCLUDED.confidence_lower,
                    confidence_upper = EXCLUDED.confidence_upper,
                    model_version    = EXCLUDED.model_version,
                    created_at       = EXCLUDED.created_at
            """, values)
            self.pg_conn.commit()
            logger.info(f"Saved {len(values)} forecast records")

        cursor.close()

    def cache_forecast_in_redis(self, forecast_df: pd.DataFrame, zip_code: str):
        logger.info(f"Caching forecast for zip_code: {zip_code}")

        forecast_data = forecast_df.to_dict(orient='records')
        self.redis_client.setex(
            f"forecast:zip:{zip_code}",
            86400,
            json.dumps(forecast_data, default=str)
        )

        summary = {
            'zip_code':        zip_code,
            'latest_forecast': float(forecast_df.iloc[-1]['yhat']),
            'avg_forecast':    float(forecast_df['yhat'].mean()),
            'trend':           'increasing' if forecast_df['yhat'].iloc[-1] > forecast_df['yhat'].iloc[0] else 'decreasing',
            'forecast_date':   datetime.now().isoformat(),
            'horizon_days':    self.forecast_horizon,
        }
        self.redis_client.setex(
            f"forecast:summary:{zip_code}",
            86400,
            json.dumps(summary)
        )

    # ------------------------------------------------------------------
    # Insights
    # ------------------------------------------------------------------

    def generate_insights(self, forecast_df: pd.DataFrame, historical_df: pd.DataFrame) -> Dict[str, Any]:
        logger.info("Generating insights...")
        latest_price = float(historical_df['y'].iloc[-1]) if not historical_df.empty else None
        forecast_end = float(forecast_df['yhat'].iloc[-1])

        change_pct = (
            (forecast_end - latest_price) / latest_price * 100
            if latest_price else None
        )

        return {
            'current_price':       latest_price,
            'forecasted_price':    forecast_end,
            'expected_change_pct': round(change_pct, 2) if change_pct is not None else None,
            'confidence_range': {
                'lower': float(forecast_df['yhat_lower'].iloc[-1]),
                'upper': float(forecast_df['yhat_upper'].iloc[-1]),
            },
            'trend_direction':  'up' if (change_pct or 0) > 0 else 'down',
            'volatility':       float(forecast_df['yhat_upper'].std()),
            'recommendation':   self.generate_recommendation(forecast_end, latest_price) if latest_price else 'insufficient_data',
        }

    @staticmethod
    def generate_recommendation(forecast_price: float, current_price: float) -> str:
        change_pct = (forecast_price - current_price) / current_price * 100
        if change_pct > 10:  return "STRONG_BUY — Expected significant appreciation"
        if change_pct > 5:   return "BUY — Good appreciation expected"
        if change_pct > 0:   return "HOLD — Moderate growth expected"
        if change_pct > -5:  return "CAUTION — Slight decline expected"
        return "SELL — Significant decline expected"

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self, zip_code: Optional[str] = None):
        try:
            historical_df = self.fetch_historical_data(zip_code)
            if historical_df.empty:
                logger.warning("No historical data available — run the scraper first")
                return None

            prepared_df = self.prepare_features(historical_df)
            forecast_df = self.train_and_forecast(prepared_df, zip_code or 'all')

            if forecast_df is None:
                logger.warning("Forecasting skipped — insufficient data")
                return None

            self.save_forecast_to_postgres(forecast_df, zip_code or 'all')
            self.cache_forecast_in_redis(forecast_df, zip_code or 'all')
            insights = self.generate_insights(forecast_df, prepared_df)

            logger.info(f"Forecast complete for {zip_code or 'all'}: {insights}")
            return {
                'forecast': forecast_df.to_dict(orient='records'),
                'insights': insights,
            }

        except Exception as e:
            logger.error(f"Forecast failed: {e}", exc_info=True)
            raise
        finally:
            if not self.pg_conn.closed:
                self.pg_conn.close()


if __name__ == "__main__":
    forecaster = RealEstateForecaster()
    zip_codes = [z.strip() for z in os.getenv('FORECAST_ZIP_CODES', '').split(',') if z.strip()]

    if zip_codes:
        for zc in zip_codes:
            forecaster.run(zc)
    else:
        forecaster.run()