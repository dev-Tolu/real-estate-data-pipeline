import os
import json
import logging
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import redis
from typing import Dict, Any, Optional
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealEstateForecaster:
    def __init__(self):
        postgres_dsn = os.getenv('POSTGRES_DSN')

        # SQLAlchemy engine — required for pd.read_sql() in pandas >= 2.0
        # Ensures the DSN uses the psycopg2 dialect explicitly
        engine_url = postgres_dsn
        if engine_url.startswith('postgresql://'):
            engine_url = engine_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
        self.engine = create_engine(engine_url)

        # Raw psycopg2 connection — used only for execute_values() writes
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

        # Use parameterised query via SQLAlchemy to avoid SQL injection
        if zip_code:
            query = text("""
                SELECT
                    ph.listing_date  AS ds,
                    AVG(ph.price)    AS y,
                    p.zip_code
                FROM price_history ph
                JOIN properties p ON ph.property_id = p.property_id
                WHERE ph.price IS NOT NULL
                  AND p.zip_code = :zip_code
                GROUP BY ph.listing_date, p.zip_code
                ORDER BY ph.listing_date
            """)
            params = {'zip_code': zip_code}
        else:
            query = text("""
                SELECT
                    ph.listing_date  AS ds,
                    AVG(ph.price)    AS y,
                    p.zip_code
                FROM price_history ph
                JOIN properties p ON ph.property_id = p.property_id
                WHERE ph.price IS NOT NULL
                GROUP BY ph.listing_date, p.zip_code
                ORDER BY ph.listing_date
            """)
            params = {}

        # pd.read_sql() with a SQLAlchemy engine — no more UserWarning
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            logger.warning(f"No data found for zip_code: {zip_code}")
            return pd.DataFrame()

        logger.info(f"Fetched {len(df)} historical records")
        return df

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Preparing features...")
        df = df.copy()
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.sort_values('ds').reset_index(drop=True)

        df['y_lag_7']        = df['y'].shift(7)
        df['y_lag_30']       = df['y'].shift(30)
        df['y_rolling_7']    = df['y'].rolling(window=7).mean()
        df['y_rolling_30']   = df['y'].rolling(window=30).mean()
        df['month']          = df['ds'].dt.month
        df['quarter']        = df['ds'].dt.quarter
        df['day_of_week']    = df['ds'].dt.dayofweek
        df['is_weekend']     = (df['day_of_week'] >= 5).astype(int)
        df['is_summer']      = df['month'].isin([6, 7, 8]).astype(int)
        df['is_winter']      = df['month'].isin([12, 1, 2]).astype(int)

        df = df.dropna()
        logger.info(f"Prepared {len(df)} records with features")
        return df

    # ------------------------------------------------------------------
    # Model training
    # ------------------------------------------------------------------

    def train_model(self, df: pd.DataFrame, zip_code: str) -> Optional[Prophet]:
        logger.info(f"Training model for zip_code: {zip_code}")

        # Prophet needs at least 2 data points; cross-validation needs ~2x initial
        if len(df) < 10:
            logger.warning(
                f"Only {len(df)} data points for zip_code={zip_code} — "
                f"need at least 10 to train. Skipping."
            )
            return None

        model = Prophet(
            interval_width=self.confidence_interval,
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            holidays_prior_scale=10.0,
            seasonality_mode='multiplicative'
        )
        model.add_seasonality(name='monthly',   period=30.5,  fourier_order=5)
        model.add_seasonality(name='quarterly', period=91.25, fourier_order=3)
        model.fit(df[['ds', 'y']])

        # Cross-validation only if we have enough data
        if len(df) >= 365 * 2:
            logger.info("Performing cross-validation...")
            try:
                cv_results = cross_validation(
                    model,
                    initial='365 days',
                    period='30 days',
                    horizon='90 days'
                )
                metrics = performance_metrics(cv_results)
                mape    = float(metrics['mape'].mean())
                rmse    = float(metrics['rmse'].mean())
                logger.info(f"CV metrics — MAPE: {mape:.2f}%  RMSE: {rmse:.2f}")

                self.redis_client.hset(
                    f"forecast:metrics:{zip_code}",
                    mapping={
                        'mape':          mape,
                        'rmse':          rmse,
                        'training_date': datetime.now().isoformat(),
                        'horizon':       self.forecast_horizon,
                    }
                )
            except Exception as e:
                logger.warning(f"Cross-validation skipped: {e}")
        else:
            logger.info(
                f"Skipping cross-validation — need 2+ years of data "
                f"(have {len(df)} points)"
            )

        return model

    # ------------------------------------------------------------------
    # Forecasting
    # ------------------------------------------------------------------

    def generate_forecast(self, model: Prophet, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Generating {self.forecast_horizon}-day forecast")
        future   = model.make_future_dataframe(periods=self.forecast_horizon, include_history=True)
        forecast = model.predict(future)
        result   = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(self.forecast_horizon)
        logger.info(f"Generated {len(result)} forecast points")
        return result

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_forecast_to_postgres(self, forecast_df: pd.DataFrame, zip_code: str):
        logger.info(f"Saving forecast for zip_code: {zip_code}")

        # Ensure the dummy property row exists so the FK constraint is satisfied
        property_id = f"forecast_zip_{zip_code}"
        cursor = self.pg_conn.cursor()

        cursor.execute("""
            INSERT INTO properties (property_id, address, property_type, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (property_id) DO NOTHING
        """, (property_id, f'Market forecast — zip {zip_code}', 'market_aggregate', datetime.now()))

        # price_forecasts has no unique constraint on (property_id, forecast_date)
        # in the current schema, so we use INSERT ... ON CONFLICT DO NOTHING
        # via a manual unique index if you add one, or plain INSERT here.
        values = [
            (
                property_id,
                row['ds'].date() if hasattr(row['ds'], 'date') else row['ds'],
                float(row['yhat']),
                float(row['yhat_lower']),
                float(row['yhat_upper']),
                'prophet_v1',
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
                ON CONFLICT DO NOTHING
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
            'current_price':      latest_price,
            'forecasted_price':   forecast_end,
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
        if change_pct > 10:   return "STRONG_BUY — Expected significant appreciation"
        if change_pct > 5:    return "BUY — Good appreciation expected"
        if change_pct > 0:    return "HOLD — Moderate growth expected"
        if change_pct > -5:   return "CAUTION — Slight decline expected"
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
            model       = self.train_model(prepared_df, zip_code or 'all')

            if model is None:
                logger.warning("Model training skipped — insufficient data")
                return None

            forecast_df = self.generate_forecast(model, prepared_df)
            self.save_forecast_to_postgres(forecast_df, zip_code or 'all')
            self.cache_forecast_in_redis(forecast_df, zip_code or 'all')
            insights    = self.generate_insights(forecast_df, prepared_df)

            logger.info(f"Forecast complete for {zip_code or 'all'}: {insights}")
            return {
                'forecast': forecast_df.to_dict(orient='records'),
                'insights': insights,
            }

        except Exception as e:
            logger.error(f"Forecast failed: {e}", exc_info=True)
            raise
        finally:
            # Close raw psycopg2 connection; SQLAlchemy engine manages its own pool
            if not self.pg_conn.closed:
                self.pg_conn.close()


if __name__ == "__main__":
    forecaster = RealEstateForecaster()
    zip_codes  = [z.strip() for z in os.getenv('FORECAST_ZIP_CODES', '').split(',') if z.strip()]

    if zip_codes:
        for zc in zip_codes:
            forecaster.run(zc)
    else:
        forecaster.run()