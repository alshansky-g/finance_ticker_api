from datetime import UTC, datetime
from pydantic import BaseModel, AliasPath, Field


class Ticker(BaseModel):
    symbol: str = Field(
        validation_alias=AliasPath("chart", "result", 0, "meta", "symbol")
    )
    timestamps: list[int] = Field(
        validation_alias=AliasPath("chart", "result", 0, "timestamp")
    )
    open: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "quote", 0, "open"
        )
    )
    high: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "quote", 0, "high"
        )
    )
    low: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "quote", 0, "low"
        )
    )
    close: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "quote", 0, "close"
        )
    )
    adjclose: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "adjclose", 0, "adjclose"
        )
    )
    volume: list[float] = Field(
        validation_alias=AliasPath(
            "chart", "result", 0, "indicators", "quote", 0, "volume"
        )
    )

    def to_rows(self) -> list[dict]:
        return [
            {
                "date": datetime.fromtimestamp(t, UTC).date().isoformat(),
                "open": self.open[i],
                "high": self.high[i],
                "low": self.low[i],
                "close": self.close[i],
                "adj_close": self.adjclose[i],
                "volume": self.volume[i],
            }
            for i, t in enumerate(self.timestamps)
        ]

    @property
    def headers(self) -> list[str]:
        return ["date", "open", "high", "low", "close", "adj_close", "volume"]
