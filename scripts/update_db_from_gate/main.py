import asyncio
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from pathlib import Path
import aiohttp
import cbor2
import gzip
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(encoding='utf-8', level=logging.INFO)

def now(): return datetime.now(timezone.utc)

SYMBOL = "KDA_USDT"
BATCH_SIZE = 50
DELTA = timedelta(minutes=5)

class PriceDb():
    DB_PATH = Path("../../db")

    @classmethod
    def db_file(cls,x):
        return cls.DB_PATH.joinpath(x)

    def __init__(self):
        self.data = None

    def last_ts(self):
        return self.data[-1]["ts"]

    def trim(self):
        self.data.pop()

    def sort_data(self):
        logger.info("Sorting data")
        self.data.sort(key=lambda x:x["ts"])

    def load(self):
        logger.info("Loading latest file")
        with open(self.db_file("latest"), "rt") as fd:
            latest = fd.read().strip()

        logger.info("Loading database:" + latest)
        with gzip.open(self.db_file(latest), "rb") as fd:
            self.data = cbor2.load(fd)

        self.sort_data()

    def save(self):
        filename = "candles_kda_usd_{}.cbor.gz".format(now().strftime("%Y-%m-%dT%H:%M:%SZ"))

        self.sort_data()
        self.trim()

        logger.info("Writing database:" + filename)
        with gzip.open(self.db_file(filename), "wb") as fd:
            cbor2.dump(self.data, fd, datetime_as_timestamp=True, timezone=timezone.utc)

        with open(self.db_file("latest"), "wt") as fd:
            fd.write(filename)


def _from_gate(doc):
    return {"ts": datetime.fromtimestamp(int(doc[0]), tz=timezone.utc),
            "prices":[Decimal(doc[4]),
                      Decimal(doc[3]),
                      Decimal(doc[5]),
                      Decimal(doc[2])]}

async def load_batch_from_gate(session, db):
    logger.info("Loading Gate data from {!s}".format(db.last_ts()))
    start_ts = int(db.last_ts().timestamp()) + 600

    params = {"interval":"5m", "currency_pair":SYMBOL.replace("-","_"), "from":start_ts, "limit":BATCH_SIZE}
    async with session.get("https://api.gateio.ws/api/v4/spot/candlesticks", params=params) as resp:
        raw_data = await resp.json()
        db.data.extend(sorted(map(_from_gate, raw_data), key=lambda x:x["ts"]))


async def load_from_gate(db):
    tries = 100
    async with aiohttp.ClientSession() as session:
        while db.last_ts() < (now() - DELTA) and tries > 0:
            await load_batch_from_gate(session, db)
            tries -= 1


def doc_to_decimal(doc):
    return {"ts":doc["ts"], "prices":[Decimal128.to_decimal(x) for x in  doc["prices"]]}


def main():
    db = PriceDb()
    db.load()
    db.trim()
    asyncio.run(load_from_gate(db))
    db.trim()
    db.save()



if __name__ == "__main__":
    main()