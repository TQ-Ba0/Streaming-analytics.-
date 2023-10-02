    use coin_price_db;
    CREATE TABLE  IF NOT EXISTS  coin_data(
        id Int64,
        rank Int64,
        symbol Text ,
        name Text,
        supply Float64,
        maxSupply  Float64,
        marketCapUsd Float64,
        volumeUsd24Hr Float64,
        priceUsd Float64,
        changePercent24Hr Float64,
        vwap24Hr Float64,
        update datetime
    )
    ENGINE = MergeTree()
    PRIMARY KEY(id)