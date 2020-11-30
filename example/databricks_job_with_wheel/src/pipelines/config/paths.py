import os

bases = {
    "local": "/home/jovyan/tests/data/",
    "development": "/mnt/dbacademy/",
    "production": None,
}
env = os.environ["STAGE"]
base = bases[env]

test_raw = base + "test/raw"
test_bronze = base + "test/bronze"
test_silver = base + "test/silver"
test_gold = base + "test/gold/"
bronze = base + "bronze"
silver = base + "silver"
gold = base + "gold/"
bronze_checkpoint = base + "checkpoints/bronze"
silver_checkpoint = base + "checkpoints/silver"
gold_checkpoint = base + "checkpoints/gold/"
