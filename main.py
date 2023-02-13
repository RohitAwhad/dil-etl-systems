from modules.etl_manager import run_health_systems
from modules.utilities.aws.rds.initialize.db_initializer import initialize_db


def main(overwrite=True, download=False):
    engine = initialize_db(overwrite, download)
    run_health_systems(engine)


if __name__ == "__main__":
    overwrite = False
    download = True
    main(overwrite, download)
