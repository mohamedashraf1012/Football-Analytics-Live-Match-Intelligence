#run_all_silver.py

import time

from spark_session import get_spark

from cleaning.clean_appearances import clean_appearances
from cleaning.clean_players import clean_players
from cleaning.clean_transfers import clean_transfers
from cleaning.clean_games import clean_games
from cleaning.clean_player_valuations import clean_player_valuations
from cleaning.clean_countries import clean_countries
from cleaning.clean_competitions import clean_competitions
from cleaning.clean_clubs import clean_clubs
from cleaning.clean_club_games import clean_club_games
from cleaning.clean_game_lineups import clean_game_lineups
from cleaning.clean_game_events import clean_game_events
from cleaning.clean_national_teams import clean_national_teams


def main():

    print("=" * 60)
    print("         SILVER LAYER PIPELINE")
    print("=" * 60)

    spark, bucket = get_spark("SilverPipeline")


    jobs = [
        ("countries", clean_countries),
        ("competitions", clean_competitions),
        ("clubs", clean_clubs),
        ("players", clean_players),
        ("games", clean_games),
        ("appearances", clean_appearances),
        ("transfers", clean_transfers),
        ("player_valuations", clean_player_valuations),
        ("game_lineups", clean_game_lineups),
        ("game_events", clean_game_events),
        ("club_games", clean_club_games),
        ("national_teams", clean_national_teams),
    ]

    failed = []

    try:

        for table_name, job in jobs:

            print(f"\n▶ Running {table_name} ...")

            start = time.time()

            try:

                job(spark, bucket)

                elapsed = round(time.time() - start, 1)

                print(
                    f"✓ {table_name} completed successfully "
                    f"({elapsed} sec)"
                )

            except Exception as e:

                elapsed = round(time.time() - start, 1)

                print(
                    f"✗ {table_name} FAILED "
                    f"({elapsed} sec)"
                )

                print(f"Error: {e}")

                failed.append(table_name)

        print("\n" + "=" * 60)

        if not failed:
            print("ALL SILVER TABLES COMPLETED SUCCESSFULLY")
        else:
            print(f"{len(failed)} TABLE(S) FAILED:")
            for table in failed:
                print(f" - {table}")

        print("=" * 60)

    finally:

        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()