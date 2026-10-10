import asyncio
import os
import uuid

import asyncpg
import websockets


# Read the database connection address from Render.
DATABASE_URL = os.environ.get("DATABASE_URL")

# Store players waiting for an opponent.
waiting_players = []

# Prevent two connections from modifying the waiting list simultaneously.
matchmaking_lock = asyncio.Lock()

# This will hold our PostgreSQL connection pool.
database_pool = None


async def connect_to_database():
    """Connect to PostgreSQL and prepare the game-history table."""
    global database_pool

    if not DATABASE_URL:
        print("DATABASE_URL is missing. Game history will not be saved.")
        return

    # Some services provide postgres:// instead of postgresql://.
    database_url = DATABASE_URL.replace(
        "postgres://",
        "postgresql://",
        1
    )

    database_pool = await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=5
    )

    async with database_pool.acquire() as connection:
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS game_history (
                id BIGSERIAL PRIMARY KEY,
                match_id UUID NOT NULL,
                round_number INTEGER NOT NULL,
                chooser TEXT NOT NULL,
                guesser TEXT NOT NULL,
                choice TEXT NOT NULL,
                guess TEXT NOT NULL,
                winner TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    print("Connected to PostgreSQL.")
    print("Game-history table is ready.")


async def save_game_result(
    match_id,
    round_number,
    chooser,
    guesser,
    choice,
    guess,
    winner
):
    """Save one completed round in PostgreSQL."""

    if database_pool is None:
        print("No database connection. Result was not saved.")
        return

    async with database_pool.acquire() as connection:
        await connection.execute("""
            INSERT INTO game_history (
                match_id,
                round_number,
                chooser,
                guesser,
                choice,
                guess,
                winner
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        match_id,
        round_number,
        chooser,
        guesser,
        choice,
        guess,
        winner
        )

    print(f"Saved round {round_number} to PostgreSQL.")


async def play_game(player1, player2):
    """Run a match between two connected players."""

    match_id = uuid.uuid4()
    chooser_number = 1
    round_number = 1

    # Give each connection a simple player label for the history.
    player1_name = "Player 1"
    player2_name = "Player 2"

    try:
        await asyncio.gather(
            player1.send("MATCH STARTED"),
            player2.send("MATCH STARTED")
        )

        while True:

            await asyncio.gather(
                player1.send(f"ROUND {round_number}"),
                player2.send(f"ROUND {round_number}")
            )

            # Decide who chooses and who guesses this round.
            if chooser_number == 1:
                choosing_player = player1
                guessing_player = player2

                chooser_name = player1_name
                guesser_name = player2_name

                await asyncio.gather(
                    player1.send(
                        "Your turn: Choose HEADS or TAILS."
                    ),
                    player2.send(
                        "Wait for Player 1 to choose."
                    )
                )

            else:
                choosing_player = player2
                guessing_player = player1

                chooser_name = player2_name
                guesser_name = player1_name

                await asyncio.gather(
                    player2.send(
                        "Your turn: Choose HEADS or TAILS."
                    ),
                    player1.send(
                        "Wait for Player 2 to choose."
                    )
                )

            # Receive the chooser's selection.
            while True:
                choice = (
                    await choosing_player.recv()
                ).strip().upper()

                if choice in ("HEADS", "TAILS"):
                    break

                await choosing_player.send(
                    "Please choose HEADS or TAILS."
                )

            await asyncio.gather(
                choosing_player.send(f"You chose {choice}."),
                guessing_player.send(
                    f"Your opponent chose {choice}."
                ),
                guessing_player.send(
                    "Your turn: Guess HEADS or TAILS."
                )
            )

            # Receive the guesser's guess.
            while True:
                guess = (
                    await guessing_player.recv()
                ).strip().upper()

                if guess in ("HEADS", "TAILS"):
                    break

                await guessing_player.send(
                    "Please guess HEADS or TAILS."
                )

            # Decide the winner.
            if guess == choice:
                winner = guesser_name
                result = f"{guesser_name} wins!"
            else:
                winner = chooser_name
                result = f"{chooser_name} wins!"

            # Send the result to both players.
            await asyncio.gather(
                player1.send(
                    f"Round {round_number}: {result}"
                ),
                player2.send(
                    f"Round {round_number}: {result}"
                )
            )

            # Save the completed round to PostgreSQL.
            try:
                await save_game_result(
                    match_id,
                    round_number,
                    chooser_name,
                    guesser_name,
                    choice,
                    guess,
                    winner
                )

            except Exception as error:
                print("Could not save game result:", error)

            # The chooser and guesser swap roles next round.
            chooser_number = 2 if chooser_number == 1 else 1

            round_number += 1

            await asyncio.sleep(3)

    except websockets.exceptions.ConnectionClosed:
        print("A player disconnected.")

    except Exception as error:
        print("Game error:", error)

    finally:
        # Tell the remaining player that the match ended.
        for player in (player1, player2):
            try:
                await player.send(
                    "Your opponent disconnected. "
                    "Reconnect to play again."
                )
            except Exception:
                pass


async def game(websocket):
    """Match each new player with a waiting player."""

    player1 = None

    try:
        async with matchmaking_lock:

            if waiting_players:
                player1 = waiting_players.pop(0)

            else:
                waiting_players.append(websocket)

        if player1 is None:
            await websocket.send(
                "Waiting for an opponent..."
            )

            await websocket.wait_closed()
            return

        await play_game(player1, websocket)

    except websockets.exceptions.ConnectionClosed:
        print("Connection closed.")

    finally:
        async with matchmaking_lock:
            if websocket in waiting_players:
                waiting_players.remove(websocket)


async def main():
    """Start the database and the WebSocket server."""

    port = int(os.environ.get("PORT", 10000))

    await connect_to_database()

    async with websockets.serve(
        game,
        "0.0.0.0",
        port,
        max_size=1024
    ):
        print(f"Game server running on port {port}.")
        await asyncio.Future()


asyncio.run(main())
