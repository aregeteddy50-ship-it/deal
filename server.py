import asyncio
import os
import websockets

waiting_players = []
lock = asyncio.Lock()


async def play_game(player1, player2):
    try:
        await player1.send("MATCH STARTED")
        await player2.send("MATCH STARTED")

        await player1.send("Choose HEADS or TAILS.")
        await player2.send("Wait for Player 1 to choose.")

        while True:
            choice = (await player1.recv()).strip().upper()

            if choice in ("HEADS", "TAILS"):
                break

            await player1.send("Type HEADS or TAILS.")

        await player2.send(f"Player 1 chose {choice}.")
        await player2.send("Guess HEADS or TAILS.")

        while True:
            guess = (await player2.recv()).strip().upper()

            if guess in ("HEADS", "TAILS"):
                break

            await player2.send("Type HEADS or TAILS.")

        if guess == choice:
            result = "Player 2 wins!"
        else:
            result = "Player 1 wins!"

        await asyncio.gather(
            player1.send(result),
            player2.send(result)
        )

    except websockets.exceptions.ConnectionClosed:
        print("A player disconnected during the game.")


async def game(websocket):
    player1 = None
    player2 = None

    try:
        async with lock:
            waiting_players.append(websocket)

            if len(waiting_players) >= 2:
                player1 = waiting_players.pop(0)
                player2 = waiting_players.pop(0)

        if player1 is None:
            await websocket.send("Waiting for an opponent...")

            # Keep this player's connection open.
            await websocket.wait_closed()
            return

        await play_game(player1, player2)

    except websockets.exceptions.ConnectionClosed:
        print("Connection closed.")

    finally:
        async with lock:
            if websocket in waiting_players:
                waiting_players.remove(websocket)


port = int(os.environ.get("PORT", 10000))


async def main():
    async with websockets.serve(
        game,
        "0.0.0.0",
        port,
        max_size=1024
    ):
        print("Game server running.")
        await asyncio.Future()


asyncio.run(main())
