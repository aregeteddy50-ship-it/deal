import asyncio
import os
import websockets

waiting_players = []
lock = asyncio.Lock()


async def play_game(player1, player2):
    chooser = 1
    round_number = 1

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

            if chooser == 1:
                choosing_player = player1
                guessing_player = player2

                await asyncio.gather(
                    player1.send("Your turn: Choose HEADS or TAILS."),
                    player2.send("Wait for Player 1 to choose.")
                )
            else:
                choosing_player = player2
                guessing_player = player1

                await asyncio.gather(
                    player2.send("Your turn: Choose HEADS or TAILS."),
                    player1.send("Wait for Player 2 to choose.")
                )

            # Read the chooser's selection.
            while True:
                choice = (await choosing_player.recv()).strip().upper()

                if choice in ("HEADS", "TAILS"):
                    break

                await choosing_player.send("Please choose HEADS or TAILS.")

            await choosing_player.send(f"You chose {choice}.")
            await guessing_player.send(
                f"Your opponent chose {choice}."
            )
            await guessing_player.send("Your turn: Guess HEADS or TAILS.")

            # Read the other player's guess.
            while True:
                guess = (await guessing_player.recv()).strip().upper()

                if guess in ("HEADS", "TAILS"):
                    break

                await guessing_player.send("Please guess HEADS or TAILS.")

            if guess == choice:
                result = "Guesser wins!"
            else:
                result = "Chooser wins!"

            await asyncio.gather(
                player1.send(
                    f"Round {round_number}: {result}"
                ),
                player2.send(
                    f"Round {round_number}: {result}"
                )
            )

            # Switch roles for the next round.
            chooser = 2 if chooser == 1 else 1
            round_number += 1

            await asyncio.sleep(3)

    except websockets.exceptions.ConnectionClosed:
        print("A player disconnected.")


async def game(websocket):
    player1 = None

    try:
        async with lock:
            if waiting_players:
                player1 = waiting_players.pop(0)
            else:
                waiting_players.append(websocket)

        if player1 is None:
            await websocket.send("Waiting for an opponent...")
            await websocket.wait_closed()
            return

        await play_game(player1, websocket)

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
