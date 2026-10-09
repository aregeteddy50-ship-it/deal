import asyncio
import os
import websockets


players = []

player_choice = None
player_guess = None


async def send_to_all_players(message):
    for player in players:
        await player.send(message)


async def game(websocket):
    global player_choice, player_guess

    if len(players) >= 2:
        await websocket.send("SERVER FULL")
        return

    players.append(websocket)
    player_number = len(players)

    print(f"Player {player_number} connected.")

    await websocket.send(f"PLAYER {player_number}")

    if player_number == 1:
        await websocket.send("Waiting for Player 2...")

    elif player_number == 2:
        await send_to_all_players("Both players are connected!")

        await players[0].send("Choose HEADS or TAILS.")

        await players[1].send(
            "You are Player 2. Wait for Player 1 to choose."
        )

    try:
        async for message in websocket:
            message = message.strip().upper()

            print(f"Player {player_number} sent: {message}")

            if message not in ("HEADS", "TAILS"):
                await websocket.send(
                    "Invalid input. Type HEADS or TAILS."
                )
                continue

            if player_number == 1:
                if player_choice is not None:
                    await websocket.send(
                        "Player 1 has already chosen."
                    )
                    continue

                player_choice = message

                await websocket.send(
                    f"You selected {player_choice}."
                )

                if len(players) == 2:
                    await players[1].send(
                        "Player 1 has chosen. "
                        "Enter HEADS or TAILS to guess."
                    )

            elif player_number == 2:
                if player_choice is None:
                    await websocket.send(
                        "Wait for Player 1 to choose first."
                    )
                    continue

                if player_guess is not None:
                    await websocket.send(
                        "You have already guessed."
                    )
                    continue

                player_guess = message

                if player_guess == player_choice:
                    result = (
                        "Player 2 wins! "
                        "The guess was correct."
                    )
                else:
                    result = (
                        "Player 1 wins! "
                        "The guess was incorrect."
                    )

                await send_to_all_players(
                    f"Player 1 chose: {player_choice}"
                )

                await send_to_all_players(
                    f"Player 2 guessed: {player_guess}"
                )

                await send_to_all_players(result)

                await send_to_all_players(
                    "Round finished. Restart the server "
                    "before playing another round."
                )

                break

    except websockets.exceptions.ConnectionClosed:
        print(f"Player {player_number} disconnected.")

    finally:
        if websocket in players:
            players.remove(websocket)

        player_choice = None
        player_guess = None

        print("Player disconnected. Game state reset.")


port = int(os.environ.get("PORT", 10000))


async def main():
    async with websockets.serve(
        game,
        "0.0.0.0",
        port
    ):
        print("Online game server is running.")
        await asyncio.Future()


asyncio.run(main())
