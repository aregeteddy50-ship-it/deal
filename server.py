import asyncio
import websockets

players = []


async def game(websocket):

    print("A player connected")

    if len(players) >= 2:
        await websocket.send("GAME FULL")
        return

    players.append(websocket)

    player_number = len(players)

    await websocket.send(f"PLAYER {player_number}")

    print(f"Player {player_number} connected")

    if player_number == 1:

        await websocket.send("Waiting for Player 2...")

    elif player_number == 2:

        await players[0].send("Player 2 has joined!")

        await players[1].send("Player 1 has joined!")

    try:

        async for message in websocket:

            print(
                f"Player {player_number} selected {message}"
            )

            # Send the selection to the other player
            for player in players:

                if player != websocket:

                    await player.send(
                        f"Player {player_number} selected {message}"
                    )

            # Tell the player their message was sent
            await websocket.send(
                f"You selected {message}"
            )

    except websockets.exceptions.ConnectionClosed:

        print(
            f"Player {player_number} disconnected"
        )

    finally:

        if websocket in players:

            players.remove(websocket)

        print(
            f"Player {player_number} left the game"
        )


async def main():

    print("Starting server...")

    async with websockets.serve(
        game,
        "localhost",
        8765
    ):

        print("Server is running")
        print("Waiting for players...")

        await asyncio.Future()


asyncio.run(main())