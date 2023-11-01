from board_settings import create_board, print_board
from players_info import get_num_players, get_player_name
from check_winner import check_winner
from check_draw import check_draw


def start_game():
    board, size_x, size_y = create_board()
    num_players = get_num_players()
    players = []

    for i in range(1, num_players + 1):
        player_name = get_player_name(i)
        players.append({'name': player_name, 'sym': str(player_name)[0].upper()})

    current_player_index = 0

    while True:
        print_board(board)
        current_player = players[current_player_index]
        print(f"Ход игрока {current_player['name']}")

        while True:
            try:
                column = int(input(f"Выберите столбец для хода (1-{size_x}): ")) - 1
                if column < 0 or column > size_x:
                    print("Некорректный ввод. Попробуйте еще раз.")
                else:
                    break
            except:
                print("Некорректный ввод. Попробуйте еще раз.")

        if board[0][column] != '  ':
            print("Недопустимый ход. Попробуйте снова.")
            continue

        for row in range(size_y-1, -1, -1):
            if board[row][column] == '  ':
                board[row][column] = ' ' + current_player['sym']
                break

        if check_winner(board, ' ' + current_player['sym']):
            print_board(board)
            print(f"Игрок {current_player['name']} победил!")
            break

        if check_draw(board):
            print_board(board)
            print("Ничья!")
            break

        current_player_index = (current_player_index + 1) % num_players


start_game()
