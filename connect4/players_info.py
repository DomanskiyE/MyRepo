def get_num_players():
    while True:
        num_players = input("Введите количество игроков (минимум 2): ")
        if num_players.isdigit() and int(num_players) >= 2:
            return int(num_players)
        print("Некорректный ввод. Попробуйте снова.")


def get_player_name(player_num):
    player_name = input(f"Введите имя игрока {player_num}: ")
    return player_name
