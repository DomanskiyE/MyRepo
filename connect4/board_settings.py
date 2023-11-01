def create_board():
    print('Стандартный размер доски 6х7. Вы хотите задать другой размер? Y/N')
    ans = input()
    size_x = 7
    size_y = 6
    if ans.upper() == 'Y':
        while True:
            try:
                size_x = int(input('Введите ширину доски: '))
                size_y = int(input('Введите высоту доски: '))
                if size_x <= 1 or size_y <= 1:
                    print('Значение должно быть целым числом больше 1!')
                else:
                    break
            except:
                print('Значение должно быть целым числом больще 1!')
    board = [['  ' for _ in range(size_x)] for _ in range(size_y)]
    return board, size_x, size_y


def print_board(board):
    for row in board:
        print('|'.join(row))
        print('-' * len(row) * 3)
    nums = ''
    for i in range(1, len(board[0]) + 1):
        nums += f' {i} '
    print(nums.strip())
