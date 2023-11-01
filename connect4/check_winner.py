def check_winner(board, player):
    for row in range(len(board)):
        for col in range(len(board[row]) - 3):
            if all(board[row][col+i] == player for i in range(4)):
                return True

    for row in range(len(board) - 3):
        for col in range(len(board[row])):
            if all(board[row+i][col] == player for i in range(4)):
                return True

    for row in range(len(board) - 3):
        for col in range(len(board[row]) - 3):
            if all(board[row+i][col+i] == player for i in range(4)):
                return True

    for row in range(len(board) - 3):
        for col in range(3, len(board[row])):
            if all(board[row+i][col-i] == player for i in range(4)):
                return True
    return False
