def check_draw(board):
    for row in board:
        if '  ' in row:
            return False
    return True
